package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/t0pm1x/orderflow/platform/outbox"
	"github.com/t0pm1x/orderflow/platform/types"

	"github.com/t0pm1x/orderflow/services/order/internal/domain"
)

// mockRepo is an in-memory repo for testing. It records every outbox
// Record passed to Insert so the test can assert on event emission
// without needing a real DB.
type mockRepo struct {
	orders       map[types.OrderID]*domain.Order
	events       map[types.OrderID][]outbox.Record
	cancelCalls  int
	lastCancelID types.OrderID
	cancelErr    error
}

func newMockRepo() *mockRepo {
	return &mockRepo{
		orders: map[types.OrderID]*domain.Order{},
		events: map[types.OrderID][]outbox.Record{},
	}
}

func (m *mockRepo) Insert(_ context.Context, o *domain.Order, events ...outbox.Record) error {
	m.orders[o.ID] = o
	m.events[o.ID] = append(m.events[o.ID], events...)
	return nil
}

func (m *mockRepo) Get(_ context.Context, id types.OrderID) (*domain.Order, error) {
	o, ok := m.orders[id]
	if !ok {
		return nil, errNotFound
	}
	return o, nil
}

func (m *mockRepo) List(_ context.Context, state domain.OrderState, _ int) ([]*domain.Order, error) {
	var out []*domain.Order
	for _, o := range m.orders {
		if state == "" || o.State == state {
			out = append(out, o)
		}
	}
	return out, nil
}

func (m *mockRepo) Cancel(_ context.Context, id types.OrderID) error {
	m.cancelCalls++
	m.lastCancelID = id
	return m.cancelErr
}

func TestSubmit_OK(t *testing.T) {
	h := NewHandler(newMockRepo())
	body := `{"customer_id":"550e8400-e29b-41d4-a716-446655440000","items":[{"sku":"A","quantity":1,"unit_price_cents":100}]}`
	req := httptest.NewRequest("POST", "/v1/orders", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	h.Routes().ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Errorf("expected 201, got %d: %s", w.Code, w.Body.String())
	}
}

// Regression test for the e2e TestE2E_HappyPath_OrderConfirmed
// failure: the response body must decode into a struct whose ID field
// is a JSON string, not a 16-byte array.
func TestSubmit_ResponseIDIsString(t *testing.T) {
	h := NewHandler(newMockRepo())
	body := `{"customer_id":"550e8400-e29b-41d4-a716-446655440000","items":[{"sku":"A","quantity":1,"unit_price_cents":100}]}`
	req := httptest.NewRequest("POST", "/v1/orders", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	h.Routes().ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}
	var got struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(w.Body).Decode(&got); err != nil {
		t.Fatalf("decode create response: %v (body=%s)", err, w.Body.String())
	}
	if got.ID == "" {
		t.Errorf("empty order id in response: %s", w.Body.String())
	}
}

func TestSubmit_InvalidPayload(t *testing.T) {
	h := NewHandler(newMockRepo())
	req := httptest.NewRequest("POST", "/v1/orders", bytes.NewBufferString(`not json`))
	w := httptest.NewRecorder()
	h.Routes().ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestSubmit_EmitsOrderCreatedEvent(t *testing.T) {
	repo := newMockRepo()
	h := NewHandler(repo)
	body := `{"customer_id":"550e8400-e29b-41d4-a716-446655440000","items":[{"sku":"A","quantity":2,"unit_price_cents":150}]}`
	req := httptest.NewRequest("POST", "/v1/orders", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	h.Routes().ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}

	// Exactly one outbox record should have been written, and it
	// must be an OrderCreated for the just-created order.
	if len(repo.events) != 1 {
		t.Fatalf("expected events for 1 order, got %d", len(repo.events))
	}
	var got []outbox.Record
	for _, recs := range repo.events {
		got = recs
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 record, got %d", len(got))
	}
	rec := got[0]
	if rec.EventType != "OrderCreated" {
		t.Errorf("event_type: got %q want OrderCreated", rec.EventType)
	}
	if rec.AggregateType != "Order" {
		t.Errorf("aggregate_type: got %q want Order", rec.AggregateType)
	}
	if rec.Topic != TopicOrderEvents {
		t.Errorf("topic: got %q want %q", rec.Topic, TopicOrderEvents)
	}
	if rec.SchemaVersion != "1.0" {
		t.Errorf("schema_version: got %q want 1.0", rec.SchemaVersion)
	}
	if rec.EventID == "" {
		t.Error("event_id must be assigned")
	}
	if len(rec.Payload) == 0 {
		t.Error("payload must be non-empty JSON")
	}
}

func TestGet_OK(t *testing.T) {
	repo := newMockRepo()
	o := domain.NewOrder(types.NewCustomerID(), []domain.OrderItem{{SKU: "A", Quantity: 1, UnitPriceCents: 100}})
	if err := repo.Insert(context.Background(), o); err != nil {
		t.Fatalf("mockRepo.Insert: %v", err)
	}

	h := NewHandler(repo)
	req := httptest.NewRequest("GET", "/v1/orders/"+o.ID.String(), nil)
	w := httptest.NewRecorder()
	h.Routes().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestGet_NotFound(t *testing.T) {
	h := NewHandler(newMockRepo())
	id := types.NewOrderID()
	req := httptest.NewRequest("GET", "/v1/orders/"+id.String(), nil)
	w := httptest.NewRecorder()
	h.Routes().ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

func TestList_OK(t *testing.T) {
	h := NewHandler(newMockRepo())
	req := httptest.NewRequest("GET", "/v1/orders", nil)
	w := httptest.NewRecorder()
	h.Routes().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

// TestCancel_OK verifies that DELETE /v1/orders/{id} calls
// Repository.Cancel and returns 204. The cancel contract is idempotent:
// terminal orders (confirmed/cancelled/failed) should also return 204
// because the order already is in (or past) the requested state.
func TestCancel_OK(t *testing.T) {
	repo := newMockRepo()
	h := NewHandler(repo)
	id := types.NewOrderID()
	req := httptest.NewRequest("DELETE", "/v1/orders/"+id.String(), nil)
	w := httptest.NewRecorder()
	h.Routes().ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Errorf("status: got %d want 204; body=%s", w.Code, w.Body.String())
	}
	if repo.cancelCalls != 1 || repo.lastCancelID != id {
		t.Errorf("Cancel calls: got %d (id=%v) want 1 (id=%v)", repo.cancelCalls, repo.lastCancelID, id)
	}
}

// TestCancel_NotFound asserts that a missing id returns 404 (not 204).
// Repository surfaces ErrNotFound for unknown ids and for terminal
// states that cannot be cancelled; the handler maps that to a 404.
func TestCancel_NotFound(t *testing.T) {
	repo := newMockRepo()
	repo.cancelErr = errNotFound
	h := NewHandler(repo)
	id := types.NewOrderID()
	req := httptest.NewRequest("DELETE", "/v1/orders/"+id.String(), nil)
	w := httptest.NewRecorder()
	h.Routes().ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("status: got %d want 404", w.Code)
	}
}

// TestCancel_InvalidID asserts 400 on a malformed UUID — Cancel must
// not silently swallow parse errors.
func TestCancel_InvalidID(t *testing.T) {
	h := NewHandler(newMockRepo())
	req := httptest.NewRequest("DELETE", "/v1/orders/not-a-uuid", nil)
	w := httptest.NewRecorder()
	h.Routes().ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status: got %d want 400; body=%s", w.Code, w.Body.String())
	}
}
