package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/t0pm1x/orderflow/platform/types"

	"github.com/t0pm1x/orderflow/services/order/internal/domain"
)

// mockRepo is an in-memory repo for testing.
type mockRepo struct {
	orders map[types.OrderID]*domain.Order
}

func newMockRepo() *mockRepo {
	return &mockRepo{orders: map[types.OrderID]*domain.Order{}}
}

func (m *mockRepo) Insert(o *domain.Order) error {
	m.orders[o.ID] = o
	return nil
}

func (m *mockRepo) Get(id types.OrderID) (*domain.Order, error) {
	o, ok := m.orders[id]
	if !ok {
		return nil, errNotFound
	}
	return o, nil
}

func (m *mockRepo) List(state domain.OrderState, limit int) ([]*domain.Order, error) {
	var out []*domain.Order
	for _, o := range m.orders {
		if state == "" || o.State == state {
			out = append(out, o)
		}
	}
	return out, nil
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

func TestSubmit_InvalidPayload(t *testing.T) {
	h := NewHandler(newMockRepo())
	req := httptest.NewRequest("POST", "/v1/orders", bytes.NewBufferString(`not json`))
	w := httptest.NewRecorder()
	h.Routes().ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGet_OK(t *testing.T) {
	repo := newMockRepo()
	o := domain.NewOrder(types.NewCustomerID(), []domain.OrderItem{{SKU: "A", Quantity: 1, UnitPriceCents: 100}})
	repo.Insert(o)

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
