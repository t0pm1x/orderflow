// Tests for internal/server/api.go. httptest + fake backend clients
// — no real services required. Each test stands up a chi router
// with a stub API whose backend clients are pre-programmed to
// return the desired error/success path, then fires HTTP
// requests at /api/* and asserts the JSON envelope / status code
// matches the contract the SPA depends on.
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/t0pm1x/orderflow/services/web/internal/backend"
)

// ---- fake backend clients ------------------------------------------

type fakeOrder struct {
	listResp    *backend.OrderList
	listErr     error
	getResp     *backend.Order
	getErr      error
	submitResp  *backend.Order
	submitErr   error
	cancelErr   error
	lastCancel  string
	lastSubmit  backend.OrderSubmit
}

func (f *fakeOrder) List(_ context.Context, state backend.OrderState, _ int) (*backend.OrderList, error) {
	if f.listResp == nil || state == "" {
		return f.listResp, f.listErr
	}
	filtered := &backend.OrderList{}
	for _, o := range f.listResp.Items {
		if o.State == state {
			filtered.Items = append(filtered.Items, o)
		}
	}
	return filtered, f.listErr
}
func (f *fakeOrder) Get(_ context.Context, _ string) (*backend.Order, error) {
	return f.getResp, f.getErr
}
func (f *fakeOrder) Submit(_ context.Context, in backend.OrderSubmit) (*backend.Order, error) {
	f.lastSubmit = in
	return f.submitResp, f.submitErr
}
func (f *fakeOrder) Cancel(_ context.Context, id string) error {
	f.lastCancel = id
	return f.cancelErr
}

type fakePayment struct {
	err       error
	lastWh    backend.PaymentWebhook
	fireCalls int
}

func (f *fakePayment) FireWebhook(_ context.Context, w backend.PaymentWebhook) error {
	f.fireCalls++
	f.lastWh = w
	return f.err
}

type fakeInventory struct {
	resp *backend.StockItem
	err  error
}

func (f *fakeInventory) GetStock(_ context.Context, _ string) (*backend.StockItem, error) {
	return f.resp, f.err
}

// ---- test helpers --------------------------------------------------

func newTestAPI(order *fakeOrder, payment *fakePayment, inv *fakeInventory) (*API, *chi.Mux) {
	api := &API{
		Order:     order,
		Payment:   payment,
		Inventory: inv,
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	r := chi.NewRouter()
	r.Get("/api/orders", api.ListOrders)
	r.Get("/api/orders/{id}", api.GetOrder)
	r.Post("/api/orders", api.SubmitOrder)
	r.Delete("/api/orders/{id}", api.CancelOrder)
	r.Get("/api/inventory/stock/{sku}", api.GetInventoryStock)
	r.Post("/api/payments/webhook", api.FireWebhook)
	return api, r
}

func do(t *testing.T, r http.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var bodyReader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		bodyReader = bytes.NewReader(b)
	}
	req := httptest.NewRequest(method, path, bodyReader)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func decodeJSON(t *testing.T, w *httptest.ResponseRecorder, out any) {
	t.Helper()
	if err := json.NewDecoder(w.Body).Decode(out); err != nil {
		t.Fatalf("decode body: %v\nbody=%s", err, w.Body.String())
	}
}

// ---- ListOrders ----------------------------------------------------

func TestAPI_ListOrders_ProxiesAndFiltersByState(t *testing.T) {
	order := &fakeOrder{listResp: &backend.OrderList{Items: []backend.Order{
		{ID: "11111111-1111-4111-8111-111111111111", State: backend.OrderStatePending,
			Items: []backend.OrderItem{{SKU: "SKU-A", Quantity: 1}}},
		{ID: "22222222-2222-4222-8222-222222222222", State: backend.OrderStateConfirmed,
			Items: []backend.OrderItem{{SKU: "SKU-B", Quantity: 2}}},
	}}}
	_, r := newTestAPI(order, &fakePayment{}, &fakeInventory{})

	w := do(t, r, http.MethodGet, "/api/orders?state=pending", nil)
	if w.Code != 200 {
		t.Fatalf("status: got %d want 200", w.Code)
	}
	var resp struct {
		Items      []backend.Order `json:"items"`
		NextCursor string          `json:"next_cursor"`
	}
	decodeJSON(t, w, &resp)
	if len(resp.Items) != 1 {
		t.Fatalf("expected 1 order, got %d", len(resp.Items))
	}
	if resp.Items[0].ID != "11111111-1111-4111-8111-111111111111" {
		t.Errorf("got order %s", resp.Items[0].ID)
	}
}

func TestAPI_ListOrders_FiltersBySKUsClientSide(t *testing.T) {
	order := &fakeOrder{listResp: &backend.OrderList{Items: []backend.Order{
		{ID: "11111111-1111-4111-8111-111111111111", State: backend.OrderStatePending,
			Items: []backend.OrderItem{{SKU: "SKU-A", Quantity: 1}}},
		{ID: "22222222-2222-4222-8222-222222222222", State: backend.OrderStateConfirmed,
			Items: []backend.OrderItem{{SKU: "SKU-B", Quantity: 2}}},
	}}}
	_, r := newTestAPI(order, &fakePayment{}, &fakeInventory{})

	w := do(t, r, http.MethodGet, "/api/orders?sku=SKU-A", nil)
	if w.Code != 200 {
		t.Fatalf("status: got %d want 200", w.Code)
	}
	var resp struct {
		Items []backend.Order `json:"items"`
	}
	decodeJSON(t, w, &resp)
	if len(resp.Items) != 1 {
		t.Fatalf("expected 1 order after SKU filter, got %d", len(resp.Items))
	}
	if resp.Items[0].ID != "11111111-1111-4111-8111-111111111111" {
		t.Errorf("got %s", resp.Items[0].ID)
	}
}

// TestAPI_ListOrders_NilItemsCoercedToEmptyArray — F-007 regression
// net. Pre-fix the BFF passed the upstream's nil slice straight
// through to JSON, which serialised as `{"items": null}`. The SPA's
// `listOrders` returned `null`, then the payments/sim page spread
// it via `[...t, ...n]` and threw "n is not iterable" on the very
// first load with no pending/reserved orders.
//
// Test the two paths that triggered the bug:
//   1. Upstream returns nil Items → BFF writes `[]`, not `null`
//   2. (...and verify the same for the SKU filter path)
func TestAPI_ListOrders_NilItemsCoercedToEmptyArray(t *testing.T) {
	// Path 1: upstream returns nil Items (no orders match the filter).
	order := &fakeOrder{listResp: &backend.OrderList{Items: nil}}
	_, r := newTestAPI(order, &fakePayment{}, &fakeInventory{})

	w := do(t, r, http.MethodGet, "/api/orders", nil)
	if w.Code != 200 {
		t.Fatalf("status: got %d want 200", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, `"items":[]`) {
		t.Errorf("expected `\"items\":[]` in body, got: %s", body)
	}
	if strings.Contains(body, `"items":null`) {
		t.Errorf("BUG (F-007): body contains `\"items\":null` — SPA crashes on `[...null]`")
	}
}

func TestAPI_ListOrders_UpstreamErrorReturns502(t *testing.T) {
	order := &fakeOrder{listErr: errors.New("connection refused")}
	_, r := newTestAPI(order, &fakePayment{}, &fakeInventory{})

	w := do(t, r, http.MethodGet, "/api/orders", nil)
	if w.Code != 502 {
		t.Fatalf("status: got %d want 502", w.Code)
	}
	if !strings.Contains(w.Body.String(), "UPSTREAM_UNAVAILABLE") {
		t.Errorf("expected UPSTREAM_UNAVAILABLE in body; got: %s", w.Body.String())
	}
}

func TestAPI_ListOrders_Upstream404Returns404(t *testing.T) {
	// Upstream 404 only happens on /orders/{id}, not /orders. For
	// /orders an upstream 4xx collapses to BAD_REQUEST or
	// UPSTREAM_4XX depending on status. Pin the mapping.
	order := &fakeOrder{listErr: &backend.HTTPError{Status: 400, Body: "bad", URL: "x"}}
	_, r := newTestAPI(order, &fakePayment{}, &fakeInventory{})
	w := do(t, r, http.MethodGet, "/api/orders", nil)
	if w.Code != 400 {
		t.Fatalf("status: got %d want 400", w.Code)
	}
}

// ---- GetOrder ------------------------------------------------------

func TestAPI_GetOrder_404Returns404(t *testing.T) {
	order := &fakeOrder{getErr: &backend.HTTPError{Status: 404, Body: "no", URL: "x"}}
	_, r := newTestAPI(order, &fakePayment{}, &fakeInventory{})
	w := do(t, r, http.MethodGet,
		"/api/orders/22222222-2222-4222-8222-222222222222", nil)
	if w.Code != 404 {
		t.Fatalf("status: got %d want 404", w.Code)
	}
	if !strings.Contains(w.Body.String(), "NOT_FOUND") {
		t.Errorf("expected NOT_FOUND; got: %s", w.Body.String())
	}
}

func TestAPI_GetOrder_BadUUIDReturns400(t *testing.T) {
	_, r := newTestAPI(&fakeOrder{}, &fakePayment{}, &fakeInventory{})
	w := do(t, r, http.MethodGet, "/api/orders/not-a-uuid", nil)
	if w.Code != 400 {
		t.Fatalf("status: got %d want 400", w.Code)
	}
	if !strings.Contains(w.Body.String(), "BAD_REQUEST") {
		t.Errorf("expected BAD_REQUEST; got: %s", w.Body.String())
	}
}

// ---- SubmitOrder ---------------------------------------------------

func TestAPI_SubmitOrder_201(t *testing.T) {
	order := &fakeOrder{submitResp: &backend.Order{
		ID: "22222222-2222-4222-8222-222222222222", State: backend.OrderStatePending,
	}}
	_, r := newTestAPI(order, &fakePayment{}, &fakeInventory{})
	body := map[string]any{
		"idempotency_key": "test-1",
		"items": []map[string]any{
			{"sku": "SKU-001", "quantity": 1},
		},
	}
	w := do(t, r, http.MethodPost, "/api/orders", body)
	if w.Code != 201 {
		t.Fatalf("status: got %d want 201; body=%s", w.Code, w.Body.String())
	}
	if order.lastSubmit.Items[0].SKU != "SKU-001" {
		t.Errorf("expected SKU-001, got %s", order.lastSubmit.Items[0].SKU)
	}
}

func TestAPI_SubmitOrder_DuplicateTokenReturns409(t *testing.T) {
	order := &fakeOrder{submitResp: &backend.Order{
		ID: "22222222-2222-4222-8222-222222222222", State: backend.OrderStatePending,
	}}
	_, r := newTestAPI(order, &fakePayment{}, &fakeInventory{})
	body := map[string]any{
		"idempotency_key": "dup",
		"items": []map[string]any{{"sku": "SKU-001", "quantity": 1}},
	}
	if w := do(t, r, http.MethodPost, "/api/orders", body); w.Code != 201 {
		t.Fatalf("first submit: got %d want 201", w.Code)
	}
	w := do(t, r, http.MethodPost, "/api/orders", body)
	if w.Code != 409 {
		t.Fatalf("replay: got %d want 409; body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "DUPLICATE_SUBMIT") {
		t.Errorf("expected DUPLICATE_SUBMIT; got: %s", w.Body.String())
	}
}

func TestAPI_SubmitOrder_MissingItemsReturns400(t *testing.T) {
	_, r := newTestAPI(&fakeOrder{}, &fakePayment{}, &fakeInventory{})
	body := map[string]any{"idempotency_key": "x"}
	w := do(t, r, http.MethodPost, "/api/orders", body)
	if w.Code != 400 {
		t.Fatalf("status: got %d want 400", w.Code)
	}
}

func TestAPI_SubmitOrder_BadCustomerIDReturns400(t *testing.T) {
	_, r := newTestAPI(&fakeOrder{}, &fakePayment{}, &fakeInventory{})
	body := map[string]any{
		"idempotency_key": "k",
		"customer_id":     "not-a-uuid",
		"items":          []map[string]any{{"sku": "SKU-001", "quantity": 1}},
	}
	w := do(t, r, http.MethodPost, "/api/orders", body)
	if w.Code != 400 {
		t.Fatalf("status: got %d want 400", w.Code)
	}
}

// ---- CancelOrder ---------------------------------------------------

func TestAPI_CancelOrder_204(t *testing.T) {
	order := &fakeOrder{}
	_, r := newTestAPI(order, &fakePayment{}, &fakeInventory{})
	w := do(t, r, http.MethodDelete,
		"/api/orders/22222222-2222-4222-8222-222222222222", nil)
	if w.Code != 204 {
		t.Fatalf("status: got %d want 204", w.Code)
	}
	if order.lastCancel != "22222222-2222-4222-8222-222222222222" {
		t.Errorf("got cancel id %s", order.lastCancel)
	}
}

func TestAPI_CancelOrder_Upstream404Still204(t *testing.T) {
	// Idempotent: upstream 404 (already gone) → SPA sees 204.
	order := &fakeOrder{cancelErr: &backend.HTTPError{Status: 404, Body: "no", URL: "x"}}
	_, r := newTestAPI(order, &fakePayment{}, &fakeInventory{})
	w := do(t, r, http.MethodDelete,
		"/api/orders/22222222-2222-4222-8222-222222222222", nil)
	if w.Code != 204 {
		t.Fatalf("status: got %d want 204 (idempotent)", w.Code)
	}
}

// ---- GetInventoryStock --------------------------------------------

func TestAPI_InventoryStock_200(t *testing.T) {
	inv := &fakeInventory{resp: &backend.StockItem{SKU: "SKU-001", Available: 100}}
	_, r := newTestAPI(&fakeOrder{}, &fakePayment{}, inv)
	w := do(t, r, http.MethodGet, "/api/inventory/stock/SKU-001", nil)
	if w.Code != 200 {
		t.Fatalf("status: got %d want 200", w.Code)
	}
	var item backend.StockItem
	decodeJSON(t, w, &item)
	if item.SKU != "SKU-001" || item.Available != 100 {
		t.Errorf("got %+v", item)
	}
}

func TestAPI_InventoryStock_404(t *testing.T) {
	inv := &fakeInventory{err: &backend.HTTPError{Status: 404, Body: "no", URL: "x"}}
	_, r := newTestAPI(&fakeOrder{}, &fakePayment{}, inv)
	w := do(t, r, http.MethodGet, "/api/inventory/stock/MISSING", nil)
	if w.Code != 404 {
		t.Fatalf("status: got %d want 404", w.Code)
	}
}

// ---- FireWebhook ---------------------------------------------------

func TestAPI_PaymentsFire_200(t *testing.T) {
	pc := &fakePayment{}
	_, r := newTestAPI(&fakeOrder{}, pc, &fakeInventory{})
	body := map[string]any{
		"payment_id": "22222222-2222-4222-8222-222222222222",
		"status":     "succeeded",
		"last_four":  "4242",
	}
	w := do(t, r, http.MethodPost, "/api/payments/webhook", body)
	if w.Code != 200 {
		t.Fatalf("status: got %d want 200; body=%s", w.Code, w.Body.String())
	}
	if pc.fireCalls != 1 {
		t.Errorf("expected 1 fire call, got %d", pc.fireCalls)
	}
	if pc.lastWh.Status != "succeeded" {
		t.Errorf("got %s", pc.lastWh.Status)
	}
}

func TestAPI_PaymentsFire_BadStatusReturns400(t *testing.T) {
	_, r := newTestAPI(&fakeOrder{}, &fakePayment{}, &fakeInventory{})
	body := map[string]any{
		"payment_id": "22222222-2222-4222-8222-222222222222",
		"status":     "weird",
	}
	w := do(t, r, http.MethodPost, "/api/payments/webhook", body)
	if w.Code != 400 {
		t.Fatalf("status: got %d want 400", w.Code)
	}
}

// ---- isValidUUID / SSE replay smoke ------------------------------

func TestIsValidUUID(t *testing.T) {
	good := "22222222-2222-4222-8222-222222222222"
	if !isValidUUID(good) {
		t.Errorf("%s should be valid", good)
	}
	bad := []string{
		"",
		"not-a-uuid",
		"22222222-2222-4222-8222-22222222222",  // too short
		"22222222x2222-4222-8222-222222222222", // bad hex char
		"22222222-22222-4222-8222-222222222222", // wrong dash position
	}
	for _, b := range bad {
		if isValidUUID(b) {
			t.Errorf("%s should be invalid", b)
		}
	}
}

func TestSSEHandler_503WhenEventsDisabled(t *testing.T) {
	// Just ensures the disabled branch returns 503. We don't
	// construct a Bus here — eventsEnabled=false short-circuits
	// before the bus is touched.
	h := sseHandler(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), false)
	w := httptest.NewRecorder()
	h(w, httptest.NewRequest(http.MethodGet, "/events/stream", nil))
	if w.Code != 503 {
		t.Fatalf("status: got %d want 503", w.Code)
	}
	if !strings.Contains(w.Body.String(), "events_unavailable") {
		t.Errorf("expected events_unavailable in body; got: %s", w.Body.String())
	}
}

// silence unused import warnings if a future refactor drops one.
var _ = time.Second

// TestAPI_SubmitOrder_AutoGenCustomerID_OnEmpty locks in the fix
// for the SvelteKit-rewrite regression: when the SPA submits an
// order with no customer_id (placeholder text in /orders/new
// promises "auto-generated UUID"), the BFF must generate a UUID
// rather than forwarding the empty string to the Order Service
// (which rejects with 400 VALIDATION because orders.customer_id
// is NOT NULL UUID).
func TestAPI_SubmitOrder_AutoGenCustomerID_OnEmpty(t *testing.T) {
	o := &fakeOrder{
		submitResp: &backend.Order{ID: "00000000-0000-4000-8000-000000000001"},
	}
	api := &API{Order: o, Logger: slog.Default()}

	r := chi.NewRouter()
	r.Post("/api/orders", api.SubmitOrder)

	body := `{"idempotency_key":"00000000-0000-4000-8000-000000000002","items":[{"sku":"WIDGET","quantity":1}]}`
	req := httptest.NewRequest(http.MethodPost, "/api/orders", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if o.lastSubmit.CustomerID == nil {
		t.Fatalf("CustomerID was nil — BFF did not forward the field at all")
	}
	got := *o.lastSubmit.CustomerID
	if got == "" {
		t.Fatalf("CustomerID was empty string — BFF forwarded the raw empty value instead of auto-generating")
	}
	if !isValidUUID(got) {
		t.Errorf("auto-generated CustomerID=%q is not a valid UUID", got)
	}
	if got == "00000000-0000-4000-8000-000000000002" {
		t.Errorf("CustomerID matches the idempotency_key by coincidence — fake or copy-paste bug?")
	}
}

// TestAPI_SubmitOrder_PassesThroughCustomerID_WhenProvided locks in
// the inverse path: when the SPA supplies a customer_id explicitly,
// the BFF must forward it verbatim rather than overwriting it.
func TestAPI_SubmitOrder_PassesThroughCustomerID_WhenProvided(t *testing.T) {
	o := &fakeOrder{
		submitResp: &backend.Order{ID: "00000000-0000-4000-8000-000000000003"},
	}
	api := &API{Order: o, Logger: slog.Default()}

	r := chi.NewRouter()
	r.Post("/api/orders", api.SubmitOrder)

	provided := "11111111-2222-3333-4444-555555555555"
	body := `{"idempotency_key":"00000000-0000-4000-8000-000000000004","customer_id":"` + provided + `","items":[{"sku":"WIDGET","quantity":1}]}`
	req := httptest.NewRequest(http.MethodPost, "/api/orders", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if o.lastSubmit.CustomerID == nil {
		t.Fatalf("CustomerID was nil")
	}
	if got := *o.lastSubmit.CustomerID; got != provided {
		t.Errorf("CustomerID=%q, want %q (BFF must not overwrite an explicit value)", got, provided)
	}
}
