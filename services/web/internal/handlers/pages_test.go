package handlers_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/t0pm1x/orderflow/services/web/internal/backend"
	"github.com/t0pm1x/orderflow/services/web/internal/events"
	"github.com/t0pm1x/orderflow/services/web/internal/handlers"
)

type fakeOrderClient struct {
	listResp    *backend.OrderList
	listErr     error
	submitResp  *backend.Order
	submitErr   error
	getResp     *backend.Order
	getErr      error
	cancelCalls int
	lastCancel  string
}

func (f *fakeOrderClient) List(_ context.Context, _ backend.OrderState, _ int) (*backend.OrderList, error) {
	return f.listResp, f.listErr
}
func (f *fakeOrderClient) Get(_ context.Context, _ string) (*backend.Order, error) {
	return f.getResp, f.getErr
}
func (f *fakeOrderClient) Submit(_ context.Context, _ backend.OrderSubmit) (*backend.Order, error) {
	return f.submitResp, f.submitErr
}
func (f *fakeOrderClient) Cancel(_ context.Context, id string) error {
	f.cancelCalls++
	f.lastCancel = id
	return nil
}

func ptrInt64(v int64) *int64 { return &v }

type fakePaymentClient struct{ lastWebhook *backend.PaymentWebhook }

func (f *fakePaymentClient) FireWebhook(_ context.Context, w backend.PaymentWebhook) error {
	f.lastWebhook = &w
	return nil
}

type fakeInventoryClient struct {
	stock map[string]*backend.StockItem
	err   error
}

func (f *fakeInventoryClient) GetStock(_ context.Context, sku string) (*backend.StockItem, error) {
	if f.err != nil {
		return nil, f.err
	}
	item, ok := f.stock[sku]
	if !ok {
		return nil, fmt.Errorf("not found: %s", sku)
	}
	return item, nil
}

func newTestSetWith(t *testing.T, oc backend.OrderClient, ic backend.InventoryClient) http.Handler {
	t.Helper()
	bus := events.NewBus()
	h := handlers.NewSet(oc, &fakePaymentClient{}, ic, bus)
	r := chi.NewRouter()
	h.Routes(r)
	return r
}

func newTestSet(t *testing.T, oc backend.OrderClient) http.Handler {
	return newTestSetWith(t, oc, &fakeInventoryClient{})
}

func TestOrdersList_OK(t *testing.T) {
	oc := &fakeOrderClient{
		listResp: &backend.OrderList{Items: []backend.Order{
			{ID: "abc-123", State: backend.OrderStatePending,
				Items: []backend.OrderItem{{SKU: "SKU-001", Quantity: 2}}},
			{ID: "def-456", State: backend.OrderStateConfirmed,
				Items: []backend.OrderItem{{SKU: "SKU-002", Quantity: 1}}},
		}},
	}
	srv := httptest.NewServer(newTestSet(t, oc))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("status: got %d want 200", resp.StatusCode)
	}
	body := new(strings.Builder)
	_, _ = io.Copy(body, resp.Body)
	if !strings.Contains(body.String(), "abc-123") {
		t.Errorf("missing abc-123 order: %s", body.String())
	}
	if !strings.Contains(body.String(), "confirmed") {
		t.Errorf("missing confirmed badge: %s", body.String())
	}
}

func TestOrdersList_BackendError_RendersPage(t *testing.T) {
	oc := &fakeOrderClient{listErr: errFake}
	srv := httptest.NewServer(newTestSet(t, oc))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("status: got %d want 200", resp.StatusCode)
	}
	body := new(strings.Builder)
	_, _ = io.Copy(body, resp.Body)
	if !strings.Contains(strings.ToLower(body.String()), "unavailable") &&
		!strings.Contains(strings.ToLower(body.String()), "backend") {
		t.Errorf("expected backend-unreachable notice: %s", body.String())
	}
}

type fakeErr struct{}

func (fakeErr) Error() string { return "upstream timeout" }

var errFake = fakeErr{}

func TestOrderNew_GET(t *testing.T) {
	srv := httptest.NewServer(newTestSet(t, &fakeOrderClient{}))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/orders/new")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("status: got %d", resp.StatusCode)
	}
	b := new(strings.Builder)
	_, _ = io.Copy(b, resp.Body)
	if !strings.Contains(b.String(), `name="sku"`) {
		t.Error("form missing sku field")
	}
	if !strings.Contains(b.String(), `name="quantity"`) {
		t.Error("form missing quantity")
	}
}

func TestOrderSubmit_OK_RedirectsViaHTMX(t *testing.T) {
	oc := &fakeOrderClient{
		submitResp: &backend.Order{
			ID:    "order-99",
			State: backend.OrderStatePending,
			Items: []backend.OrderItem{{SKU: "X", Quantity: 1}},
		},
	}
	srv := httptest.NewServer(newTestSet(t, oc))
	defer srv.Close()
	form := strings.NewReader("sku=X&quantity=1")
	req, _ := http.NewRequest("POST", srv.URL+"/v1/orders", form)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("status: got %d want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("HX-Redirect"); got != "/orders/order-99" {
		t.Errorf("HX-Redirect: got %q want /orders/order-99", got)
	}
}

func TestOrderSubmit_ValidationError(t *testing.T) {
	srv := httptest.NewServer(newTestSet(t, &fakeOrderClient{}))
	defer srv.Close()
	form := strings.NewReader("sku=&quantity=0")
	req, _ := http.NewRequest("POST", srv.URL+"/v1/orders", form)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 400 {
		t.Fatalf("status: got %d want 400", resp.StatusCode)
	}
	b := new(strings.Builder)
	_, _ = io.Copy(b, resp.Body)
	low := strings.ToLower(b.String())
	if !strings.Contains(low, "required") && !strings.Contains(low, "invalid") {
		t.Errorf("expected validation error: %s", b.String())
	}
}

func TestOrderSubmit_Upstream5xx(t *testing.T) {
	oc := &fakeOrderClient{}
	oc.submitErr = fmt.Errorf("upstream 503")
	srv := httptest.NewServer(newTestSet(t, oc))
	defer srv.Close()
	form := strings.NewReader("sku=X&quantity=1")
	req, _ := http.NewRequest("POST", srv.URL+"/v1/orders", form)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 502 {
		t.Fatalf("status: got %d want 502", resp.StatusCode)
	}
}

// TestOrderSubmit_Upstream4xx_BadRequest covers the typed-HTTPError
// path: when the upstream returns 4xx (user error — bad SKU, bad
// qty, schema mismatch), the BFF should surface 400 + form
// re-render so the user can fix their input, NOT 502.
func TestOrderSubmit_Upstream4xx_BadRequest(t *testing.T) {
	oc := &fakeOrderClient{}
	oc.submitErr = &backend.HTTPError{Status: 400, Body: "bad sku"}
	srv := httptest.NewServer(newTestSet(t, oc))
	defer srv.Close()
	form := strings.NewReader("sku=X&quantity=1")
	req, _ := http.NewRequest("POST", srv.URL+"/v1/orders", form)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 400 {
		t.Fatalf("status: got %d want 400", resp.StatusCode)
	}
}

func TestOrderDetail_OK(t *testing.T) {
	oc := &fakeOrderClient{}
	oc.getResp = &backend.Order{
		ID: "order-1", State: backend.OrderStateReserved,
		Items: []backend.OrderItem{{SKU: "SKU-001", Quantity: 2, UnitPriceCents: ptrInt64(1999)}},
	}
	srv := httptest.NewServer(newTestSet(t, oc))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/orders/order-1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("status: got %d want 200", resp.StatusCode)
	}
	b := new(strings.Builder)
	_, _ = io.Copy(b, resp.Body)
	if !strings.Contains(b.String(), "order-1") {
		t.Error("missing id")
	}
	if !strings.Contains(b.String(), "reserved") {
		t.Error("missing state badge")
	}
}

func TestOrderDetail_NotFound(t *testing.T) {
	oc := &fakeOrderClient{}
	oc.getErr = fmt.Errorf("upstream 404: status 404: not found")
	srv := httptest.NewServer(newTestSet(t, oc))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/orders/missing")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 404 {
		t.Fatalf("status: got %d want 404", resp.StatusCode)
	}
}

func TestOrderCancel_OK(t *testing.T) {
	oc := &fakeOrderClient{}
	oc.cancelCalls = 0
	srv := httptest.NewServer(newTestSet(t, oc))
	defer srv.Close()
	resp, err := http.Post(srv.URL+"/v1/orders/order-1", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("status: got %d want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("HX-Redirect"); got != "/orders/order-1" {
		t.Errorf("HX-Redirect: got %q want /orders/order-1", got)
	}
	if oc.cancelCalls != 1 {
		t.Errorf("Cancel calls: got %d want 1", oc.cancelCalls)
	}
	if oc.lastCancel != "order-1" {
		t.Errorf("Cancel id: got %q want order-1 (handler must forward the chi URL param)", oc.lastCancel)
	}
}

func TestInventory_OK(t *testing.T) {
	oc := &fakeOrderClient{
		listResp: &backend.OrderList{Items: []backend.Order{
			{ID: "ord-1", State: backend.OrderStatePending,
				Items: []backend.OrderItem{{SKU: "SKU-001", Quantity: 2}}},
			{ID: "ord-2", State: backend.OrderStateReserved,
				Items: []backend.OrderItem{{SKU: "SKU-002", Quantity: 1}}},
		}},
	}
	ic := &fakeInventoryClient{
		stock: map[string]*backend.StockItem{
			"SKU-001": {SKU: "SKU-001", Available: 99, Reserved: 1, Version: 3},
			"SKU-002": {SKU: "SKU-002", Available: 50, Reserved: 0, Version: 1},
		},
	}
	srv := httptest.NewServer(newTestSetWith(t, oc, ic))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/inventory")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("status: got %d want 200", resp.StatusCode)
	}
	b := new(strings.Builder)
	_, _ = io.Copy(b, resp.Body)
	for _, want := range []string{"SKU-001", "SKU-002", "99", "50"} {
		if !strings.Contains(b.String(), want) {
			t.Errorf("missing %q in body: %s", want, b.String())
		}
	}
}

// TestInventory_OrderBackendDown covers the order-side failure path:
// when Order.List errors, the handler renders a backend-unavailable
// banner and still returns 200 so the layout shell stays usable.
// Per-SKU inventory errors are handled separately by
// TestInventory_StockRowMissing (they degrade to Missing: true).
func TestInventory_OrderBackendDown(t *testing.T) {
	oc := &fakeOrderClient{listErr: fmt.Errorf("upstream 503")}
	srv := httptest.NewServer(newTestSetWith(t, oc, &fakeInventoryClient{}))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/inventory")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("status: got %d want 200", resp.StatusCode)
	}
	b := new(strings.Builder)
	_, _ = io.Copy(b, resp.Body)
	low := strings.ToLower(b.String())
	if !strings.Contains(low, "unavailable") && !strings.Contains(low, "backend") {
		t.Errorf("expected backend notice: %s", b.String())
	}
}

// TestInventory_StockRowMissing covers the per-SKU degradation path:
// the order side returns a SKU the inventory service has no row for
// (GetStock returns (nil, error)). The handler keeps the row in the
// list with Missing: true and renders &mdash; for the numeric columns
// rather than failing the whole page.
func TestInventory_StockRowMissing(t *testing.T) {
	oc := &fakeOrderClient{
		listResp: &backend.OrderList{Items: []backend.Order{
			{ID: "ord-1", State: backend.OrderStatePending,
				Items: []backend.OrderItem{{SKU: "SKU-001", Quantity: 2}}},
			{ID: "ord-2", State: backend.OrderStateReserved,
				Items: []backend.OrderItem{{SKU: "SKU-002", Quantity: 1}}},
		}},
	}
	ic := &fakeInventoryClient{
		stock: map[string]*backend.StockItem{
			"SKU-001": {SKU: "SKU-001", Available: 99, Reserved: 1, Version: 3},
		},
	}
	srv := httptest.NewServer(newTestSetWith(t, oc, ic))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/inventory")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("status: got %d want 200", resp.StatusCode)
	}
	b := new(strings.Builder)
	_, _ = io.Copy(b, resp.Body)
	body := b.String()
	if !strings.Contains(body, "SKU-001") {
		t.Errorf("missing resolved SKU-001 row: %s", body)
	}
	if !strings.Contains(body, "SKU-002") {
		t.Errorf("missing row SKU-002 should still appear: %s", body)
	}
	if !strings.Contains(body, "&mdash;") {
		t.Errorf("expected &mdash; rendering for missing row: %s", body)
	}
}

// TestPaymentsSim_OK covers GET /payments/sim: when the order backend
// returns a reserved order, the page renders the order id and the
// force-success / force-fail buttons. The page only lists orders in
// in-flight states (pending or reserved), so a reserved order must
// appear. The button label and id both come from the rendered HTML.
func TestPaymentsSim_OK(t *testing.T) {
	oc := &fakeOrderClient{
		listResp: &backend.OrderList{Items: []backend.Order{
			{ID: "o-1", State: backend.OrderStateReserved,
				Items: []backend.OrderItem{{SKU: "X", Quantity: 1}}},
		}},
	}
	srv := httptest.NewServer(newTestSet(t, oc))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/payments/sim")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("status: got %d want 200", resp.StatusCode)
	}
	b := new(strings.Builder)
	_, _ = io.Copy(b, resp.Body)
	body := b.String()
	if !strings.Contains(body, "o-1") {
		t.Errorf("missing order id: %s", body)
	}
	if !strings.Contains(body, "force") {
		t.Errorf("missing force buttons: %s", body)
	}
	if !strings.Contains(body, "/payments/sim/fire") {
		t.Errorf("missing fire action URL: %s", body)
	}
}

// TestPaymentsFire_OK covers POST /payments/sim/fire: builds a
// PaymentWebhook from the form (order_id + status + error_code) and
// proxies it to PaymentClient.FireWebhook. The handler must echo
// HX-Redirect to /payments/sim so htmx reloads the page, and the
// payment_id must be deterministic on order_id so the mock's replay
// guard accepts repeat fires for the same order.
func TestPaymentsFire_OK(t *testing.T) {
	pc := &fakePaymentClient{}
	set := handlers.NewSet(&fakeOrderClient{}, pc, &fakeInventoryClient{}, events.NewBus())
	r := chi.NewRouter()
	set.Routes(r)
	srv := httptest.NewServer(r)
	defer srv.Close()
	form := strings.NewReader("order_id=o-1&status=failed&error_code=card_declined")
	req, _ := http.NewRequest("POST", srv.URL+"/payments/sim/fire", form)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("status: got %d want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("HX-Redirect"); got != "/payments/sim" {
		t.Errorf("HX-Redirect: got %q want /payments/sim", got)
	}
	if pc.lastWebhook == nil {
		t.Fatal("webhook not fired")
	}
	if pc.lastWebhook.Status != "failed" {
		t.Errorf("Status: got %q want failed", pc.lastWebhook.Status)
	}
	if pc.lastWebhook.ErrorCode != "card_declined" {
		t.Errorf("ErrorCode: got %q want card_declined", pc.lastWebhook.ErrorCode)
	}
	if pc.lastWebhook.PaymentID != "o-1" {
		t.Errorf("PaymentID determinism: got %q want o-1", pc.lastWebhook.PaymentID)
	}
}
