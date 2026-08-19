package handlers_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/t0pm1x/orderflow/services/web/internal/backend"
	"github.com/t0pm1x/orderflow/services/web/internal/events"
	"github.com/t0pm1x/orderflow/services/web/internal/handlers"
)

type fakeOrderClient struct {
	listResp       *backend.OrderList
	listErr        error
	listErrPending error
	listErrReserved error
	submitResp     *backend.Order
	submitErr      error
	submitCallsN   int
	getResp        *backend.Order
	getErr         error
	getCalls       int
	cancelCalls    int
	cancelErr      error
	lastCancel     string
	lastSubmit     *backend.OrderSubmit
}

func (f *fakeOrderClient) List(_ context.Context, state backend.OrderState, _ int) (*backend.OrderList, error) {
	if f.listErr != nil {
		return f.listResp, f.listErr
	}
	if state == backend.OrderStatePending && f.listErrPending != nil {
		return f.listResp, f.listErrPending
	}
	if state == backend.OrderStateReserved && f.listErrReserved != nil {
		return f.listResp, f.listErrReserved
	}
	if state == "" || f.listResp == nil {
		return f.listResp, f.listErr
	}
	filtered := &backend.OrderList{}
	for _, o := range f.listResp.Items {
		if o.State == state {
			filtered.Items = append(filtered.Items, o)
		}
	}
	return filtered, nil
}
func (f *fakeOrderClient) Get(_ context.Context, _ string) (*backend.Order, error) {
	f.getCalls++
	return f.getResp, f.getErr
}
func (f *fakeOrderClient) Submit(_ context.Context, in backend.OrderSubmit) (*backend.Order, error) {
	f.submitCallsN++
	cp := in
	f.lastSubmit = &cp
	return f.submitResp, f.submitErr
}
func (f *fakeOrderClient) Cancel(_ context.Context, id string) error {
	f.cancelCalls++
	f.lastCancel = id
	if f.cancelErr != nil {
		return f.cancelErr
	}
	return nil
}
func (f *fakeOrderClient) submitCalls() int { return f.submitCallsN }

func ptrInt64(v int64) *int64 { return &v }

type fakePaymentClient struct {
	lastWebhook *backend.PaymentWebhook
	fireCallsN  int
	fireErr     error
}

func (f *fakePaymentClient) FireWebhook(_ context.Context, w backend.PaymentWebhook) error {
	f.fireCallsN++
	cp := w
	f.lastWebhook = &cp
	if f.fireErr != nil {
		return f.fireErr
	}
	return nil
}
func (f *fakePaymentClient) fireCalls() int { return f.fireCallsN }

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
	h := handlers.NewSet(oc, &fakePaymentClient{}, ic, bus, slog.Default())
	h.SetEventsEnabled(true)
	r := chi.NewRouter()
	h.Routes(r)
	return r
}

func newTestSet(t *testing.T, oc backend.OrderClient) http.Handler {
	return newTestSetWith(t, oc, &fakeInventoryClient{})
}

// newTestSetWithEvents wires a Set with a specific EventsEnabled
// value so tests can exercise the "Kafka tail disabled" branch
// (sidebar badge + SSE 503).
func newTestSetWithEvents(t *testing.T, oc backend.OrderClient, enabled bool) http.Handler {
	t.Helper()
	bus := events.NewBus()
	h := handlers.NewSet(oc, &fakePaymentClient{}, &fakeInventoryClient{}, bus, slog.Default())
	h.SetEventsEnabled(enabled)
	r := chi.NewRouter()
	h.Routes(r)
	return r
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
	form := strings.NewReader("sku=X&quantity=1&idempotency_token=submit-ok-tok")
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
	form := strings.NewReader("sku=&quantity=0&idempotency_token=submit-val-tok")
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
	form := strings.NewReader("sku=X&quantity=1&idempotency_token=submit-5xx-tok")
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
	form := strings.NewReader("sku=X&quantity=1&idempotency_token=submit-4xx-tok")
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
		ID: uuid.MustParse("11111111-1111-4111-8111-111111111111").String(), State: backend.OrderStateReserved,
		Items: []backend.OrderItem{{SKU: "SKU-001", Quantity: 2, UnitPriceCents: ptrInt64(1999)}},
	}
	srv := httptest.NewServer(newTestSet(t, oc))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/orders/11111111-1111-4111-8111-111111111111")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("status: got %d want 200", resp.StatusCode)
	}
	b := new(strings.Builder)
	_, _ = io.Copy(b, resp.Body)
	if !strings.Contains(b.String(), "11111111-1111-4111-8111-111111111111") {
		t.Error("missing id")
	}
	if !strings.Contains(b.String(), "reserved") {
		t.Error("missing state badge")
	}
}

func TestOrderDetail_NotFound(t *testing.T) {
	oc := &fakeOrderClient{}
	oc.getErr = &backend.HTTPError{Status: 404, Body: "not found", URL: "http://order/v1/orders/22222222-2222-4222-8222-222222222222"}
	srv := httptest.NewServer(newTestSet(t, oc))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/orders/22222222-2222-4222-8222-222222222222")
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
	idUUID := uuid.MustParse("33333333-3333-4333-8333-333333333333").String()
	srv := httptest.NewServer(newTestSet(t, oc))
	defer srv.Close()
	form := strings.NewReader("idempotency_token=cancel-ok-tok")
	req, _ := http.NewRequest("POST", srv.URL+"/v1/orders/"+idUUID, form)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("status: got %d want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("HX-Redirect"); got != "/orders/"+idUUID {
		t.Errorf("HX-Redirect: got %q want /orders/%s", got, idUUID)
	}
	if oc.cancelCalls != 1 {
		t.Errorf("Cancel calls: got %d want 1", oc.cancelCalls)
	}
	if oc.lastCancel != idUUID {
		t.Errorf("Cancel id: got %q want %s (handler must forward the chi URL param)", oc.lastCancel, idUUID)
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
	idUUID := uuid.MustParse("66666666-6666-4666-8666-666666666666").String()
	set := handlers.NewSet(&fakeOrderClient{}, pc, &fakeInventoryClient{}, events.NewBus(), slog.Default())
	r := chi.NewRouter()
	set.Routes(r)
	srv := httptest.NewServer(r)
	defer srv.Close()
	form := strings.NewReader("order_id=" + idUUID + "&status=failed&error_code=card_declined&idempotency_token=fire-ok-tok")
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
	if pc.lastWebhook.PaymentID != idUUID {
		t.Errorf("PaymentID determinism: got %q want %s", pc.lastWebhook.PaymentID, idUUID)
	}
}

// TestOrderNew_GET_RendersIdempotencyToken covers the form-render
// contract: GET /orders/new embeds a per-render hidden input named
// `idempotency_token` so htmx submits carry a server-issued nonce
// the BFF can dedupe. Without it the double-submit replay cache
// has nothing to key on.
func TestOrderNew_GET_RendersIdempotencyToken(t *testing.T) {
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
	body := b.String()
	if !strings.Contains(body, `name="idempotency_token"`) {
		t.Errorf("form missing idempotency_token hidden field: %s", body)
	}
}

// TestOrderDetail_GET_RendersIdempotencyToken covers the cancel
// button: while the order is non-terminal, the cancel form must
// embed an `idempotency_token` so a double-click on Cancel cannot
// re-fire the upstream cancel call.
func TestOrderDetail_GET_RendersIdempotencyToken(t *testing.T) {
	oc := &fakeOrderClient{}
	idUUID := uuid.MustParse("44444444-4444-4444-8444-444444444444").String()
	oc.getResp = &backend.Order{
		ID:    idUUID,
		State: backend.OrderStateReserved,
		Items: []backend.OrderItem{{SKU: "SKU-001", Quantity: 2}},
	}
	srv := httptest.NewServer(newTestSet(t, oc))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/orders/" + idUUID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("status: got %d", resp.StatusCode)
	}
	b := new(strings.Builder)
	_, _ = io.Copy(b, resp.Body)
	body := b.String()
	if !strings.Contains(body, `name="idempotency_token"`) {
		t.Errorf("cancel form missing idempotency_token hidden field: %s", body)
	}
	if !strings.Contains(body, "hx-disabled-elt") {
		t.Errorf("cancel button missing hx-disabled-elt: %s", body)
	}
}

// TestPaymentsSim_GET_RendersIdempotencyTokens covers the force ✓
// / force ✗ buttons: every row's force forms must embed a fresh
// `idempotency_token` so double-click on either button cannot
// re-fire the upstream webhook.
func TestPaymentsSim_GET_RendersIdempotencyTokens(t *testing.T) {
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
		t.Fatalf("status: got %d", resp.StatusCode)
	}
	b := new(strings.Builder)
	_, _ = io.Copy(b, resp.Body)
	body := b.String()
	// two forms per row (force ✓ + force ✗) => two tokens per row
	if c := strings.Count(body, `name="idempotency_token"`); c != 2 {
		t.Errorf("idempotency_token count: got %d want 2 (one per force form): %s", c, body)
	}
	if !strings.Contains(body, "hx-disabled-elt") {
		t.Errorf("force buttons missing hx-disabled-elt: %s", body)
	}
}

// TestOrderSubmit_DuplicateToken_409 covers the BFF-level replay
// guard: posting the same `idempotency_token` twice to /v1/orders
// must yield 200 on the first call (Submit called once) and 409
// on the second (Submit NOT called again, replay cache hit). This
// is the P0.3 double-submit protection contract.
func TestOrderSubmit_DuplicateToken_409(t *testing.T) {
	oc := &fakeOrderClient{
		submitResp: &backend.Order{
			ID:    "order-dup",
			State: backend.OrderStatePending,
			Items: []backend.OrderItem{{SKU: "X", Quantity: 1}},
		},
	}
	srv := httptest.NewServer(newTestSet(t, oc))
	defer srv.Close()

	post := func(tok string) *http.Response {
		t.Helper()
		form := strings.NewReader("sku=X&quantity=1&idempotency_token=" + tok)
		req, _ := http.NewRequest("POST", srv.URL+"/v1/orders", form)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST /v1/orders: %v", err)
		}
		return resp
	}
	first := post("dup-submit-tok-1")
	defer func() { _ = first.Body.Close() }()
	if first.StatusCode != 200 {
		t.Fatalf("first POST: got %d want 200", first.StatusCode)
	}
	second := post("dup-submit-tok-1")
	defer func() { _ = second.Body.Close() }()
	if second.StatusCode != 409 {
		t.Fatalf("replay POST: got %d want 409", second.StatusCode)
	}
	if oc.submitCalls() != 1 {
		t.Errorf("Submit called %d times, want 1 (replay must NOT hit backend)", oc.submitCalls())
	}
}

// TestOrderSubmit_MissingToken_400 covers the contract that the
// idempotency_token field is required on POST: a form posted
// without it must surface 400, never reach the backend.
func TestOrderSubmit_MissingToken_400(t *testing.T) {
	oc := &fakeOrderClient{
		submitResp: &backend.Order{ID: "x", State: backend.OrderStatePending},
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
	if resp.StatusCode != 400 {
		t.Fatalf("status: got %d want 400", resp.StatusCode)
	}
	if oc.submitCalls() != 0 {
		t.Errorf("Submit called %d times, want 0 (missing token must NOT hit backend)", oc.submitCalls())
	}
}

// TestOrderCancel_DuplicateToken_409 mirrors TestOrderSubmit_…409
// for the cancel action: same `idempotency_token` posted twice
// must yield 200 then 409; the second must not re-call Cancel.
func TestOrderCancel_DuplicateToken_409(t *testing.T) {
	oc := &fakeOrderClient{}
	idUUID := uuid.MustParse("55555555-5555-4555-8555-555555555555").String()
	srv := httptest.NewServer(newTestSet(t, oc))
	defer srv.Close()
	post := func() *http.Response {
		t.Helper()
		form := strings.NewReader("idempotency_token=dup-cancel-tok")
		req, _ := http.NewRequest("POST", srv.URL+"/v1/orders/"+idUUID, form)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST /v1/orders/%s: %v", idUUID, err)
		}
		return resp
	}
	first := post()
	defer func() { _ = first.Body.Close() }()
	if first.StatusCode != 200 {
		t.Fatalf("first POST: got %d want 200", first.StatusCode)
	}
	second := post()
	defer func() { _ = second.Body.Close() }()
	if second.StatusCode != 409 {
		t.Fatalf("replay POST: got %d want 409", second.StatusCode)
	}
	if oc.cancelCalls != 1 {
		t.Errorf("Cancel called %d times, want 1 (replay must NOT hit backend)", oc.cancelCalls)
	}
}

// TestPaymentsFire_DuplicateToken_409 mirrors the duplicate-token
// 409 contract for the force ✓/✗ buttons.
func TestPaymentsFire_DuplicateToken_409(t *testing.T) {
	pc := &fakePaymentClient{}
	idUUID := uuid.MustParse("77777777-7777-4777-8777-777777777777").String()
	set := handlers.NewSet(&fakeOrderClient{}, pc, &fakeInventoryClient{}, events.NewBus(), slog.Default())
	r := chi.NewRouter()
	set.Routes(r)
	srv := httptest.NewServer(r)
	defer srv.Close()
	post := func() *http.Response {
		t.Helper()
		form := strings.NewReader("order_id=" + idUUID + "&status=succeeded&idempotency_token=dup-fire-tok")
		req, _ := http.NewRequest("POST", srv.URL+"/payments/sim/fire", form)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST /payments/sim/fire: %v", err)
		}
		return resp
	}
	first := post()
	defer func() { _ = first.Body.Close() }()
	if first.StatusCode != 200 {
		t.Fatalf("first POST: got %d want 200", first.StatusCode)
	}
	second := post()
	defer func() { _ = second.Body.Close() }()
	if second.StatusCode != 409 {
		t.Fatalf("replay POST: got %d want 409", second.StatusCode)
	}
	if pc.fireCalls() != 1 {
		t.Errorf("FireWebhook called %d times, want 1 (replay must NOT hit backend)", pc.fireCalls())
	}
}

// TestOrderDetail_BadID_400 covers P0.4: a non-UUID {id} on
// GET /orders/{id} must be rejected with 400 by the BFF before
// reaching the Order service. A bareword or a malformed id
// would otherwise propagate straight into the upstream URL —
// refusing here closes the door to SSRF-via-path on the upstream
// and to garbage 404s on the BFF. The 400 body is plain text,
// not the layout (htmx swaps the body and the layout's error
// template is for form re-render — non-form GETs get a
// plain-text error per the http.Error convention).
func TestOrderDetail_BadID_400(t *testing.T) {
	oc := &fakeOrderClient{}
	srv := httptest.NewServer(newTestSet(t, oc))
	defer srv.Close()
	cases := []string{"not-a-uuid", "123", "0xdeadbeef", "SKU-001"}
	for _, id := range cases {
		t.Run(id, func(t *testing.T) {
			resp, err := http.Get(srv.URL + "/orders/" + id)
			if err != nil {
				t.Fatalf("GET /orders/%s: %v", id, err)
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != 400 {
				t.Fatalf("status: got %d want 400", resp.StatusCode)
			}
			b := new(strings.Builder)
			_, _ = io.Copy(b, resp.Body)
			if !strings.Contains(b.String(), "UUID") {
				t.Errorf("body should mention UUID, got: %s", b.String())
			}
			if oc.getCalls != 0 {
				t.Errorf("upstream Get called %d times, want 0 (bad UUID must NOT hit backend)", oc.getCalls)
			}
		})
	}
}

// TestOrderCancel_BadID_400 covers P0.4: a non-UUID {id} on
// POST /v1/orders/{id} must be rejected with 400 before the
// replay cache consumes the token, so a buggy client posting
// a junk id with a fresh token still gets the right message.
// (Replay-check ordering matters: token check stays first so
// duplicate-token replays still 409, but junk-id must 400.)
func TestOrderCancel_BadID_400(t *testing.T) {
	oc := &fakeOrderClient{}
	srv := httptest.NewServer(newTestSet(t, oc))
	defer srv.Close()
	form := strings.NewReader("idempotency_token=cancel-bad-id-tok")
	req, _ := http.NewRequest("POST", srv.URL+"/v1/orders/not-a-uuid", form)
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
	if !strings.Contains(b.String(), "UUID") {
		t.Errorf("body should mention UUID, got: %s", b.String())
	}
	if oc.cancelCalls != 0 {
		t.Errorf("upstream Cancel called %d times, want 0 (bad UUID must NOT hit backend)", oc.cancelCalls)
	}
}

// TestOrderCancel_GoodIDWithValidToken_200 covers the regression
// corner of the bad-id test: when {id} IS a valid UUID AND the
// idempotency token is fresh, the cancel must proceed and call
// the upstream. Without this sister test, a naive change that
// rejected all path payloads (uuid or not) would pass
// TestOrderCancel_BadID_400 but silently break the working path.
func TestOrderCancel_GoodIDWithValidToken_200(t *testing.T) {
	oc := &fakeOrderClient{}
	idUUID := uuid.MustParse("88888888-8888-4888-8888-888888888888").String()
	srv := httptest.NewServer(newTestSet(t, oc))
	defer srv.Close()
	form := strings.NewReader("idempotency_token=cancel-good-tok")
	req, _ := http.NewRequest("POST", srv.URL+"/v1/orders/"+idUUID, form)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("status: got %d want 200", resp.StatusCode)
	}
	if oc.cancelCalls != 1 {
		t.Errorf("upstream Cancel called %d times, want 1", oc.cancelCalls)
	}
	if oc.lastCancel != idUUID {
		t.Errorf("Cancel id: got %q want %s", oc.lastCancel, idUUID)
	}
}

// TestOrderSubmit_BadCustomerID_400 covers P0.4: a non-UUID
// customer_id on POST /v1/orders must be rejected with 400
// (the form re-renders, like the SKU/quantity check). A blank
// customer_id is still allowed — the handler auto-generates a
// UUID per the form's placeholder promise — so the bad-id
// branch is the empty-suppression-OR'd UUID validator.
func TestOrderSubmit_BadCustomerID_400(t *testing.T) {
	srv := httptest.NewServer(newTestSet(t, &fakeOrderClient{}))
	defer srv.Close()
	form := strings.NewReader("sku=X&quantity=1&customer_id=not-a-uuid&idempotency_token=submit-bad-cust-tok")
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
	if !strings.Contains(b.String(), "UUID") {
		t.Errorf("body should mention UUID, got: %s", b.String())
	}
}

// TestOrderSubmit_BlankCustomerID_GeneratesUUID pins the
// happy-path of the customer_id field: empty input must
// auto-generate a valid UUID (and not 400). A regression to
// "always require UUID" would break this contract — the form
// placeholder text on the page promises "leave blank for
// auto-generated UUID".
func TestOrderSubmit_BlankCustomerID_GeneratesUUID(t *testing.T) {
	oc := &fakeOrderClient{
		submitResp: &backend.Order{
			ID:    "auto-id",
			State: backend.OrderStatePending,
			Items: []backend.OrderItem{{SKU: "X", Quantity: 1}},
		},
	}
	srv := httptest.NewServer(newTestSet(t, oc))
	defer srv.Close()
	form := strings.NewReader("sku=X&quantity=1&customer_id=&idempotency_token=submit-blank-cust-tok")
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
	if oc.lastSubmit == nil {
		t.Fatal("Submit not called")
	}
	if oc.lastSubmit.CustomerID == nil {
		t.Fatal("CustomerID is nil — handler should auto-generate")
	}
	if _, err := uuid.Parse(*oc.lastSubmit.CustomerID); err != nil {
		t.Errorf("CustomerID is not a UUID: %q (%v)", *oc.lastSubmit.CustomerID, err)
	}
}

// TestPaymentsFire_BadOrderID_400 covers P0.4: a non-UUID
// order_id on POST /payments/sim/fire must be rejected with
// 400 before the replay cache consumes the token, and the
// payment client must NOT be called (an invalid order id
// could otherwise poison the upstream idempotency cache).
func TestPaymentsFire_BadOrderID_400(t *testing.T) {
	pc := &fakePaymentClient{}
	set := handlers.NewSet(&fakeOrderClient{}, pc, &fakeInventoryClient{}, events.NewBus(), slog.Default())
	r := chi.NewRouter()
	set.Routes(r)
	srv := httptest.NewServer(r)
	defer srv.Close()
	form := strings.NewReader("order_id=not-a-uuid&status=succeeded&idempotency_token=fire-bad-oid-tok")
	req, _ := http.NewRequest("POST", srv.URL+"/payments/sim/fire", form)
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
	if !strings.Contains(b.String(), "UUID") {
		t.Errorf("body should mention UUID, got: %s", b.String())
	}
	if pc.fireCalls() != 0 {
		t.Errorf("FireWebhook called %d times, want 0 (bad order_id must NOT hit payment client)", pc.fireCalls())
	}
}

// TestOrderSubmit_Upstream400_HidesRawBody covers the P1.1
// contract: when the upstream returns a 4xx with a payload that
// contains operator-debug string fragments (e.g. a stack trace),
// the BFF must surface a generic user-friendly message and MUST
// NOT echo any of those fragments in the rendered HTML. The body
// is preserved server-side in the slog log, not in the response.
func TestOrderSubmit_Upstream400_HidesRawBody(t *testing.T) {
	rawBody := "internal debug: stack trace here — DO NOT LEAK"
	oc := &fakeOrderClient{}
	oc.submitErr = &backend.HTTPError{Status: 400, Body: rawBody, URL: "http://order/v1/orders"}
	srv := httptest.NewServer(newTestSet(t, oc))
	defer srv.Close()
	form := strings.NewReader("sku=X&quantity=1&idempotency_token=submit-hide-body-tok")
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
	body := b.String()
	if strings.Contains(body, "stack trace") {
		t.Errorf("body leaked upstream payload 'stack trace': %s", body)
	}
	if strings.Contains(body, "internal debug") {
		t.Errorf("body leaked upstream payload 'internal debug': %s", body)
	}
	if strings.Contains(body, "DO NOT LEAK") {
		t.Errorf("body leaked upstream payload 'DO NOT LEAK': %s", body)
	}
}

// TestOrderCancel_Upstream5xx_HidesRawBody covers the cancel
// action's 5xx branch: when the upstream errors out with a 500 +
// debug payload, the BFF must respond 502 with a generic "try
// again" message and never echo the upstream body verbatim.
func TestOrderCancel_Upstream5xx_HidesRawBody(t *testing.T) {
	oc := &fakeOrderClient{}
	oc.cancelErr = &backend.HTTPError{Status: 500, Body: "db panic: nil pointer at saga.go:42", URL: "http://order/v1/orders/x"}
	srv := httptest.NewServer(newTestSet(t, oc))
	defer srv.Close()
	idUUID := uuid.MustParse("99999999-9999-4999-8999-999999999999").String()
	form := strings.NewReader("idempotency_token=cancel-hide-body-tok")
	req, _ := http.NewRequest("POST", srv.URL+"/v1/orders/"+idUUID, form)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 502 {
		t.Fatalf("status: got %d want 502", resp.StatusCode)
	}
	b := new(strings.Builder)
	_, _ = io.Copy(b, resp.Body)
	body := b.String()
	if strings.Contains(body, "nil pointer") {
		t.Errorf("body leaked upstream payload 'nil pointer': %s", body)
	}
	if strings.Contains(body, "saga.go") {
		t.Errorf("body leaked upstream payload 'saga.go': %s", body)
	}
}

// TestPaymentsFire_Upstream5xx_HidesRawBody mirrors the contract
// for the payment-fire action: 5xx upstream maps to 502 + generic
// message, and the raw body never escapes the handler.
func TestPaymentsFire_Upstream5xx_HidesRawBody(t *testing.T) {
	pc := &fakePaymentClient{}
	pc.fireErr = &backend.HTTPError{Status: 500, Body: "redis: connection refused at 10.0.0.5", URL: "http://payment/webhooks"}
	set := handlers.NewSet(&fakeOrderClient{}, pc, &fakeInventoryClient{}, events.NewBus(), slog.Default())
	r := chi.NewRouter()
	set.Routes(r)
	srv := httptest.NewServer(r)
	defer srv.Close()
	idUUID := uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").String()
	form := strings.NewReader("order_id=" + idUUID + "&status=succeeded&idempotency_token=fire-hide-body-tok")
	req, _ := http.NewRequest("POST", srv.URL+"/payments/sim/fire", form)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 502 {
		t.Fatalf("status: got %d want 502", resp.StatusCode)
	}
	b := new(strings.Builder)
	_, _ = io.Copy(b, resp.Body)
	body := b.String()
	if strings.Contains(body, "connection refused") {
		t.Errorf("body leaked upstream payload 'connection refused': %s", body)
	}
	if strings.Contains(body, "10.0.0.5") {
		t.Errorf("body leaked upstream payload '10.0.0.5': %s", body)
	}
}

// TestPageOrderDetail_UpstreamError_HidesRawBody covers the order
// detail page: when the upstream get returns a 5xx with a debug
// payload, the rendered HTML must not echo it. The status code
// is 502 (mapped), and the user message is the generic "try again"
// hint.
func TestPageOrderDetail_UpstreamError_HidesRawBody(t *testing.T) {
	oc := &fakeOrderClient{}
	oc.getErr = &backend.HTTPError{Status: 500, Body: "stack trace: goroutine 1 [running]: main.main", URL: "http://order/v1/orders/x"}
	srv := httptest.NewServer(newTestSet(t, oc))
	defer srv.Close()
	idUUID := uuid.MustParse("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb").String()
	resp, err := http.Get(srv.URL + "/orders/" + idUUID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 502 {
		t.Fatalf("status: got %d want 502", resp.StatusCode)
	}
	b := new(strings.Builder)
	_, _ = io.Copy(b, resp.Body)
	body := b.String()
	if strings.Contains(body, "stack trace") {
		t.Errorf("body leaked upstream payload 'stack trace': %s", body)
	}
	if strings.Contains(body, "goroutine") {
		t.Errorf("body leaked upstream payload 'goroutine': %s", body)
	}
}

// TestPageOrdersList_UpstreamError_HidesRawBody covers the list
// page: when the upstream list errors with a debug payload, the
// banner must use the generic 502 message and never echo the body.
func TestPageOrdersList_UpstreamError_HidesRawBody(t *testing.T) {
	oc := &fakeOrderClient{}
	oc.listErr = &backend.HTTPError{Status: 500, Body: "internal: OOM at line 42", URL: "http://order/v1/orders"}
	srv := httptest.NewServer(newTestSet(t, oc))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("status: got %d want 200 (list still renders layout on backend down)", resp.StatusCode)
	}
	b := new(strings.Builder)
	_, _ = io.Copy(b, resp.Body)
	body := b.String()
	if strings.Contains(body, "OOM") {
		t.Errorf("body leaked upstream payload 'OOM': %s", body)
	}
	if strings.Contains(body, "line 42") {
		t.Errorf("body leaked upstream payload 'line 42': %s", body)
	}
}

// TestPageInventory_UpstreamError_HidesRawBody mirrors the list
// page check for the inventory view: upstream list error must
// surface a generic banner, never the raw body.
func TestPageInventory_UpstreamError_HidesRawBody(t *testing.T) {
	oc := &fakeOrderClient{}
	oc.listErr = &backend.HTTPError{Status: 500, Body: "rotate secrets and try again", URL: "http://order/v1/orders"}
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
	body := b.String()
	if strings.Contains(body, "rotate secrets") {
		t.Errorf("body leaked upstream payload 'rotate secrets': %s", body)
	}
}

// TestPagePaymentsSim_UpstreamError_HidesRawBody covers the
// payments-sim page: when both list queries fail, the banner uses
// the generic "try again" hint and never echoes the raw debug
// payload from the upstream.
func TestPagePaymentsSim_UpstreamError_HidesRawBody(t *testing.T) {
	oc := &fakeOrderClient{}
	oc.listErr = &backend.HTTPError{Status: 500, Body: "trace: mysql deadlocks — secret info", URL: "http://order/v1/orders"}
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
	if strings.Contains(body, "mysql") {
		t.Errorf("body leaked upstream payload 'mysql': %s", body)
	}
	if strings.Contains(body, "deadlocks") {
		t.Errorf("body leaked upstream payload 'deadlocks': %s", body)
	}
	if strings.Contains(body, "secret info") {
		t.Errorf("body leaked upstream payload 'secret info': %s", body)
	}
}

// TestPagePaymentsSim_PartialFailure_HidesRawBody exercises the
// partial-failure branch: the pending list returns a 4xx (bad filter)
// while the reserved list succeeds. The handler must (a) hide the
// upstream's raw payload, (b) route the 4xx through mapUpstreamError
// so the operator sees a user-fixable message rather than the
// hardcoded 5xx "temporarily unavailable" hint, and (c) still render
// the reserved rows so the page stays usable.
func TestPagePaymentsSim_PartialFailure_HidesRawBody(t *testing.T) {
	oc := &fakeOrderClient{
		listResp: &backend.OrderList{Items: []backend.Order{
			{ID: "ord-res-1", State: backend.OrderStateReserved},
		}},
		listErrPending: &backend.HTTPError{Status: 422, Body: "internal: invalid_state_filter 'pendng' at parse.go:42", URL: "http://order/v1/orders?state=pendng"},
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
	for _, leak := range []string{"internal:", "invalid_state_filter", "parse.go", "pendng"} {
		if strings.Contains(body, leak) {
			t.Errorf("body leaked upstream payload %q: %s", leak, body)
		}
	}
	if !strings.Contains(body, "ord-res-1") {
		t.Errorf("expected successful reserved row to render: %s", body)
	}
	if !strings.Contains(body, "Partial backend failure") {
		t.Errorf("expected partial-failure banner: %s", body)
	}
	if strings.Contains(body, "temporarily unavailable") {
		t.Errorf("4xx leaked as 5xx 'temporarily unavailable': %s", body)
	}
}

// TestPageEventsStream_Disabled_Returns503 covers P1.2: when the
// Kafka tail isn't running (Set.EventsEnabled == false), GET
// /events/stream short-circuits with 503 + a JSON body of
// {"error":"events unavailable"} so the htmx-sse client can
// distinguish "events truly absent" from "server is broken".
// No SSE stream is opened and no heartbeat is emitted.
func TestPageEventsStream_Disabled_Returns503(t *testing.T) {
	srv := httptest.NewServer(newTestSetWithEvents(t, &fakeOrderClient{}, false))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/events/stream")
	if err != nil {
		t.Fatalf("GET /events/stream: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status: got %d want 503", resp.StatusCode)
	}
	ct := resp.Header.Get("Content-Type")
	if !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type: got %q want application/json", ct)
	}
	b := new(strings.Builder)
	_, _ = io.Copy(b, resp.Body)
	body := strings.TrimSpace(b.String())
	if body != `{"error":"events unavailable"}` {
		t.Errorf("body: got %q want %q", body, `{"error":"events unavailable"}`)
	}
	if strings.HasPrefix(ct, "text/event-stream") {
		t.Errorf("503 response leaked text/event-stream content type: %s", ct)
	}
}

// TestSidebar_Disabled_RendersBadge covers P1.2: with
// Set.EventsEnabled == false, the layout renders a "disconnected"
// badge next to the "Live events" heading and a muted paragraph
// explaining why. The page must remain a 200 — the playground is
// still usable, the operator just won't see live updates.
func TestSidebar_Disabled_RendersBadge(t *testing.T) {
	oc := &fakeOrderClient{listResp: &backend.OrderList{Items: []backend.Order{}}}
	srv := httptest.NewServer(newTestSetWithEvents(t, oc, false))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("status: got %d want 200 (page must still render)", resp.StatusCode)
	}
	b := new(strings.Builder)
	_, _ = io.Copy(b, resp.Body)
	body := b.String()
	if !strings.Contains(body, "disconnected") {
		t.Errorf("expected 'disconnected' badge in sidebar: %s", body)
	}
	if !strings.Contains(body, `class="badge cancelled"`) {
		t.Errorf("expected cancelled-badge styling: %s", body)
	}
	if !strings.Contains(body, "KAFKA_BROKERS") {
		t.Errorf("expected explanation referencing KAFKA_BROKERS: %s", body)
	}
}

// TestSidebar_Enabled_NoBadge is the regression-pole of the
// disconnected-badge test: with EventsEnabled == true, the sidebar
// MUST NOT show the disconnected badge. Without this sister test a
// naive change that always renders the badge would still pass
// TestSidebar_Disabled_RendersBadge but break the live-events
// happy path.
func TestSidebar_Enabled_NoBadge(t *testing.T) {
	oc := &fakeOrderClient{listResp: &backend.OrderList{Items: []backend.Order{}}}
	srv := httptest.NewServer(newTestSetWithEvents(t, oc, true))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("status: got %d want 200", resp.StatusCode)
	}
	b := new(strings.Builder)
	_, _ = io.Copy(b, resp.Body)
	body := b.String()
	if strings.Contains(body, "disconnected") {
		t.Errorf("did not expect 'disconnected' badge when events enabled: %s", body)
	}
	if strings.Contains(body, "KAFKA_BROKERS not set") {
		t.Errorf("did not expect disconnected explanation when events enabled: %s", body)
	}
}
