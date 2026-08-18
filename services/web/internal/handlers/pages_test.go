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
}

func (f *fakeOrderClient) List(ctx context.Context, _ backend.OrderState, _ int) (*backend.OrderList, error) {
	return f.listResp, f.listErr
}
func (f *fakeOrderClient) Get(_ context.Context, _ string) (*backend.Order, error) {
	return f.getResp, f.getErr
}
func (f *fakeOrderClient) Submit(_ context.Context, _ backend.OrderSubmit) (*backend.Order, error) {
	return f.submitResp, f.submitErr
}
func (f *fakeOrderClient) Cancel(_ context.Context, _ string) error {
	f.cancelCalls++
	return nil
}

func ptrInt64(v int64) *int64 { return &v }

type fakePaymentClient struct{}

func (f *fakePaymentClient) FireWebhook(_ context.Context, _ backend.PaymentWebhook) error { return nil }

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
	defer resp.Body.Close()
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
	defer resp.Body.Close()
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
	defer resp.Body.Close()
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
	defer resp.Body.Close()
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
	defer resp.Body.Close()
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
	defer resp.Body.Close()
	if resp.StatusCode != 502 {
		t.Fatalf("status: got %d want 502", resp.StatusCode)
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
	defer resp.Body.Close()
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
	defer resp.Body.Close()
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
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: got %d want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("HX-Redirect"); got != "/orders/order-1" {
		t.Errorf("HX-Redirect: got %q want /orders/order-1", got)
	}
	if oc.cancelCalls != 1 {
		t.Errorf("Cancel calls: got %d want 1", oc.cancelCalls)
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
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: got %d want 200", resp.StatusCode)
	}
	b := new(strings.Builder)
	_, _ = io.Copy(b, resp.Body)
	if !strings.Contains(b.String(), "SKU-001") {
		t.Errorf("missing SKU-001: %s", b.String())
	}
	if !strings.Contains(b.String(), "99") {
		t.Errorf("missing available qty 99: %s", b.String())
	}
}

func TestInventory_BackendError(t *testing.T) {
	oc := &fakeOrderClient{listErr: fmt.Errorf("upstream 503")}
	srv := httptest.NewServer(newTestSetWith(t, oc, &fakeInventoryClient{}))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/inventory")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
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