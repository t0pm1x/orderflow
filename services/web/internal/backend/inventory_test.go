package backend_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/t0pm1x/orderflow/services/web/internal/backend"
)

func TestInventoryClient_GetStock(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/inventory/stock/SKU-001" || r.Method != http.MethodGet {
			http.Error(w, "bad path", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"sku":"SKU-001","available":99,"reserved":1,"version":3,"updated_at":"2026-01-01T00:00:00Z"
		}`))
	}))
	defer srv.Close()
	c := backend.New(nil, "http://localhost:8080", "http://localhost:8081", srv.URL)
	got, err := c.GetStock(context.Background(), "SKU-001")
	if err != nil {
		t.Fatalf("GetStock: %v", err)
	}
	if got.SKU != "SKU-001" || got.Available != 99 || got.Reserved != 1 || got.Version != 3 {
		t.Fatalf("unexpected: %+v", got)
	}
}

// TestInventoryClient_GetStock_PathEscape pins P0.4: a sku with
// URL-meaningful characters must be percent-escaped before being
// interpolated into /v1/inventory/stock/{sku}. A `/` in the sku
// would otherwise split the request across a deeper path; a `?`
// would split into a query. We assert against `r.URL.RawPath`
// (which preserves the on-the-wire encoding) since `r.URL.Path`
// is decoded by net/url for routing. A regression to plain
// `fmt.Sprintf` interpolation would leave `RawPath` empty (no
// bytes needing encoding in the segment) and the test fails.
func TestInventoryClient_GetStock_PathEscape(t *testing.T) {
	const sku = "WEIRD/SKU?with#anchor and%percent"
	const wantRaw = "/v1/inventory/stock/WEIRD%2FSKU%3Fwith%23anchor%20and%25percent"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RawPath != wantRaw {
			t.Errorf("RawPath: got %q want %q", r.URL.RawPath, wantRaw)
		}
		if r.URL.RawQuery != "" {
			t.Errorf("unexpected query %q (must be empty)", r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"sku":"WEIRD/SKU?with#anchor and%percent","available":1,"reserved":0,"version":1,"updated_at":"2026-01-01T00:00:00Z"
		}`))
	}))
	defer srv.Close()
	c := backend.New(nil, "http://localhost:8080", "http://localhost:8081", srv.URL)
	if _, err := c.GetStock(context.Background(), sku); err != nil {
		t.Fatalf("GetStock: %v", err)
	}
}
