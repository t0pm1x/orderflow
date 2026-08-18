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
