package backend_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/t0pm1x/orderflow/services/web/internal/backend"
)

func TestInventoryClient_ListStock(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/inventory/stock" || r.Method != http.MethodGet {
			http.Error(w, "bad path", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[
			{"sku":"SKU-001","available_qty":99,"reserved_qty":1,"version":3},
			{"sku":"SKU-002","available_qty":50,"reserved_qty":0,"version":1}
		]`))
	}))
	defer srv.Close()
	c := backend.New(nil, "http://localhost:8080", "http://localhost:8081", srv.URL)
	got, err := c.ListStock(context.Background())
	if err != nil {
		t.Fatalf("ListStock: %v", err)
	}
	if len(got) != 2 || got[0].SKU != "SKU-001" {
		t.Fatalf("unexpected: %+v", got)
	}
}
