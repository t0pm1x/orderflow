package backend_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/t0pm1x/orderflow/services/web/internal/backend"
)

func TestOrderClient_List(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/orders" || r.Method != http.MethodGet {
			http.Error(w, "unexpected path", http.StatusBadRequest)
			return
		}
		if got := r.URL.Query().Get("state"); got != "pending" {
			t.Errorf("query state: got %q want pending", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"items":[
			{"id":"` + uuid.NewString() + `","state":"pending","items":[{"sku":"SKU-001","quantity":2}],"created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z"}
		]}`))
	}))
	defer srv.Close()

	c := backend.New(nil, srv.URL, "http://localhost:8081", "http://localhost:8082")
	got, err := c.List(context.Background(), backend.OrderStatePending, 50)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got.Items) != 1 {
		t.Fatalf("items: got %d want 1", len(got.Items))
	}
}

func TestOrderClient_Get_NotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"code":"not_found","message":"order not found"}`, http.StatusNotFound)
	}))
	defer srv.Close()
	c := backend.New(nil, srv.URL, "http://localhost:8081", "http://localhost:8082")
	_, err := c.Get(context.Background(), "missing-id")
	if err == nil || !strings.Contains(err.Error(), "status 404") {
		t.Fatalf("expected 404 error, got %v", err)
	}
}

func TestOrderClient_Submit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/orders" {
			http.Error(w, "unexpected", http.StatusBadRequest)
			return
		}
		var in backend.OrderSubmit
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, "decode", http.StatusBadRequest)
			return
		}
		if len(in.Items) != 1 || in.Items[0].SKU != "SKU-001" {
			http.Error(w, "bad items", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(backend.Order{
			ID: uuid.NewString(), State: backend.OrderStatePending,
			Items: in.Items, CreatedAt: time.Now(), UpdatedAt: time.Now(),
		})
	}))
	defer srv.Close()
	c := backend.New(nil, srv.URL, "http://localhost:8081", "http://localhost:8082")
	got, err := c.Submit(context.Background(), backend.OrderSubmit{
		Items: []backend.OrderItem{{SKU: "SKU-001", Quantity: 1}},
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if got.State != backend.OrderStatePending {
		t.Errorf("state: got %s want pending", got.State)
	}
}

func TestOrderClient_Cancel_Idempotent(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(http.StatusNoContent) // 204 is OK
	}))
	defer srv.Close()
	c := backend.New(nil, srv.URL, "http://localhost:8081", "http://localhost:8082")
	if err := c.Cancel(context.Background(), "any"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if calls != 1 {
		t.Errorf("calls: got %d want 1", calls)
	}
}

func TestOrderClient_Cancel_404IsOK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	c := backend.New(nil, srv.URL, "http://localhost:8081", "http://localhost:8082")
	// Per OpenAPI, idempotent — 404 on cancel is acceptable.
	if err := c.Cancel(context.Background(), "missing"); err != nil {
		t.Fatalf("Cancel should accept 404, got: %v", err)
	}
}

// TestOrderClient_Cancel_UsesDELETE pins the wire-level cancel
// contract: HTTP method MUST be DELETE, path MUST be /v1/orders/{id}.
// The web UI form posts to /v1/orders/{id} on the BFF but the BFF
// proxies a DELETE to the upstream; a regression to POST or PUT
// would silently break the contract with the Order Service.
func TestOrderClient_Cancel_UsesDELETE(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("method: got %q want DELETE", r.Method)
		}
		if r.URL.Path != "/v1/orders/order-42" {
			t.Errorf("path: got %q want /v1/orders/order-42", r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	c := backend.New(nil, srv.URL, "http://localhost:8081", "http://localhost:8082")
	if err := c.Cancel(context.Background(), "order-42"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
}
