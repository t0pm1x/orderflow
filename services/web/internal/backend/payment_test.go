package backend_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"github.com/t0pm1x/orderflow/services/web/internal/backend"
)

func TestPaymentClient_FireWebhook_Succeed(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != http.MethodPost || r.URL.Path != "/v1/payments/webhook" {
			http.Error(w, "bad path", http.StatusBadRequest)
			return
		}
		var w2 backend.PaymentWebhook
		if err := json.NewDecoder(r.Body).Decode(&w2); err != nil {
			http.Error(w, "decode", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	c := backend.New(nil, "http://localhost:8080", srv.URL, "http://localhost:8082")
	if err := c.FireWebhook(context.Background(), backend.PaymentWebhook{
		PaymentID: uuid.NewString(), Status: "succeeded",
	}); err != nil {
		t.Fatalf("FireWebhook: %v", err)
	}
	if calls != 1 {
		t.Errorf("calls: got %d want 1", calls)
	}
}

func TestPaymentClient_FireWebhook_4xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"code":"bad","message":"nope"}`, http.StatusBadRequest)
	}))
	defer srv.Close()
	c := backend.New(nil, "http://localhost:8080", srv.URL, "http://localhost:8082")
	err := c.FireWebhook(context.Background(), backend.PaymentWebhook{PaymentID: "x", Status: "failed"})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}
