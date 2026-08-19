package backend_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
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
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"code":"bad","message":"nope"}`, http.StatusBadRequest)
	}))
	defer srv.Close()
	c := backend.New(nil, "http://localhost:8080", srv.URL, "http://localhost:8082")
	err := c.FireWebhook(context.Background(), backend.PaymentWebhook{PaymentID: "x", Status: "failed"})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

// TestPaymentClient_FireWebhook_SetsIdempotencyKey pins the contract
// the Payment Service requires: a POST to /v1/payments/webhook MUST
// carry an Idempotency-Key header, otherwise the idempotency
// middleware returns 400. The web UI "force ✓/✗" buttons on
// /payments/sim drive this client — without the header, every
// fire-webhook click surfaced a 400 to the user. We assert the
// header is set to the deterministic event signature
// "orderflow-web:{order_id}:{status}" so the provider mock can
// dedupe identical replays while a unique fire still produces a
// unique key.
func TestPaymentClient_FireWebhook_SetsIdempotencyKey(t *testing.T) {
	var seen atomic.Value // last observed Idempotency-Key
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.Store(r.Header.Get("Idempotency-Key"))
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	c := backend.New(nil, "http://localhost:8080", srv.URL, "http://localhost:8082")
	if err := c.FireWebhook(context.Background(), backend.PaymentWebhook{
		PaymentID: "order-1", Status: "succeeded",
	}); err != nil {
		t.Fatalf("FireWebhook: %v", err)
	}
	key, _ := seen.Load().(string)
	if key == "" {
		t.Fatal("Idempotency-Key: got \"\" want non-empty")
	}
	// Replay with the same signature — provider mock (and any
	// downstream idempotency-aware consumer) must see the same
	// key so it can dedupe.
	var seen2 atomic.Value
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen2.Store(r.Header.Get("Idempotency-Key"))
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv2.Close()
	c2 := backend.New(nil, "http://localhost:8080", srv2.URL, "http://localhost:8082")
	if err := c2.FireWebhook(context.Background(), backend.PaymentWebhook{
		PaymentID: "order-1", Status: "succeeded",
	}); err != nil {
		t.Fatalf("replay FireWebhook: %v", err)
	}
	if got, _ := seen.Load().(string); got == "" {
		t.Fatal("first key empty")
	}
	if got, _ := seen2.Load().(string); got != seen.Load().(string) {
		t.Errorf("replay key mismatch: %q vs %q — provider mock cannot dedupe", got, seen.Load().(string))
	}
}
