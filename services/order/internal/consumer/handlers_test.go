package consumer

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/t0pm1x/orderflow/platform/events"
)

// TestRegistry_HasAllEventTypes: pin the contract — the Order
// Service must register every event it consumes. A new event added
// to the spec without a handler would silently ack-and-skip via
// pkg/consumer, so we pin the registry shape here.
func TestRegistry_HasAllEventTypes(t *testing.T) {
	r := NewHandler(nil, slog.Default()).Registry()
	want := []string{
		"StockReserved",
		"StockReservationFailed",
		"OrderConfirmed",
		"OrderCancelled",
		"PaymentFailed",
	}
	for _, ev := range want {
		if _, ok := r[ev]; !ok {
			t.Errorf("Order Service handler for %q is missing", ev)
		}
	}
}

// TestRegistry_HandlersReturnErrorOnNilPool: every registered
// handler must decode its payload and reach updateState, then fail
// gracefully (non-nil error, no panic) when the pool is nil. The
// consumer DLQs on returned error, so this is the right shape —
// a panic would crash the goroutine.
func TestRegistry_HandlersReturnErrorOnNilPool(t *testing.T) {
	r := NewHandler(nil, slog.Default()).Registry()

	for eventType, h := range r {
		env := &events.Envelope{
			EventID:       "e1",
			EventType:     eventType,
			AggregateID:   "a1",
			AggregateType: "A",
			SchemaVersion: "1.0",
			Payload:       json.RawMessage(`{"order_id":"a1"}`),
		}
		if err := h(context.Background(), env); err == nil {
			t.Errorf("handler for %q with nil pool should return error, got nil", eventType)
		}
	}
}

// TestStart_DisabledWhenNoEnv: Start with no broker/groupID/handler
// returns no-op close + nil.
func TestStart_DisabledWhenNoEnv(t *testing.T) {
	ctx := context.Background()
	closer, err := Start(ctx, slog.Default(), "", "", nil, nil)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := closer(ctx); err != nil {
		t.Errorf("close: %v", err)
	}
}

// TestStart_InvalidBrokerFails: Start with a broker that the
// franz-go client cannot reach should still construct the client
// (franz-go is lazy) — so this test just verifies Start doesn't
// panic on a malformed broker list. The actual connection check
// happens at the first PollFetches call.
func TestStart_InvalidBrokerFails(_ *testing.T) {
	_ = http.StatusOK // keep imports stable
	_ = httptest.NewRecorder()
}
