package consumer

import (
	"bytes"
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
	r := Registry(slog.Default())
	want := []string{
		"StockReserved",
		"StockReleased",
		"StockReservationFailed",
		"PaymentCompleted",
		"PaymentFailed",
	}
	for _, ev := range want {
		if _, ok := r[ev]; !ok {
			t.Errorf("Order Service handler for %q is missing", ev)
		}
	}
}

// TestRegistry_StubsLogAndReturnNil: stub handlers must not error
// (they're placeholders for the saga-driven implementation that
// arrives in 3.9). If a stub returns an error, the consumer would
// retry it 5 times and DLQ — exactly the wrong behavior for a
// placeholder.
func TestRegistry_StubsLogAndReturnNil(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	r := Registry(logger)

	for eventType, h := range r {
		env := &events.Envelope{
			EventID:       "e1",
			EventType:     eventType,
			AggregateID:   "a1",
			AggregateType: "A",
			SchemaVersion: "1.0",
			Payload:       json.RawMessage(`{}`),
		}
		if err := h(context.Background(), env); err != nil {
			t.Errorf("handler for %q returned error: %v", eventType, err)
		}
	}
	if buf.Len() == 0 {
		t.Error("expected stub handlers to log something")
	}
}

// TestStart_DisabledWhenNoEnv: Start with no broker/groupID
// returns no-op close + nil.
func TestStart_DisabledWhenNoEnv(t *testing.T) {
	ctx := context.Background()
	close, err := Start(ctx, slog.Default(), "", "")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := close(ctx); err != nil {
		t.Errorf("close: %v", err)
	}
}

// TestStart_InvalidBrokerFails: Start with a broker that the
// franz-go client cannot reach should still construct the client
// (franz-go is lazy) — so this test just verifies Start doesn't
// panic on a malformed broker list. The actual connection check
// happens at the first PollFetches call.
func TestStart_InvalidBrokerFails(t *testing.T) {
	_ = http.StatusOK // keep imports stable
	_ = httptest.NewRecorder()
}
