package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/t0pm1x/orderflow/platform/events"
)

func TestRegistry_HasAllEventTypes(t *testing.T) {
	r := Registry(slog.Default())
	want := []string{"PaymentRequested"}
	for _, ev := range want {
		if _, ok := r[ev]; !ok {
			t.Errorf("Payment Service handler for %q is missing", ev)
		}
	}
}

func TestRegistry_StubsLogAndReturnNil(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	r := Registry(logger)
	for eventType, h := range r {
		env := &events.Envelope{
			EventID:       "e1",
			EventType:     eventType,
			AggregateID:   "a1",
			AggregateType: "Payment",
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

func TestStart_DisabledWhenNoEnv(t *testing.T) {
	ctx := context.Background()
	closer, err := Start(ctx, slog.Default(), "", "")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := closer(ctx); err != nil {
		t.Errorf("close: %v", err)
	}
}
