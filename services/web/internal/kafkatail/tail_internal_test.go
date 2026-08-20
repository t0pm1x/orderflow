package kafkatail

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	pkgEvents "github.com/t0pm1x/orderflow/platform/events"
	"github.com/t0pm1x/orderflow/services/web/internal/events"
)

// TestForwardToBus_ZeroOccurredAt_DefaultsToNow guards against the
// v1.1.3 saga-timeline "all zeros" regression. Producers across
// the stack (order/saga/inventory/payment) currently do not set
// Envelope.OccurredAt when emitting events, so the field arrives
// in the JSON-bus as the Go zero-time. Without this defensive
// default the bus stores 0001-01-01 timestamps and the order
// events page renders every entry as 00:00:00. The consumer
// default keeps the UI usable until each producer is updated
// to populate the field at emission time.
func TestForwardToBus_ZeroOccurredAt_DefaultsToNow(t *testing.T) {
	bus := events.NewBus()
	defer bus.Close()

	ch, unsub := bus.Subscribe()
	defer unsub()

	h := forwardToBus(bus)
	env := &pkgEvents.Envelope{
		EventID:       uuid.NewString(),
		EventType:     "OrderCreated",
		AggregateID:   "test-order-" + uuid.NewString(),
		AggregateType: "Order",
		SchemaVersion: "1.0",
		// OccurredAt intentionally omitted -> zero value
	}
	if err := h(context.Background(), env); err != nil {
		t.Fatalf("h: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	select {
	case got := <-ch:
		if got.Envelope.OccurredAt.IsZero() {
			t.Errorf("bus event has zero OccurredAt; consumer default did not fire")
		}
		if time.Since(got.Envelope.OccurredAt) > 5*time.Second {
			t.Errorf("bus event OccurredAt=%v is older than the test setup (regression: default was not applied at the receive point)",
				got.Envelope.OccurredAt)
		}
	case <-ctx.Done():
		t.Fatal("no event in 2s")
	}
}

// TestForwardToBus_PreservesExistingOccurredAt covers the no-op
// case: when the producer DOES set OccurredAt (the eventual
// corrected state), the consumer must not overwrite it.
func TestForwardToBus_PreservesExistingOccurredAt(t *testing.T) {
	bus := events.NewBus()
	defer bus.Close()

	ch, unsub := bus.Subscribe()
	defer unsub()

	h := forwardToBus(bus)
	want := time.Date(2026, 8, 20, 12, 34, 56, 0, time.UTC)
	env := &pkgEvents.Envelope{
		EventID:       uuid.NewString(),
		EventType:     "OrderConfirmed",
		AggregateID:   "test-order-" + uuid.NewString(),
		AggregateType: "Order",
		SchemaVersion: "1.0",
		OccurredAt:    want,
	}
	if err := h(context.Background(), env); err != nil {
		t.Fatalf("h: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	select {
	case got := <-ch:
		if !got.Envelope.OccurredAt.Equal(want) {
			t.Errorf("forwardToBus overwrote producer-set OccurredAt: got %v want %v",
				got.Envelope.OccurredAt, want)
		}
	case <-ctx.Done():
		t.Fatal("no event in 2s")
	}
}
