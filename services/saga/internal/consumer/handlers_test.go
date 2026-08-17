package consumer

import (
	"context"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	pkgconsumer "github.com/t0pm1x/orderflow/consumer"
)

// TestRegistry_HasAllEventTypes pins the contract — the Saga
// Service must register every event it consumes. A new event added
// to the spec without a handler would silently ack-and-skip via
// pkg/consumer, so we pin the registry shape here. The set is
// the union of the brief's consumer list (OrderCreated,
// StockReserved, PaymentCompleted, PaymentFailed, StockReleased)
// plus StockReservationFailed which the existing saga state
// machine already handles as compensation.
func TestRegistry_HasAllEventTypes(t *testing.T) {
	r := newRegistryForTest()
	want := []string{
		"OrderCreated",
		"StockReserved",
		"PaymentCompleted",
		"PaymentFailed",
		"StockReleased",
		"StockReservationFailed",
	}
	for _, ev := range want {
		if _, ok := r[ev]; !ok {
			t.Errorf("Saga handler for %q is missing", ev)
		}
	}
}

// TestRegistry_NoUnexpectedEventTypes guards against handler
// drift: if a handler is added for an event the brief doesn't
// list, this fails so the new event is reviewed against the
// contract.
func TestRegistry_NoUnexpectedEventTypes(t *testing.T) {
	r := newRegistryForTest()
	want := map[string]bool{
		"OrderCreated":            true,
		"StockReserved":           true,
		"PaymentCompleted":        true,
		"PaymentFailed":           true,
		"StockReleased":           true,
		"StockReservationFailed":  true,
	}
	for ev := range r {
		if !want[ev] {
			t.Errorf("unexpected handler for %q", ev)
		}
	}
}

// TestStart_DisabledWhenNoEnv: Start with no broker/groupID/pool
// returns no-op close + nil. Matches services/order pattern.
func TestStart_DisabledWhenNoEnv(t *testing.T) {
	ctx := context.Background()
	close, err := Start(ctx, slog.Default(), "", "", nil)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := close(ctx); err != nil {
		t.Errorf("close: %v", err)
	}
}

// TestStart_DisabledWhenPoolNil: even with broker+groupID, a nil
// pool must short-circuit to no-op (no consumer dialed, no panic).
func TestStart_DisabledWhenPoolNil(t *testing.T) {
	ctx := context.Background()
	close, err := Start(ctx, slog.Default(), "broker:9092", "saga-group", nil)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := close(ctx); err != nil {
		t.Errorf("close: %v", err)
	}
}

// newRegistryForTest builds a Handler with a nil pool so we can
// iterate the registry keys without spinning up Postgres. The
// handlers themselves aren't invoked here — see the integration
// tests in pg_repo_test for handler-level coverage.
func newRegistryForTest() pkgconsumer.HandlerRegistry {
	var pool *pgxpool.Pool
	h := NewHandler(pool, slog.Default())
	return h.Registry()
}