package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/t0pm1x/orderflow/platform/events"
	"github.com/t0pm1x/orderflow/services/saga"
	"github.com/t0pm1x/orderflow/services/saga/internal/outbox"
	"github.com/t0pm1x/orderflow/services/saga/internal/repository"
)

// Integration tests for the saga consumer handlers. These exercise
// the FULL handler body (state-transition + outbox emission) against
// a real Postgres so we catch the regression class the prior
// v1.1.1 batch missed: handlers that emit downstream events
// unconditionally on every delivery. Without the per-state
// guard, every Kafka redelivery produces a second PaymentRequested
// (and a second Charge() call downstream).

// testDB opens a connection pool, applies migrations, and truncates
// the saga tables. Mirrors the harness pattern.
func testHandlerDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set; skipping saga handler integration test")
	}
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)

	if err := applyHandlerMigrations(t, pool); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if _, err := pool.Exec(context.Background(),
		`TRUNCATE TABLE order_sagas, saga_outbox RESTART IDENTITY`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return pool
}

func applyHandlerMigrations(t *testing.T, pool *pgxpool.Pool) error {
	t.Helper()
	repoRoot, err := findHandlerRepoRoot()
	if err != nil {
		return err
	}
	migDir := filepath.Join(repoRoot, "services", "saga", "migrations")
	entries, err := os.ReadDir(migDir)
	if err != nil {
		return fmt.Errorf("read migrations dir: %w", err)
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)
	if len(files) == 0 {
		return fmt.Errorf("no .sql migrations found in %s", migDir)
	}
	for _, f := range files {
		body, err := os.ReadFile(filepath.Join(migDir, f))
		if err != nil {
			return err
		}
		if _, err := pool.Exec(context.Background(), string(body)); err != nil {
			return fmt.Errorf("exec %s: %w", f, err)
		}
	}
	return nil
}

func findHandlerRepoRoot() (string, error) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", errors.New("cannot determine source file")
	}
	dir := filepath.Dir(thisFile)
	for i := 0; i < 8; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.work")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", errors.New("go.work not found above handlers_test.go")
}

// seedSaga inserts a saga row directly (bypassing InsertTx) so tests
// can drive handler bodies from a known state.
func seedSaga(t *testing.T, pool *pgxpool.Pool, orderID string, state saga.State, items []byte) {
	t.Helper()
	if _, err := pool.Exec(context.Background(),
		`INSERT INTO order_sagas (order_id, state, items, total_cents, reservation_id, expires_at)
		 VALUES ($1, $2, $3::jsonb, 0, '', NOW() + INTERVAL '5 minutes')`,
		orderID, string(state), items); err != nil {
		t.Fatalf("seed saga: %v", err)
	}
}

// countOutboxFor returns the number of saga_outbox rows for a given
// order_id and (optionally) event_type.
func countOutboxFor(t *testing.T, pool *pgxpool.Pool, orderID, eventType string) int {
	t.Helper()
	var n int
	if eventType == "" {
		if err := pool.QueryRow(context.Background(),
			`SELECT COUNT(*) FROM saga_outbox WHERE aggregate_id = $1`, orderID,
		).Scan(&n); err != nil {
			t.Fatalf("count outbox: %v", err)
		}
	} else {
		if err := pool.QueryRow(context.Background(),
			`SELECT COUNT(*) FROM saga_outbox WHERE aggregate_id = $1 AND event_type = $2`,
			orderID, eventType,
		).Scan(&n); err != nil {
			t.Fatalf("count outbox: %v", err)
		}
	}
	return n
}

// stockReservedEnvelope builds a minimal StockReserved envelope
// for testing the consumer handler.
func stockReservedEnvelope(orderID string) *events.Envelope {
	body := []byte(fmt.Sprintf(`{"order_id":"%s"}`, orderID))
	return &events.Envelope{
		EventID:       "test-event-" + orderID,
		EventType:     "StockReserved",
		AggregateID:   orderID,
		AggregateType: "Order",
		SchemaVersion: "1.0",
		Payload:       body,
	}
}

func paymentCompletedEnvelope(orderID string) *events.Envelope {
	body := []byte(fmt.Sprintf(`{"order_id":"%s"}`, orderID))
	return &events.Envelope{
		EventID:       "test-event-" + orderID,
		EventType:     "PaymentCompleted",
		AggregateID:   orderID,
		AggregateType: "Order",
		SchemaVersion: "1.0",
		Payload:       body,
	}
}

func paymentFailedEnvelope(orderID string) *events.Envelope {
	body := []byte(fmt.Sprintf(`{"order_id":"%s"}`, orderID))
	return &events.Envelope{
		EventID:       "test-event-" + orderID,
		EventType:     "PaymentFailed",
		AggregateID:   orderID,
		AggregateType: "Order",
		SchemaVersion: "1.0",
		Payload:       body,
	}
}

func orderCreatedEnvelope(orderID string) *events.Envelope {
	itemsJSON, _ := json.Marshal([]map[string]any{
		{"sku": "SKU-A", "quantity": 1, "unit_price_cents": 1000},
	})
	body := []byte(fmt.Sprintf(
		`{"order_id":"%s","customer_id":"00000000-0000-0000-0000-000000000000","items":%s,"total_cents":1000}`,
		orderID, string(itemsJSON)))
	return &events.Envelope{
		EventID:       "test-event-" + orderID,
		EventType:     "OrderCreated",
		AggregateID:   orderID,
		AggregateType: "Order",
		SchemaVersion: "1.0",
		Payload:       body,
	}
}

// TestStockReservedHandler_Idempotent_OnReplay is the regression
// guard for P0-#2 from the v1.1.1 audit: a redelivered
// StockReserved event must NOT emit a second PaymentRequested
// outbox row. Pre-fix, the handler called an unguarded UPDATE
// which matched any state, then unconditionally emitted — every
// Kafka redelivery caused a duplicate PaymentRequested, which
// downstream became a duplicate Charge() in the payment service.
func TestStockReservedHandler_Idempotent_OnReplay(t *testing.T) {
	pool := testHandlerDB(t)
	repo := repository.NewPGRepo(pool)
	writer := outbox.NewPGWriter()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	h := NewHandler(pool, logger).WithRepoAndWriter(repo, writer)

	const orderID = "11111111-1111-1111-1111-111111111111"
	seedSaga(t, pool, orderID, saga.StateInitiated, []byte(`[{"sku":"A","quantity":1}]`))
	env := stockReservedEnvelope(orderID)

	// First delivery: state advances and a PaymentRequested is
	// queued for publish.
	if err := h.StockReservedHandler(context.Background(), env); err != nil {
		t.Fatalf("first StockReserved: %v", err)
	}
	if got := countOutboxFor(t, pool, orderID, "PaymentRequested"); got != 1 {
		t.Fatalf("after first delivery: PaymentRequested rows = %d want 1", got)
	}

	// Second delivery (Kafka rebalance, consumer restart, etc.):
	// must NOT emit a second PaymentRequested. Pre-fix this was 2.
	if err := h.StockReservedHandler(context.Background(), env); err != nil {
		t.Fatalf("redelivered StockReserved: %v", err)
	}
	if got := countOutboxFor(t, pool, orderID, "PaymentRequested"); got != 1 {
		t.Errorf("after replay: PaymentRequested rows = %d want 1 (handler must be idempotent at outbox level)", got)
	}
}

// TestPaymentCompletedHandler_Idempotent_OnReplay: a redelivered
// PaymentCompleted must NOT emit a second OrderConfirmed.
func TestPaymentCompletedHandler_Idempotent_OnReplay(t *testing.T) {
	pool := testHandlerDB(t)
	repo := repository.NewPGRepo(pool)
	writer := outbox.NewPGWriter()
	h := NewHandler(pool, slog.New(slog.NewTextHandler(os.Stderr, nil))).WithRepoAndWriter(repo, writer)

	const orderID = "22222222-2222-2222-2222-222222222222"
	seedSaga(t, pool, orderID, saga.StateStockReserved, []byte(`[{"sku":"A","quantity":1}]`))
	env := paymentCompletedEnvelope(orderID)

	if err := h.PaymentCompletedHandler(context.Background(), env); err != nil {
		t.Fatalf("first PaymentCompleted: %v", err)
	}
	if got := countOutboxFor(t, pool, orderID, "OrderConfirmed"); got != 1 {
		t.Fatalf("after first: OrderConfirmed rows = %d want 1", got)
	}
	if err := h.PaymentCompletedHandler(context.Background(), env); err != nil {
		t.Fatalf("redelivered PaymentCompleted: %v", err)
	}
	if got := countOutboxFor(t, pool, orderID, "OrderConfirmed"); got != 1 {
		t.Errorf("after replay: OrderConfirmed rows = %d want 1", got)
	}
}

// TestPaymentFailedHandler_Idempotent_OnReplay: a redelivered
// PaymentFailed must NOT emit a second StockReleaseRequested +
// OrderCancelled.
func TestPaymentFailedHandler_Idempotent_OnReplay(t *testing.T) {
	pool := testHandlerDB(t)
	repo := repository.NewPGRepo(pool)
	writer := outbox.NewPGWriter()
	h := NewHandler(pool, slog.New(slog.NewTextHandler(os.Stderr, nil))).WithRepoAndWriter(repo, writer)

	const orderID = "33333333-3333-3333-3333-333333333333"
	seedSaga(t, pool, orderID, saga.StateStockReserved,
		[]byte(`[{"sku":"A","quantity":2},{"sku":"B","quantity":1}]`))
	env := paymentFailedEnvelope(orderID)

	if err := h.PaymentFailedHandler(context.Background(), env); err != nil {
		t.Fatalf("first PaymentFailed: %v", err)
	}
	if got := countOutboxFor(t, pool, orderID, "StockReleaseRequested"); got != 2 {
		t.Errorf("after first: StockReleaseRequested rows = %d want 2 (per item)", got)
	}
	if got := countOutboxFor(t, pool, orderID, "OrderCancelled"); got != 1 {
		t.Errorf("after first: OrderCancelled rows = %d want 1", got)
	}
	if err := h.PaymentFailedHandler(context.Background(), env); err != nil {
		t.Fatalf("redelivered PaymentFailed: %v", err)
	}
	if got := countOutboxFor(t, pool, orderID, "StockReleaseRequested"); got != 2 {
		t.Errorf("after replay: StockReleaseRequested rows = %d want 2 (still 2, no duplicate)", got)
	}
	if got := countOutboxFor(t, pool, orderID, "OrderCancelled"); got != 1 {
		t.Errorf("after replay: OrderCancelled rows = %d want 1 (no duplicate)", got)
	}
}

// TestStockReservationFailedHandler_Idempotent_OnReplay: a
// redelivered StockReservationFailed must NOT emit a second
// OrderCancelled.
func TestStockReservationFailedHandler_Idempotent_OnReplay(t *testing.T) {
	pool := testHandlerDB(t)
	repo := repository.NewPGRepo(pool)
	writer := outbox.NewPGWriter()
	h := NewHandler(pool, slog.New(slog.NewTextHandler(os.Stderr, nil))).WithRepoAndWriter(repo, writer)

	const orderID = "44444444-4444-4444-4444-444444444444"
	seedSaga(t, pool, orderID, saga.StateInitiated, []byte(`[{"sku":"A","quantity":1}]`))
	body := []byte(fmt.Sprintf(`{"order_id":"%s"}`, orderID))
	env := &events.Envelope{
		EventID:       "test-event",
		EventType:     "StockReservationFailed",
		AggregateID:   orderID,
		AggregateType: "Order",
		SchemaVersion: "1.0",
		Payload:       body,
	}

	if err := h.StockReservationFailedHandler(context.Background(), env); err != nil {
		t.Fatalf("first: %v", err)
	}
	if got := countOutboxFor(t, pool, orderID, "OrderCancelled"); got != 1 {
		t.Fatalf("after first: OrderCancelled rows = %d want 1", got)
	}
	if err := h.StockReservationFailedHandler(context.Background(), env); err != nil {
		t.Fatalf("redelivered: %v", err)
	}
	if got := countOutboxFor(t, pool, orderID, "OrderCancelled"); got != 1 {
		t.Errorf("after replay: OrderCancelled rows = %d want 1 (no duplicate)", got)
	}
}

// TestOrderCreatedHandler_DuplicateKeyReturnsErrorAndNoEmit:
// OrderCreated uses InsertTx — a duplicate order_id is a unique
// violation. The handler returns the error so the consumer
// retries → DLQ rather than silently ack-skipping and never
// emitting StockReserveRequested. Without this, a retry after
// a crash between Insert and emit would leave the order in the
// HTTP response but the saga never starting.
func TestOrderCreatedHandler_DuplicateKeyReturnsErrorAndNoEmit(t *testing.T) {
	pool := testHandlerDB(t)
	repo := repository.NewPGRepo(pool)
	writer := outbox.NewPGWriter()
	h := NewHandler(pool, slog.New(slog.NewTextHandler(os.Stderr, nil))).WithRepoAndWriter(repo, writer)

	const orderID = "55555555-5555-5555-5555-555555555555"
	env := orderCreatedEnvelope(orderID)

	// First delivery succeeds.
	if err := h.OrderCreatedHandler(context.Background(), env); err != nil {
		t.Fatalf("first OrderCreated: %v", err)
	}
	if got := countOutboxFor(t, pool, orderID, "StockReserveRequested"); got != 1 {
		t.Fatalf("after first: StockReserveRequested rows = %d want 1", got)
	}

	// Second delivery: InsertTx hits a unique violation. Handler
	// MUST return the error (not nil), so the consumer retries
	// and eventually DLQs — preserving observability.
	err := h.OrderCreatedHandler(context.Background(), env)
	if err == nil {
		t.Fatal("duplicate OrderCreated must return error, got nil")
	}
	// Outbox count unchanged (the duplicate insert rolled back).
	if got := countOutboxFor(t, pool, orderID, "StockReserveRequested"); got != 1 {
		t.Errorf("after duplicate: StockReserveRequested rows = %d want 1", got)
	}
}
