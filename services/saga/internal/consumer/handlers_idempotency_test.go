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
	sagaev "github.com/t0pm1x/orderflow/services/saga/internal/events"
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

// TestStockReleasedHandler_AckSkipsOnEmptyOrderID is the SAGA-2
// regression guard: a StockReleased payload with no order_id field
// must be acked-skipped, not retried. Pre-fix, decoding OrderID=""
// caused UPDATE WHERE order_id=” against the UUID column to raise
// SQLSTATE 22P02, blocking the saga consumer for 5 retries × 1s = 5s
// per cancelled order.
//
// Post-fix the handler logs a Warn and returns nil; the consumer
// commits the offset and moves on.
func TestStockReleasedHandler_AckSkipsOnEmptyOrderID(t *testing.T) {
	pool := testHandlerDB(t)
	repo := repository.NewPGRepo(pool)
	writer := outbox.NewPGWriter()
	h := NewHandler(pool, slog.New(slog.NewTextHandler(os.Stderr, nil))).WithRepoAndWriter(repo, writer)

	// No order_id field at all — exactly the legacy pre-SAGA-2 shape.
	env := &events.Envelope{
		EventID:       "test-stockreleased-empty",
		EventType:     "StockReleased",
		AggregateID:   "reservation-legacy",
		AggregateType: "Reservation",
		SchemaVersion: "1.0",
		Payload:       []byte(`{"reservation_id":"reservation-legacy","sku":"X","quantity":1,"reason":"order_cancelled"}`),
	}
	if err := h.StockReleasedHandler(context.Background(), env); err != nil {
		t.Fatalf("StockReleasedHandler with empty order_id must return nil; got %v", err)
	}
}

// TestStockReleasedHandler_CompensatedSagaStaysCompensated is the
// SAGA-2 happy-path guard: a StockReleased payload with a real
// order_id and a saga already in StateCompensated must remain
// compensated (TransitionStateTx is a compensated→compensated
// self-transition; no outbox row is emitted because no state
// advanced).
func TestStockReleasedHandler_CompensatedSagaStaysCompensated(t *testing.T) {
	pool := testHandlerDB(t)
	repo := repository.NewPGRepo(pool)
	writer := outbox.NewPGWriter()
	h := NewHandler(pool, slog.New(slog.NewTextHandler(os.Stderr, nil))).WithRepoAndWriter(repo, writer)

	const orderID = "77777777-aaaa-bbbb-cccc-777777777777"
	seedSaga(t, pool, orderID, saga.StateCompensated, []byte(`[{"sku":"A","quantity":1}]`))
	body := []byte(fmt.Sprintf(`{"order_id":%q,"reservation_id":"res-x","sku":"A","quantity":1,"reason":"order_cancelled"}`, orderID))
	env := &events.Envelope{
		EventID:       "test-stockreleased-happy",
		EventType:     "StockReleased",
		AggregateID:   "res-x",
		AggregateType: "Reservation",
		SchemaVersion: "1.0",
		Payload:       body,
	}
	if err := h.StockReleasedHandler(context.Background(), env); err != nil {
		t.Fatalf("StockReleasedHandler: %v", err)
	}
	got, err := repo.Get(context.Background(), orderID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.State != saga.StateCompensated {
		t.Errorf("state after StockReleased: got %q want compensated", got.State)
	}
	if n := countOutboxFor(t, pool, orderID, ""); n != 0 {
		t.Errorf("StockReleased must not emit a fresh outbox row; got %d rows", n)
	}
}

// TestPaymentCompletedHandler_EmitsRefundOnCompensatedSaga is the
// SAGA-4 regression guard: a PaymentCompleted event landing on an
// already-compensated saga must emit PaymentRefundRequested so the
// payment service can refund the captured charge. Pre-fix, the
// handler silently swallowed the event (the saga row was already
// terminal; TransitionStateTx returned (false, nil); the
// "skipping emit" branch ran). Result: customer charged for a
// cancelled order, no refund, silent money loss.
func TestPaymentCompletedHandler_EmitsRefundOnCompensatedSaga(t *testing.T) {
	pool := testHandlerDB(t)
	repo := repository.NewPGRepo(pool)
	writer := outbox.NewPGWriter()
	h := NewHandler(pool, slog.New(slog.NewTextHandler(os.Stderr, nil))).WithRepoAndWriter(repo, writer)

	const orderID = "c4444444-cccc-4444-cccc-444444444444"
	// Seed the saga in StateCompensated with a known TotalCents so
	// the refund payload's AmountCents matches.
	if _, err := pool.Exec(context.Background(),
		`INSERT INTO order_sagas (order_id, state, items, total_cents, reservation_id, expires_at)
		 VALUES ($1, 'compensated', '[]'::jsonb, 4200, 'res-refund', NOW() + INTERVAL '1 hour')`,
		orderID); err != nil {
		t.Fatalf("seed compensated saga: %v", err)
	}

	body := []byte(fmt.Sprintf(`{"order_id":%q,"payment_id":"pay-abc123"}`, orderID))
	env := &events.Envelope{
		EventID:       "test-payment-completed",
		EventType:     "PaymentCompleted",
		AggregateID:   orderID,
		AggregateType: "Order",
		SchemaVersion: "1.0",
		Payload:       body,
	}
	if err := h.PaymentCompletedHandler(context.Background(), env); err != nil {
		t.Fatalf("PaymentCompletedHandler: %v", err)
	}

	if got := countOutboxFor(t, pool, orderID, "PaymentRefundRequested"); got != 1 {
		t.Fatalf("PaymentRefundRequested rows: got %d want 1 (SAGA-4: must refund on compensated saga)", got)
	}
	if got := countOutboxFor(t, pool, orderID, "OrderConfirmed"); got != 0 {
		t.Errorf("OrderConfirmed rows on compensated saga: got %d want 0 (must NOT confirm a cancelled order)", got)
	}

	// Decode the refund payload to verify payment_id and amount
	// are propagated. The payment service uses these to issue the
	// refund against the exact captured transaction.
	rows, err := pool.Query(context.Background(),
		`SELECT payload FROM saga_outbox
		  WHERE aggregate_id = $1 AND event_type = 'PaymentRefundRequested'
		  ORDER BY id ASC LIMIT 1`, orderID)
	if err != nil {
		t.Fatalf("query outbox: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("PaymentRefundRequested outbox row missing")
	}
	var payload []byte
	if err := rows.Scan(&payload); err != nil {
		t.Fatalf("scan: %v", err)
	}
	var refund sagaev.PaymentRefundRequestedPayload
	if err := json.Unmarshal(payload, &refund); err != nil {
		t.Fatalf("decode refund payload: %v", err)
	}
	if refund.OrderID != orderID {
		t.Errorf("refund OrderID: got %q want %q", refund.OrderID, orderID)
	}
	if refund.PaymentID != "pay-abc123" {
		t.Errorf("refund PaymentID: got %q want pay-abc123", refund.PaymentID)
	}
	if refund.AmountCents != 4200 {
		t.Errorf("refund AmountCents: got %d want 4200", refund.AmountCents)
	}
}

// TestPaymentCompletedHandler_NoRefundOnCompletedSaga is the
// SAGA-4 negative guard: a PaymentCompleted event landing on an
// already-completed saga (e.g. redelivery after a successful
// terminal transition) must NOT emit a refund — the saga was
// finalized correctly the first time. Pre-fix, the handler
// silently swallowed this too; post-fix it still swallows but
// without emitting a spurious PaymentRefundRequested.
func TestPaymentCompletedHandler_NoRefundOnCompletedSaga(t *testing.T) {
	pool := testHandlerDB(t)
	repo := repository.NewPGRepo(pool)
	writer := outbox.NewPGWriter()
	h := NewHandler(pool, slog.New(slog.NewTextHandler(os.Stderr, nil))).WithRepoAndWriter(repo, writer)

	const orderID = "d5555555-dddd-5555-dddd-555555555555"
	if _, err := pool.Exec(context.Background(),
		`INSERT INTO order_sagas (order_id, state, items, total_cents, reservation_id, expires_at)
		 VALUES ($1, 'completed', '[]'::jsonb, 1000, 'res-done', NOW() + INTERVAL '1 hour')`,
		orderID); err != nil {
		t.Fatalf("seed completed saga: %v", err)
	}

	body := []byte(fmt.Sprintf(`{"order_id":%q,"payment_id":"pay-done"}`, orderID))
	env := &events.Envelope{
		EventID:       "test-payment-completed-completed",
		EventType:     "PaymentCompleted",
		AggregateID:   orderID,
		AggregateType: "Order",
		SchemaVersion: "1.0",
		Payload:       body,
	}
	if err := h.PaymentCompletedHandler(context.Background(), env); err != nil {
		t.Fatalf("PaymentCompletedHandler: %v", err)
	}

	if got := countOutboxFor(t, pool, orderID, "PaymentRefundRequested"); got != 0 {
		t.Errorf("PaymentRefundRequested on completed saga: got %d want 0 (refund is only for compensated sagas)", got)
	}
	if got := countOutboxFor(t, pool, orderID, "OrderConfirmed"); got != 0 {
		t.Errorf("OrderConfirmed on completed saga: got %d want 0", got)
	}
}

// TestOrderCancelledHandler_CompensatesSaga is the SAGA-5
// regression guard: a user-initiated OrderCancelled event must
// drive the saga to StateCompensated and emit StockReleaseRequested
// per item so inventory can release the reservation. Pre-fix the
// saga had no handler for OrderCancelled; the event was
// ack-and-skipped and the saga proceeded to charge the card.
func TestOrderCancelledHandler_CompensatesSaga(t *testing.T) {
	pool := testHandlerDB(t)
	repo := repository.NewPGRepo(pool)
	writer := outbox.NewPGWriter()
	h := NewHandler(pool, slog.New(slog.NewTextHandler(os.Stderr, nil))).WithRepoAndWriter(repo, writer)

	const orderID = "ee666666-eeee-6666-eeee-666666666666"
	seedSaga(t, pool, orderID, saga.StateStockReserved,
		[]byte(`[{"sku":"SKU-A","quantity":1,"unit_price_cents":1000,"reservation_id":"res-x"}]`))

	body := []byte(fmt.Sprintf(`{"order_id":%q,"reason":"user_request","source":"user"}`, orderID))
	env := &events.Envelope{
		EventID:       "test-ordercancelled",
		EventType:     "OrderCancelled",
		AggregateID:   orderID,
		AggregateType: "Order",
		SchemaVersion: "1.0",
		Payload:       body,
	}
	if err := h.OrderCancelledHandler(context.Background(), env); err != nil {
		t.Fatalf("OrderCancelledHandler: %v", err)
	}

	got, err := repo.Get(context.Background(), orderID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.State != saga.StateCompensated {
		t.Errorf("state after OrderCancelled: got %q want compensated", got.State)
	}

	if got := countOutboxFor(t, pool, orderID, "StockReleaseRequested"); got != 1 {
		t.Errorf("StockReleaseRequested rows: got %d want 1", got)
	}
	if got := countOutboxFor(t, pool, orderID, "OrderCancelled"); got != 1 {
		t.Errorf("OrderCancelled rows: got %d want 1", got)
	}
}

// TestOrderCancelledHandler_NoOpOnTerminal is the SAGA-5 idempotency
// guard: a redelivered OrderCancelled for an already-terminal saga
// is a silent no-op (no fresh StockReleaseRequested, no second
// OrderCancelled). The TransitionStateTx guard fails because the
// saga is in StateCompensated or StateCompleted; the handler logs
// and returns nil.
func TestOrderCancelledHandler_NoOpOnTerminal(t *testing.T) {
	pool := testHandlerDB(t)
	repo := repository.NewPGRepo(pool)
	writer := outbox.NewPGWriter()
	h := NewHandler(pool, slog.New(slog.NewTextHandler(os.Stderr, nil))).WithRepoAndWriter(repo, writer)

	const orderID = "ff777777-ffff-7777-ffff-777777777777"
	seedSaga(t, pool, orderID, saga.StateCompensated, []byte(`[{"sku":"SKU-A","quantity":1}]`))

	body := []byte(fmt.Sprintf(`{"order_id":%q,"reason":"user_request","source":"user"}`, orderID))
	env := &events.Envelope{
		EventID:       "test-ordercancelled-terminal",
		EventType:     "OrderCancelled",
		AggregateID:   orderID,
		AggregateType: "Order",
		SchemaVersion: "1.0",
		Payload:       body,
	}
	if err := h.OrderCancelledHandler(context.Background(), env); err != nil {
		t.Fatalf("OrderCancelledHandler: %v", err)
	}

	if got := countOutboxFor(t, pool, orderID, "StockReleaseRequested"); got != 0 {
		t.Errorf("StockReleaseRequested on terminal saga: got %d want 0 (must not double-emit)", got)
	}
	if got := countOutboxFor(t, pool, orderID, "OrderCancelled"); got != 0 {
		t.Errorf("OrderCancelled on terminal saga: got %d want 0", got)
	}
}

// TestOrderCreatedHandler_IdempotentOnReplay is the SAGA-6
// regression guard: a redelivered OrderCreated event must NOT emit
// a second batch of StockReserveRequested outbox rows. Pre-fix,
// InsertTx was a plain INSERT and raised 23505 on the second
// delivery; the handler returned the error, the consumer retried
// 5x1s, and the record hit the DLQ — every redelivery blocked
// the saga consumer for 5 seconds. Post-fix, InsertTx uses ON
// CONFLICT (order_id) DO NOTHING and returns (false, nil) on
// replay; the handler returns nil (silent no-op) without emitting
// any new outbox rows.
//
// The test seeds two items so it also covers the SAGA-3 fix:
// pre-fix the handler emitted one StockReserveRequested (items[0]
// only); post-fix it emits one per item.
func TestOrderCreatedHandler_IdempotentOnReplay(t *testing.T) {
	pool := testHandlerDB(t)
	repo := repository.NewPGRepo(pool)
	writer := outbox.NewPGWriter()
	h := NewHandler(pool, slog.New(slog.NewTextHandler(os.Stderr, nil))).WithRepoAndWriter(repo, writer)

	const orderID = "55555555-5555-5555-5555-555555555555"
	// Build an OrderCreated envelope with TWO items so the
	// SAGA-3 "emit one StockReserveRequested per item" assertion
	// can run alongside the SAGA-6 idempotency check.
	body := []byte(fmt.Sprintf(
		`{"order_id":"%s","customer_id":"00000000-0000-0000-0000-000000000000",`+
			`"items":[`+
			`{"sku":"SKU-A","quantity":1,"unit_price_cents":1000},`+
			`{"sku":"SKU-B","quantity":2,"unit_price_cents":2000}`+
			`],"total_cents":5000}`,
		orderID))
	env := &events.Envelope{
		EventID:       "test-event",
		EventType:     "OrderCreated",
		AggregateID:   orderID,
		AggregateType: "Order",
		SchemaVersion: "1.0",
		Payload:       body,
	}

	// First delivery: state advances and StockReserveRequested is
	// queued for EACH item (SAGA-3).
	if err := h.OrderCreatedHandler(context.Background(), env); err != nil {
		t.Fatalf("first OrderCreated: %v", err)
	}
	if got := countOutboxFor(t, pool, orderID, "StockReserveRequested"); got != 2 {
		t.Fatalf("after first delivery: StockReserveRequested rows = %d want 2 (one per item, SAGA-3)", got)
	}

	// Second delivery (Kafka rebalance, consumer restart, etc.):
	// must NOT emit a second batch of StockReserveRequested rows.
	// Pre-fix this was 4 (SAGA-3 doubled), or DLQ'd (SAGA-6 unique
	// violation). Post-fix it stays 2.
	if err := h.OrderCreatedHandler(context.Background(), env); err != nil {
		t.Fatalf("redelivered OrderCreated: %v", err)
	}
	if got := countOutboxFor(t, pool, orderID, "StockReserveRequested"); got != 2 {
		t.Errorf("after replay: StockReserveRequested rows = %d want 2 (SAGA-3 + SAGA-6 idempotency)", got)
	}
}

// TestOrderCreatedHandler_EmitsReserveForAllItems is the SAGA-3
// regression guard for the cross-order-stock-theft fix: the
// handler must emit one StockReserveRequested per item, not just
// items[0]. The pre-fix handler reserved only items[0] but
// PaymentFailed released ALL items, so a release for items[1..n]
// decremented stock from another order's reservation.
func TestOrderCreatedHandler_EmitsReserveForAllItems(t *testing.T) {
	pool := testHandlerDB(t)
	repo := repository.NewPGRepo(pool)
	writer := outbox.NewPGWriter()
	h := NewHandler(pool, slog.New(slog.NewTextHandler(os.Stderr, nil))).WithRepoAndWriter(repo, writer)

	const orderID = "5aaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	items := []map[string]any{
		{"sku": "SKU-A", "quantity": 1, "unit_price_cents": 1000},
		{"sku": "SKU-B", "quantity": 2, "unit_price_cents": 2000},
		{"sku": "SKU-C", "quantity": 3, "unit_price_cents": 3000},
	}
	itemsJSON, _ := json.Marshal(items)
	body := []byte(fmt.Sprintf(
		`{"order_id":"%s","customer_id":"00000000-0000-0000-0000-000000000000",`+
			`"items":%s,"total_cents":14000}`,
		orderID, string(itemsJSON)))
	env := &events.Envelope{
		EventID:       "test-event-multi",
		EventType:     "OrderCreated",
		AggregateID:   orderID,
		AggregateType: "Order",
		SchemaVersion: "1.0",
		Payload:       body,
	}

	if err := h.OrderCreatedHandler(context.Background(), env); err != nil {
		t.Fatalf("OrderCreated: %v", err)
	}

	// SAGA-3: exactly one StockReserveRequested per item. Pre-fix
	// this was 1 (items[0] only).
	if got := countOutboxFor(t, pool, orderID, "StockReserveRequested"); got != len(items) {
		t.Errorf("StockReserveRequested rows: got %d want %d (SAGA-3: one per item)", got, len(items))
	}

	// Each emitted row must carry a distinct reservation_id (so
	// the release flow can match each item's stock independently).
	rows, err := pool.Query(context.Background(),
		`SELECT payload FROM saga_outbox
		  WHERE aggregate_id = $1 AND event_type = 'StockReserveRequested'
		  ORDER BY id ASC`, orderID)
	if err != nil {
		t.Fatalf("query outbox: %v", err)
	}
	defer rows.Close()
	seen := map[string]bool{}
	for rows.Next() {
		var body []byte
		if err := rows.Scan(&body); err != nil {
			t.Fatalf("scan: %v", err)
		}
		var p sagaev.StockReserveRequestedPayload
		if err := json.Unmarshal(body, &p); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		if p.ReservationID == "" {
			t.Errorf("StockReserveRequested payload missing reservation_id")
		}
		if seen[p.ReservationID] {
			t.Errorf("duplicate reservation_id %q across per-item StockReserveRequested rows", p.ReservationID)
		}
		seen[p.ReservationID] = true
	}
	if len(seen) != len(items) {
		t.Errorf("distinct reservation_ids: got %d want %d", len(seen), len(items))
	}
}
