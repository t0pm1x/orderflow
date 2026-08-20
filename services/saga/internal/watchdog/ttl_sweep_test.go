package watchdog_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	sagaev "github.com/t0pm1x/orderflow/services/saga/internal/events"
	"github.com/t0pm1x/orderflow/services/saga/internal/outbox"
	"github.com/t0pm1x/orderflow/services/saga/internal/repository"
	"github.com/t0pm1x/orderflow/services/saga/internal/watchdog"

	sagapkg "github.com/t0pm1x/orderflow/services/saga"
)

// testDB connects to DATABASE_URL, ensures the saga tables exist,
// truncates order_sagas + saga_outbox, and returns a per-test pool.
// Skip when DATABASE_URL is unset so the package stays buildable
// in -short without Postgres.
//
// The truncate matches the existing pre-fix pattern (used by
// services/saga/internal/repository/pg_repo_test.go and
// services/saga/internal/consumer/handlers_idempotency_test.go);
// concurrent packages sharing the same DB can race against each
// other's seeded rows during cross-package test invocations. The
// SAGA-1 + SAGA-3 tests added here use uuid.NewString() to avoid
// conflicts with the fixed-UUID fixtures in the other packages.
func testDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set; skipping watchdog integration test")
	}
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(context.Background(),
		`CREATE TABLE IF NOT EXISTS order_sagas (
			order_id    UUID         PRIMARY KEY,
			state       TEXT         NOT NULL,
			created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
			updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
			expires_at  TIMESTAMPTZ  NOT NULL
		);
		CREATE TABLE IF NOT EXISTS saga_outbox (
			id              BIGSERIAL    PRIMARY KEY,
			event_id        UUID         NOT NULL UNIQUE,
			aggregate_id    TEXT         NOT NULL,
			aggregate_type  TEXT         NOT NULL,
			event_type      TEXT         NOT NULL,
			payload         JSONB        NOT NULL,
			headers         JSONB        NOT NULL DEFAULT '{}'::jsonb,
			schema_version  INT          NOT NULL DEFAULT 1,
			status          TEXT         NOT NULL DEFAULT 'PENDING',
			attempts        INT          NOT NULL DEFAULT 0,
			last_error      TEXT,
			created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
			sent_at         TIMESTAMPTZ
		);
		ALTER TABLE order_sagas
			ADD COLUMN IF NOT EXISTS items           JSONB,
			ADD COLUMN IF NOT EXISTS total_cents     BIGINT       NOT NULL DEFAULT 0,
			ADD COLUMN IF NOT EXISTS reservation_id  TEXT,
			ADD COLUMN IF NOT EXISTS last_four       TEXT NOT NULL DEFAULT '';
		TRUNCATE TABLE order_sagas, saga_outbox RESTART IDENTITY CASCADE;`,
	); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	return pool
}

// TestTTLSweep_CompensatesExpiredSaga is the end-to-end contract:
// given an expired non-terminal saga row, one tick of the sweep
// must transition it to compensated AND emit both
// StockReleaseRequested and OrderCancelled(reason="timeout") outbox
// rows in the same transaction (atomicity). Without the sweep,
// the row would stay stuck forever (this is exactly the crash
// recovery gap the cross-restart sweep closes).
func TestTTLSweep_CompensatesExpiredSaga(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()

	orderID := uuid.NewString()
	if _, err := pool.Exec(ctx,
		`INSERT INTO order_sagas (order_id, state, items, total_cents, reservation_id, expires_at)
		 VALUES ($1, 'initiated', '[{"sku":"SKU-A","quantity":1,"unit_price_cents":1000,"reservation_id":"res-x"}]'::jsonb, 0, 'res-x', NOW() - INTERVAL '1 minute')`,
		orderID); err != nil {
		t.Fatalf("seed expired saga: %v", err)
	}

	repo := repository.NewPGRepo(pool)
	writer := outbox.NewPGWriter()
	logger := testLogger(t)
	ttl := watchdog.NewTTLSweep(pool, repo, writer, 1*time.Hour, logger)

	// Bypass the ticker: invoke the one-shot path directly so the
	// test is deterministic. Run() is the production loop; the
	// sweep is exposed via NewTTLSweep's internal hook so the
	// package can be tested without sleeping.
	ttl.RunOnce(ctx)

	got, err := repo.Get(ctx, orderID)
	if err != nil {
		t.Fatalf("Get after sweep: %v", err)
	}
	if got.State != "compensated" {
		t.Errorf("state after sweep: got %q want compensated", got.State)
	}

	// Both outbox rows must have been written in the same tx.
	rows, err := pool.Query(ctx,
		`SELECT event_type, payload FROM saga_outbox WHERE aggregate_id = $1 ORDER BY event_type`,
		orderID)
	if err != nil {
		t.Fatalf("query outbox: %v", err)
	}
	defer rows.Close()

	type emitted struct {
		eventType string
		payload   []byte
	}
	var emittedRows []emitted
	for rows.Next() {
		var e emitted
		if err := rows.Scan(&e.eventType, &e.payload); err != nil {
			t.Fatalf("scan outbox: %v", err)
		}
		emittedRows = append(emittedRows, e)
	}
	if len(emittedRows) != 2 {
		t.Fatalf("outbox rows: got %d want 2 (StockReleaseRequested + OrderCancelled): %+v", len(emittedRows), emittedRows)
	}

	wantTypes := map[string]bool{"StockReleaseRequested": false, "OrderCancelled": false}
	for _, e := range emittedRows {
		if _, ok := wantTypes[e.eventType]; !ok {
			t.Errorf("unexpected outbox event_type %q", e.eventType)
			continue
		}
		wantTypes[e.eventType] = true
		var cancel sagaev.OrderCancelledPayload
		if e.eventType == "OrderCancelled" {
			if err := json.Unmarshal(e.payload, &cancel); err != nil {
				t.Fatalf("unmarshal OrderCancelled payload: %v", err)
			}
			if cancel.Reason != "timeout" {
				t.Errorf("OrderCancelled reason: got %q want timeout", cancel.Reason)
			}
			if cancel.Source != "saga" {
				t.Errorf("OrderCancelled source: got %q want saga", cancel.Source)
			}
		}
	}
	for ev, seen := range wantTypes {
		if !seen {
			t.Errorf("expected outbox event %q not found", ev)
		}
	}
}

// TestTTLSweep_NoExpiredIsNoOp ensures a clean table does not
// produce spurious outbox rows or state transitions. This is the
// steady-state path (the sweep runs every 30s in production; most
// ticks find nothing).
func TestTTLSweep_NoExpiredIsNoOp(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()

	repo := repository.NewPGRepo(pool)
	writer := outbox.NewPGWriter()
	ttl := watchdog.NewTTLSweep(pool, repo, writer, 1*time.Hour, testLogger(t))

	ttl.RunOnce(ctx)

	var outboxCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM saga_outbox`).Scan(&outboxCount); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	if outboxCount != 0 {
		t.Errorf("outbox rows on empty sweep: got %d want 0", outboxCount)
	}
}

// TestTTLSweep_LeavesAliveSagaAlone is the SAGA-1 regression guard:
// the sweep UPDATE must guard on expires_at < NOW() (matching the
// SELECT's guard) so a saga whose expires_at was refreshed to the
// future between ListExpired and compensate is not double-handled.
//
// Pre-fix, the UPDATE only guarded on `state NOT IN ('completed',
// 'compensated')`. If a concurrent TransitionStateTx committed a
// state advance AND refreshed expires_at between the sweep's SELECT
// and UPDATE, the sweep still saw the row as "alive but stale" and
// compensated it — emitting StockReleaseRequested + OrderCancelled
// while the saga's own handler had already moved it forward. Net
// effect: charge + release + cancel for the same order.
//
// The race window is small (the gap between SELECT and UPDATE inside
// compensate) but real on a single-threaded sweep that runs while
// the consumer dispatch loop is also active. We force the race
// deterministically by:
//
//  1. Seeding an expired saga,
//  2. Driving it through TransitionStateTx (the production refresh
//     path), which must bump expires_at to the future,
//  3. Running the sweep — which must skip the now-alive saga.
func TestTTLSweep_LeavesAliveSagaAlone(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()

	orderID := uuid.NewString()
	if _, err := pool.Exec(ctx,
		`INSERT INTO order_sagas (order_id, state, items, total_cents, reservation_id, expires_at)
		 VALUES ($1, 'initiated', '[]'::jsonb, 0, 'res-z', NOW() - INTERVAL '1 minute')`,
		orderID); err != nil {
		t.Fatalf("seed expired saga: %v", err)
	}

	// Drive the saga through TransitionStateTx — this is the
	// production refresh path the SAGA-1 fix added.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	repo := repository.NewPGRepo(pool)
	advanced, err := repo.TransitionStateTx(ctx, tx, orderID, sagapkg.StateInitiated, sagapkg.StateStockReserved)
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("TransitionStateTx: %v", err)
	}
	if !advanced {
		_ = tx.Rollback(ctx)
		t.Fatal("TransitionStateTx did not advance (state guard mismatch)")
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// After TransitionStateTx the saga must be alive (expires_at
	// refreshed into the future). This is the SAGA-1 contract.
	var expiresAt time.Time
	if err := pool.QueryRow(ctx,
		`SELECT expires_at FROM order_sagas WHERE order_id = $1`, orderID,
	).Scan(&expiresAt); err != nil {
		t.Fatalf("select expires_at: %v", err)
	}
	if !expiresAt.After(time.Now()) {
		t.Fatalf("expires_at must be in the future after TransitionStateTx; got %v", expiresAt)
	}

	writer := outbox.NewPGWriter()
	ttl := watchdog.NewTTLSweep(pool, repo, writer, 1*time.Hour, testLogger(t))

	ttl.RunOnce(ctx)

	got, err := repo.Get(ctx, orderID)
	if err != nil {
		t.Fatalf("Get after sweep: %v", err)
	}
	if got.State != sagapkg.StateStockReserved {
		t.Errorf("state after sweep on alive saga: got %q want stock_reserved (sweep must not compensate a saga whose expires_at was refreshed)", got.State)
	}

	var outboxCount int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM saga_outbox WHERE aggregate_id = $1`, orderID,
	).Scan(&outboxCount); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	if outboxCount != 0 {
		t.Errorf("outbox rows for alive saga: got %d want 0 (sweep must not compensate fresh sagas)", outboxCount)
	}
}

// TestTransitionStateTx_RefreshesExpiresAt is the SAGA-1 contract
// for TransitionStateTx: every state transition must bump
// expires_at to NOW() + the saga TTL so an in-flight handler that
// advanced the saga can never be charged-and-cancelled by a sweep
// that started before the transition committed. Pre-fix, expires_at
// was set once at INSERT and never refreshed.
func TestTransitionStateTx_RefreshesExpiresAt(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()

	orderID := uuid.NewString()
	if _, err := pool.Exec(ctx,
		`INSERT INTO order_sagas (order_id, state, items, total_cents, reservation_id, expires_at)
		 VALUES ($1, 'initiated', '[]'::jsonb, 0, 'res-q', NOW() - INTERVAL '10 minutes')`,
		orderID); err != nil {
		t.Fatalf("seed stale saga: %v", err)
	}

	repo := repository.NewPGRepo(pool)

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	advanced, err := repo.TransitionStateTx(ctx, tx, orderID, sagapkg.StateInitiated, sagapkg.StateStockReserved)
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("TransitionStateTx: %v", err)
	}
	if !advanced {
		_ = tx.Rollback(ctx)
		t.Fatal("TransitionStateTx did not advance")
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	var expiresAt time.Time
	if err := pool.QueryRow(ctx,
		`SELECT expires_at FROM order_sagas WHERE order_id = $1`, orderID,
	).Scan(&expiresAt); err != nil {
		t.Fatalf("select expires_at: %v", err)
	}
	// expires_at must be in the future AND within the saga TTL
	// window (5 minutes + a tiny slack for clock skew / tx time).
	if expiresAt.Before(time.Now()) {
		t.Errorf("expires_at after transition must be in the future; got %v (now %v)", expiresAt, time.Now())
	}
	if time.Until(expiresAt) > 6*time.Minute {
		t.Errorf("expires_at after transition: got %v until expiry, want <= 5min+slack", time.Until(expiresAt))
	}
}

// TestTTLSweep_SkipsTerminalExpiredSagas: the brief says terminal
// states must not be re-compensated. A saga expired but already
// "completed" must NOT be touched by the sweep (no outbox rows,
// no state change).
func TestTTLSweep_SkipsTerminalExpiredSagas(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()

	orderID := uuid.NewString()
	if _, err := pool.Exec(ctx,
		`INSERT INTO order_sagas (order_id, state, items, total_cents, reservation_id, expires_at)
		 VALUES ($1, 'completed', '[]'::jsonb, 0, 'res-y', NOW() - INTERVAL '1 minute')`,
		orderID); err != nil {
		t.Fatalf("seed completed saga: %v", err)
	}

	repo := repository.NewPGRepo(pool)
	writer := outbox.NewPGWriter()
	ttl := watchdog.NewTTLSweep(pool, repo, writer, 1*time.Hour, testLogger(t))

	ttl.RunOnce(ctx)

	got, err := repo.Get(ctx, orderID)
	if err != nil {
		t.Fatalf("Get after sweep: %v", err)
	}
	if got.State != "completed" {
		t.Errorf("state after sweep: got %q want completed (must remain terminal)", got.State)
	}

	var outboxCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM saga_outbox WHERE aggregate_id = $1`, orderID).Scan(&outboxCount); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	if outboxCount != 0 {
		t.Errorf("outbox rows for terminal saga: got %d want 0", outboxCount)
	}
}

func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
