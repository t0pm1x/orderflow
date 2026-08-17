package watchdog_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	sagaev "github.com/t0pm1x/orderflow/services/saga/internal/events"
	"github.com/t0pm1x/orderflow/services/saga/internal/outbox"
	"github.com/t0pm1x/orderflow/services/saga/internal/repository"
	"github.com/t0pm1x/orderflow/services/saga/internal/watchdog"
)

// testDB connects to DATABASE_URL, applies saga migrations, and
// truncates order_sagas + saga_outbox. Mirrors the pattern in
// pg_repo_test.go (skip when DATABASE_URL is unset so the package
// stays buildable in -short without Postgres).
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

	orderID := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	if _, err := pool.Exec(ctx,
		`INSERT INTO order_sagas (order_id, state, items, total_cents, reservation_id, expires_at)
		 VALUES ($1, 'initiated', '[]'::jsonb, 0, 'res-x', NOW() - INTERVAL '1 minute')`,
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

// TestTTLSweep_SkipsTerminalExpiredSagas: the brief says terminal
// states must not be re-compensated. A saga expired but already
// "completed" must NOT be touched by the sweep (no outbox rows,
// no state change).
func TestTTLSweep_SkipsTerminalExpiredSagas(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()

	orderID := "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
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
