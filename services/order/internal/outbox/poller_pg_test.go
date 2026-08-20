// Real-PG regression net for the OBX-001 "DB attempts survives
// restarts" fix. Pairs with the unit test in
// pkg/outbox/poller_test.go:TestPoller_BumpsAttemptsOnEveryFailure.
//
// Pre-fix, the v1.1.4 claim was inert: MarkFailedTx was the only
// writer of `attempts` and was only invoked at the terminal FAILED
// transition — which excluded the row from future fetches. So every
// PENDING row had attempts=0 in the DB, and AttemptsOfTx always
// returned all-zeros.
//
// The fix: Source.BumpAttempts issues an autonomous (non-tx) UPDATE
// that increments `attempts` for PENDING rows on every publish
// failure. This test wires a poller with an always-error publisher
// against a real Postgres and asserts SELECT attempts > 0 after a
// few poll intervals.
//
// Skipped when DATABASE_URL is empty so it remains runnable on
// developer machines without a local Postgres. CI runs it with the
// URL provided by the test harness.
package outbox

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	pkgox "github.com/t0pm1x/orderflow/outbox"
	"github.com/t0pm1x/orderflow/platform/outbox"
)

// testPGPool opens a pgxpool against DATABASE_URL and TRUNCATEs
// order_outbox (assumes the schema has already been applied by
// the test harness or a manual migration run). Skipped when
// DATABASE_URL is empty.
//
// Unlike the saga's poller_pg_test, this one does NOT re-apply
// migrations on every call: services/order's 0002_outbox_headers.sql
// is `ALTER TABLE ADD COLUMN headers` without `IF NOT EXISTS`,
// so re-running it after the table already exists with the
// column fails. CI runs migrations via the harness before tests;
// local dev applies them via the compose init script.
func testPGPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set; skipping PG-backed poller test")
	}
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)

	if _, err := pool.Exec(context.Background(),
		`TRUNCATE TABLE order_outbox RESTART IDENTITY`); err != nil {
		t.Fatalf("truncate order_outbox (is the order schema applied?): %v", err)
	}
	return pool
}

// seedOrderOutboxRow inserts one PENDING order_outbox row with
// attempts=0 so the poller's MaxAttempts=100 path will never trigger
// DLQ — the test only cares about the BumpAttempts autonomous
// UPDATE incrementing attempts across multiple poll intervals.
func seedOrderOutboxRow(t *testing.T, pool *pgxpool.Pool, eventID string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(),
		`INSERT INTO order_outbox
		   (event_id, event_type, aggregate_id, aggregate_type,
		    schema_version, topic, payload, status, attempts)
		 VALUES
		   ($1, 'OrderCreated', $1, 'Order', '1.0', 'order-events',
		    '{}'::jsonb, 'PENDING', 0)`,
		eventID); err != nil {
		t.Fatalf("seed order_outbox: %v", err)
	}
}

// alwaysErrPublisher always returns an error from Publish so the
// poller's BumpAttempts path is exercised on every iteration.
type alwaysErrPublisher struct{ err error }

func (p *alwaysErrPublisher) Publish(_ context.Context, _ []outbox.Record) error {
	return p.err
}

// TestPGPoller_BumpAttemptsOnEveryFailure is the OBX-001
// integration regression net. Runs a poller for 3 poll intervals
// with an always-error publisher and MaxAttempts=100 (high enough
// that DLQ never fires). Asserts that SELECT attempts > 0 from
// the DB after the run completes — pre-fix this query always
// returned 0 because MarkFailedTx was the only writer of the
// `attempts` column.
func TestPGPoller_BumpAttemptsOnEveryFailure(t *testing.T) {
	pool := testPGPool(t)
	eventID := fmt.Sprintf("00000000-0000-0000-0000-%012x", time.Now().UnixNano()&0xffffffffffff)
	seedOrderOutboxRow(t, pool, eventID)

	src := NewPGSource(pool)
	pub := &alwaysErrPublisher{err: errors.New("kafka down")}
	poller := pkgox.New(pkgox.PollerConfig{
		Table:       "order_outbox",
		BatchSize:   10,
		Interval:    10 * time.Millisecond,
		MaxAttempts: 100, // never crosses
		MaxRetryAge: 0,   // disabled
	}, src, pub, nil, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	_ = poller.Run(ctx)

	var attempts int
	if err := pool.QueryRow(context.Background(),
		`SELECT attempts FROM order_outbox WHERE event_id = $1`, eventID,
	).Scan(&attempts); err != nil {
		t.Fatalf("query order_outbox: %v", err)
	}
	if attempts < 1 {
		t.Errorf("attempts: got %d want >= 1 (pre-fix regression: DB attempts never incremented for under-cap failures; the v1.1.4 claim was inert)", attempts)
	}
}

// TestPGPoller_BumpAttemptsIdempotentOnTerminal is the safety net
// for OBX-001: the autonomous UPDATE's `AND status='PENDING'`
// guard must skip rows that have been transitioned to SENT or
// FAILED by a concurrent replica. We seed a row, mark it SENT
// directly via SQL, then call BumpAttempts and assert the row's
// attempts counter is still 0.
func TestPGPoller_BumpAttemptsIdempotentOnTerminal(t *testing.T) {
	pool := testPGPool(t)
	eventID := fmt.Sprintf("00000000-0000-0000-0000-%012x", time.Now().UnixNano()&0xffffffffffff)
	seedOrderOutboxRow(t, pool, eventID)
	if _, err := pool.Exec(context.Background(),
		`UPDATE order_outbox SET status='SENT', sent_at=NOW()
		 WHERE event_id = $1`, eventID); err != nil {
		t.Fatalf("mark SENT: %v", err)
	}

	src := NewPGSource(pool)
	if err := src.BumpAttempts(context.Background(), []string{eventID}, "test"); err != nil {
		t.Fatalf("BumpAttempts: %v", err)
	}

	var attempts int
	var status string
	if err := pool.QueryRow(context.Background(),
		`SELECT attempts, status FROM order_outbox WHERE event_id = $1`, eventID,
	).Scan(&attempts, &status); err != nil {
		t.Fatalf("query order_outbox: %v", err)
	}
	if attempts != 0 {
		t.Errorf("attempts: got %d want 0 (BumpAttempts must skip non-PENDING rows)", attempts)
	}
	if status != "SENT" {
		t.Errorf("status: got %q want SENT", status)
	}
}

// avoid unused imports when the file is built standalone
var _ = sync.Mutex{}
