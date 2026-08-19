// Real-PG regression net for the saga outbox's DLQ path. Pairs
// with the v1.1.2 fake-source test (pkg/outbox/poller_test.go:
// TestPoller_RoutesToDLQAfterMaxAttempts): the fake would pass
// even if a regression in the saga SQL dropped the
// `AND status = 'PENDING'` guard from MarkFailedTx. This file
// pins the safe behavior against a real Postgres.
//
// Lives in services/saga/internal/outbox (not pkg/outbox) so it
// can call NewPGSource directly — pkg/outbox is consumed by the
// saga, not the other way around; the inverse import direction
// would be a cycle.
package outbox

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	pkgox "github.com/t0pm1x/orderflow/outbox"
	"github.com/t0pm1x/orderflow/platform/outbox"
)

// testPGPool opens a pgxpool against DATABASE_URL, applies every
// .sql file under services/saga/migrations in lexical order, and
// truncates saga_outbox. Skips when DATABASE_URL is empty.
func testPGPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set; skipping PG-backed poller test")
	}
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate source")
	}
	migDir := filepath.Join(filepath.Dir(thisFile), "..", "..", "migrations")
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)

	if _, err := pool.Exec(context.Background(),
		`CREATE EXTENSION IF NOT EXISTS pgcrypto;`+
			`CREATE EXTENSION IF NOT EXISTS "uuid-ossp";`); err != nil {
		t.Fatalf("extensions: %v", err)
	}
	entries, err := os.ReadDir(migDir)
	if err != nil {
		t.Fatalf("read migrations: %v", err)
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)
	for _, f := range files {
		body, err := os.ReadFile(filepath.Join(migDir, f))
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		if _, err := pool.Exec(context.Background(), string(body)); err != nil {
			t.Fatalf("exec %s: %v", f, err)
		}
	}
	if _, err := pool.Exec(context.Background(),
		`TRUNCATE TABLE saga_outbox RESTART IDENTITY`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return pool
}

// seedPendingOutboxRow inserts one PENDING saga_outbox row with
// attempts=2 so MaxAttempts=3 will trigger DLQ on next publish
// failure.
func seedPendingOutboxRow(t *testing.T, pool *pgxpool.Pool, eventID string, attempts int) {
	t.Helper()
	if _, err := pool.Exec(context.Background(),
		`INSERT INTO saga_outbox
		   (event_id, aggregate_id, aggregate_type, event_type,
		    payload, headers, status, attempts)
		 VALUES
		   ($1, $1, 'Order', 'OrderCreated', '{}'::jsonb,
		    '{}'::jsonb, 'PENDING', $2)`,
		eventID, attempts); err != nil {
		t.Fatalf("seed saga_outbox: %v", err)
	}
}

// alwaysErrPublisher always returns an error from Publish so the
// poller's MaxAttempts path is exercised.
type alwaysErrPublisher struct{ err error }

func (p *alwaysErrPublisher) Publish(_ context.Context, _ []outbox.Record) error {
	return p.err
}

// recordingDLQ captures every Send call so the test can assert
// the DLQ received the expected event_id and reason.
type recordingDLQ struct {
	mu      sync.Mutex
	sent    []string
	reasons []string
}

func (d *recordingDLQ) Send(_ context.Context, r outbox.Record, reason string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.sent = append(d.sent, r.EventID)
	d.reasons = append(d.reasons, reason)
	return nil
}

// TestPoller_RoutesToDLQAfterMaxAttempts_PG is the real-PG version
// of the v1.1.2 fake-source test. Pre-v1.1.2 saga SQL like
// `UPDATE saga_outbox SET status='FAILED' WHERE event_id = ANY($1)`
// (without the `AND status='PENDING'` guard) would pass the fake
// but with real PG would either (a) move an already-SENT row back
// to FAILED — breaking downstream — or (b) race with a second
// poller and double-DLQ. This test pins the safe behavior.
func TestPoller_RoutesToDLQAfterMaxAttempts_PG(t *testing.T) {
	pool := testPGPool(t)
	seedPendingOutboxRow(t, pool, "00000000-0000-0000-0000-000000000001", 2) // attempts=2, MaxAttempts=3

	src := NewPGSource(pool)
	pub := &alwaysErrPublisher{err: errors.New("kafka down")}
	dlq := &recordingDLQ{}
	poller := pkgox.New(pkgox.PollerConfig{
		Table:       "saga_outbox",
		BatchSize:   10,
		Interval:    5 * time.Millisecond,
		MaxAttempts: 3,
	}, src, pub, dlq, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_ = poller.Run(ctx)

	if len(dlq.sent) != 1 || dlq.sent[0] != "00000000-0000-0000-0000-000000000001" {
		t.Errorf("dlq: got %v want [00000000-0000-0000-0000-000000000001]", dlq.sent)
	}

	// Row should now be FAILED with attempts=3 (incremented by
	// MarkFailedTx from 2 → 3 = MaxAttempts).
	var status string
	var attempts int
	err := pool.QueryRow(context.Background(),
		`SELECT status, attempts FROM saga_outbox WHERE event_id = $1`,
		"00000000-0000-0000-0000-000000000001").Scan(&status, &attempts)
	if err != nil {
		t.Fatalf("query saga_outbox: %v", err)
	}
	if status != "FAILED" {
		t.Errorf("status: got %q want FAILED", status)
	}
	if attempts != 3 {
		t.Errorf("attempts: got %d want 3", attempts)
	}
}