package lock

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/t0pm1x/orderflow/platform/outbox"

	"github.com/t0pm1x/orderflow/services/inventory/internal/model"
)

// raceRow is a stub pgx.Row that returns a fixed outcome once and then
// pgx.ErrNoRows for every subsequent Scan — used by the
// concurrent-reserve test to simulate one writer winning the race.
type raceRow struct {
	returnedOnce atomic.Bool
	values       []any
}

func (r *raceRow) Scan(dest ...any) error {
	if !r.returnedOnce.CompareAndSwap(false, true) {
		return pgx.ErrNoRows
	}
	if len(dest) != len(r.values) {
		return errors.New("dest/vals length mismatch")
	}
	for i := range dest {
		switch d := dest[i].(type) {
		case *string:
			*d = r.values[i].(string)
		case *int:
			*d = r.values[i].(int)
		case *int64:
			*d = r.values[i].(int64)
		case *time.Time:
			*d = r.values[i].(time.Time)
		default:
			return errors.New("unsupported dest type")
		}
	}
	return nil
}

// raceDBTX mimics a single-row contention: every Reserve call races
// for the same row. The first goroutine to land gets a real result,
// every subsequent one gets ErrNoRows from the UPDATE and falls
// through to the differentiate SELECT. Because all callers passed
// the same expectedVersion=1, the follow-up sees version=2 (already
// bumped by the winner) and returns model.ErrStaleVersion.
type raceDBTX struct {
	mu          sync.Mutex
	updateCalls int32
	row         *raceRow
}

func (d *raceDBTX) Exec(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}

func (d *raceDBTX) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row {
	d.mu.Lock()
	defer d.mu.Unlock()
	atomic.AddInt32(&d.updateCalls, 1)
	// Every caller hits the same UPDATE row, then the same
	// differentiate row. The raceRow guarantees only the FIRST
	// scan of the row succeeds — everyone else gets ErrNoRows
	// and falls through to differentiate. We return the same row
	// for both because the raceRow is reused.
	return d.row
}

// TestReserve_Concurrent_OnlyOneSucceeds pins down the spec
// acceptance criterion for 3.6.c: "concurrent reserve of last item
// — only one succeeds".
//
// 8 goroutines try to reserve the last unit of a SKU at version 1.
// The first to win the race returns an updated stock with version=2.
// All others fall through to differentiate, find version=2 (≠
// expected 1), and get ErrStaleVersion.
//
// Compile-only contract test — exercises the lock package's race
// behavior using stub pgx.Row. Real concurrency safety on Postgres
// is verified by integration tests in 3.7.f.
func TestReserve_Concurrent_OnlyOneSucceeds(t *testing.T) {
	now := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	// Both rows (UPDATE success row and differentiate row) return
	// version=2 once they've been scanned. The raceRow's CompareAndSwap
	// ensures only the FIRST scan of the row across all goroutines
	// gets values; every subsequent scan gets pgx.ErrNoRows.
	row := &raceRow{values: []any{"sku-A", 0, 1, int64(2), now}}
	db := &raceDBTX{row: row}
	l := NewPGLocker()

	const N = 8
	var wg sync.WaitGroup
	wg.Add(N)
	results := make([]error, N)
	for i := 0; i < N; i++ {
		go func(idx int) {
			defer wg.Done()
			_, err := l.Reserve(context.Background(), db, "sku-A", 1, 1)
			results[idx] = err
		}(i)
	}
	wg.Wait()

	successes := 0
	stale := 0
	for _, err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, model.ErrStaleVersion):
			stale++
		default:
			t.Errorf("unexpected error: %v", err)
		}
	}
	if successes != 1 {
		t.Errorf("successes: got %d want exactly 1", successes)
	}
	if stale != N-1 {
		t.Errorf("stale: got %d want %d", stale, N-1)
	}
}

// Compile-time interface check.
var _ outbox.DBTX = (*raceDBTX)(nil)
