package lock

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/t0pm1x/orderflow/platform/outbox"

	"github.com/t0pm1x/orderflow/services/inventory/internal/model"
)

// stubRow implements pgx.Row so the lock package can drive both the
// happy path and the no-rows path. Scan returns errSet on .Scan();
// rowNoRows() swaps it to pgx.ErrNoRows.
type stubRow struct {
	errSet  error
	vals    []any
	scanned bool
}

func (r *stubRow) Scan(dest ...any) error {
	if r.scanned {
		return errors.New("Scan called twice")
	}
	r.scanned = true
	if errors.Is(r.errSet, pgx.ErrNoRows) {
		return pgx.ErrNoRows
	}
	if r.errSet != nil {
		return r.errSet
	}
	if len(dest) != len(r.vals) {
		return errors.New("dest/vals length mismatch")
	}
	for i := range dest {
		switch d := dest[i].(type) {
		case *string:
			*d = r.vals[i].(string)
		case *int:
			*d = r.vals[i].(int)
		case *int64:
			*d = r.vals[i].(int64)
		case *time.Time:
			*d = r.vals[i].(time.Time)
		default:
			return errors.New("unsupported dest type")
		}
	}
	return nil
}

// fakeDBTX satisfies outbox.DBTX. Each Exec/QueryRow pushes onto
// calls; the next QueryRow's row comes from queue (FIFO) so the
// test controls return values per-call.
type fakeDBTX struct {
	calls []fakeCall
	rows  []*stubRow
	err   error
}

type fakeCall struct {
	sql  string
	args []any
}

func (f *fakeDBTX) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	f.calls = append(f.calls, fakeCall{sql: sql, args: args})
	return pgconn.CommandTag{}, f.err
}

func (f *fakeDBTX) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	f.calls = append(f.calls, fakeCall{sql: sql, args: args})
	if len(f.rows) == 0 {
		return &stubRow{errSet: pgx.ErrNoRows}
	}
	r := f.rows[0]
	f.rows = f.rows[1:]
	return r
}

func TestReserve_HappyPath(t *testing.T) {
	now := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	db := &fakeDBTX{
		rows: []*stubRow{{
			vals: []any{"sku-A", 7, 3, int64(2), now},
		}},
	}
	got, err := NewPGLocker().Reserve(context.Background(), db, "sku-A", 3, 1)
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if got.Available != 7 || got.Reserved != 3 || got.Version != 2 {
		t.Errorf("got %+v", got)
	}
}

func TestReserve_StaleVersion(t *testing.T) {
	db := &fakeDBTX{
		// First QueryRow (UPDATE) returns no rows.
		// Second QueryRow (differentiate) returns version=5 (≠ expected 1).
		rows: []*stubRow{
			{errSet: pgx.ErrNoRows},
			{vals: []any{int64(5), 10}}, // version, stock
		},
	}
	_, err := NewPGLocker().Reserve(context.Background(), db, "sku-A", 3, 1)
	if !errors.Is(err, model.ErrStaleVersion) {
		t.Fatalf("expected ErrStaleVersion, got %v", err)
	}
}

func TestReserve_InsufficientStock(t *testing.T) {
	db := &fakeDBTX{
		// First QueryRow no rows. Second QueryRow: same version, low stock.
		rows: []*stubRow{
			{errSet: pgx.ErrNoRows},
			{vals: []any{int64(1), 1}}, // version=1, stock=1
		},
	}
	_, err := NewPGLocker().Reserve(context.Background(), db, "sku-A", 5, 1)
	if !errors.Is(err, model.ErrInsufficientStock) {
		t.Fatalf("expected ErrInsufficientStock, got %v", err)
	}
}

func TestRelease_HappyPath(t *testing.T) {
	now := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	db := &fakeDBTX{
		rows: []*stubRow{{
			vals: []any{"sku-A", 10, 0, int64(2), now},
		}},
	}
	got, err := NewPGLocker().Release(context.Background(), db, "sku-A", 3, 1)
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if got.Available != 10 || got.Reserved != 0 {
		t.Errorf("got %+v", got)
	}
}

func TestRelease_StaleVersion(t *testing.T) {
	db := &fakeDBTX{
		rows: []*stubRow{
			{errSet: pgx.ErrNoRows},
			{vals: []any{int64(7), 5}}, // version=7, reserved=5
		},
	}
	_, err := NewPGLocker().Release(context.Background(), db, "sku-A", 3, 1)
	if !errors.Is(err, model.ErrStaleVersion) {
		t.Fatalf("expected ErrStaleVersion, got %v", err)
	}
}

// Compile-time interface checks.
var (
	_ outbox.DBTX = (*fakeDBTX)(nil)
)
