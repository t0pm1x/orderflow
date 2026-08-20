// Package repository contains the Inventory Service's data-access
// implementations. PGRepo is the production implementation backed by
// a pgxpool against the schema defined in
// services/inventory/migrations/0001_init.sql.
//
// ReserveStock and ReleaseStock are atomic: the stock_items row
// mutation and the inventory_outbox row commit (or roll back) in the
// same transaction. The outbox INSERT delegates to
// services/inventory/internal/outbox.PGWriter so the canonical
// outbox INSERT lives in exactly one place.
//
// The tests in this file skip when DATABASE_URL is empty so they
// remain runnable on developer machines without a local Postgres.
// CI runs them with the URL provided by the test harness.
package repository

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/t0pm1x/orderflow/platform/outbox"
)

// testDB returns a connected pgxpool against the database referenced
// by DATABASE_URL, with the Inventory Service's migrations applied
// first. If DATABASE_URL is empty, the test is skipped (the package
// is expected to remain buildable in that mode).
func testDB(t *testing.T) *pgxpool.Pool {
	t.Helper()

	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set; skipping PGRepo integration test")
	}

	if err := applyMigrations(url); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)

	if _, err := pool.Exec(context.Background(),
		`TRUNCATE TABLE stock_items, inventory_outbox RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return pool
}

// applyMigrations opens a connection to url and runs every *.sql
// file in services/inventory/migrations/ in lexical order, but
// only if the stock_items table does not already exist. Migration
// 0002 uses ALTER TABLE (no IF NOT EXISTS), so re-running it errors
// with 42701; this guard makes the helper safe across test
// processes. Mirrors services/order/internal/repository/pg_repo_test.go.
func applyMigrations(url string) error {
	repoRoot, err := findRepoRoot()
	if err != nil {
		return err
	}
	migDir := filepath.Join(repoRoot, "services", "inventory", "migrations")

	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		return err
	}
	defer pool.Close()

	var exists bool
	if err := pool.QueryRow(context.Background(),
		`SELECT EXISTS (SELECT 1 FROM information_schema.tables
		                WHERE table_name = 'stock_items')`,
	).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return nil // already migrated
	}

	entries, err := os.ReadDir(migDir)
	if err != nil {
		return err
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)
	if len(files) == 0 {
		return errNoMigrations
	}

	if _, err := pool.Exec(context.Background(),
		`CREATE EXTENSION IF NOT EXISTS pgcrypto;`+
			`CREATE EXTENSION IF NOT EXISTS "uuid-ossp";`,
	); err != nil {
		return err
	}

	for _, f := range files {
		body, err := os.ReadFile(filepath.Join(migDir, f))
		if err != nil {
			return err
		}
		if _, err := pool.Exec(context.Background(), string(body)); err != nil {
			return err
		}
	}
	return nil
}

// findRepoRoot walks up from this source file until it finds
// go.work. Mirrors services/order/internal/repository/pg_repo_test.go.
func findRepoRoot() (string, error) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", errRepoRoot
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
	return "", errRepoRoot
}

var (
	errRepoRoot     = errString("repository: cannot locate go.work")
	errNoMigrations = errString("repository: no migrations found")
)

type errString string

func (e errString) Error() string { return string(e) }

// seedStock inserts (or resets) a stock_items row for tests.
func seedStock(t *testing.T, pool *pgxpool.Pool, sku string, available, reserved int) {
	t.Helper()
	if _, err := pool.Exec(context.Background(),
		`INSERT INTO stock_items (sku, available, reserved)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (sku) DO UPDATE
		   SET available = EXCLUDED.available,
		       reserved  = EXCLUDED.reserved,
		       version   = stock_items.version + 1`,
		sku, available, reserved); err != nil {
		t.Fatalf("seed stock: %v", err)
	}
}

// sampleRecord builds a valid outbox.Record carrying a JSON payload
// that mentions sku. eventID is exposed so the test can assert on
// the outbox row. eventID must be a UUID (column type) so we
// hardcode a stable value rather than importing a uuid library.
func sampleRecord(sku, eventType string) outbox.Record {
	payload, _ := json.Marshal(map[string]string{"sku": sku})
	return outbox.Record{
		EventID:       "00000000-0000-0000-0000-000000000001",
		EventType:     eventType,
		AggregateID:   sku,
		AggregateType: "Stock",
		SchemaVersion: "1.0",
		Topic:         "inventory-events",
		Payload:       payload,
	}
}

// TestPGRepo_ReserveStock_HappyPath reserves 3 of 10 and verifies
// available/reserved were decremented/incremented atomically with
// an outbox row carrying the same event id.
func TestPGRepo_ReserveStock_HappyPath(t *testing.T) {
	pool := testDB(t)
	repo := NewPGRepo(pool)
	ctx := context.Background()

	if err := ensureStockReservationsTable(ctx, pool); err != nil {
		t.Fatalf("ensure stock_reservations table: %v", err)
	}
	seedStock(t, pool, "SKU-R1", 10, 0)
	ev := sampleRecord("res-R1", "StockReserved")
	ev.AggregateID = "res-R1"

	if err := repo.ReserveStock(context.Background(), "SKU-R1", 3, ev); err != nil {
		t.Fatalf("ReserveStock: %v", err)
	}

	s, err := repo.GetStock(context.Background(), "SKU-R1")
	if err != nil {
		t.Fatalf("GetStock: %v", err)
	}
	if s.Available != 7 {
		t.Errorf("available: got %d want 7", s.Available)
	}
	if s.Reserved != 3 {
		t.Errorf("reserved: got %d want 3", s.Reserved)
	}
	if s.Version < 2 {
		t.Errorf("version: got %d want >= 2 (bumped on update)", s.Version)
	}

	var n int
	if err := pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM inventory_outbox WHERE event_id = $1`, ev.EventID,
	).Scan(&n); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	if n != 1 {
		t.Errorf("outbox rows for event %s: got %d want 1", ev.EventID, n)
	}
}

// TestPGRepo_ReserveStock_InsufficientStock asks for more than is
// available and asserts ErrInsufficientStock is returned. The
// transaction must roll back so the outbox stays empty.
func TestPGRepo_ReserveStock_InsufficientStock(t *testing.T) {
	pool := testDB(t)
	repo := NewPGRepo(pool)

	seedStock(t, pool, "SKU-R2", 2, 0)
	ev := sampleRecord("SKU-R2", "StockReserved")

	err := repo.ReserveStock(context.Background(), "SKU-R2", 5, ev)
	if !errors.Is(err, ErrInsufficientStock) {
		t.Fatalf("expected ErrInsufficientStock, got %v", err)
	}

	var n int
	if err := pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM inventory_outbox WHERE event_id = $1`, ev.EventID,
	).Scan(&n); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	if n != 0 {
		t.Errorf("outbox must be empty after failed reserve, got %d rows", n)
	}

	s, err := repo.GetStock(context.Background(), "SKU-R2")
	if err != nil {
		t.Fatalf("GetStock: %v", err)
	}
	if s.Available != 2 || s.Reserved != 0 {
		t.Errorf("stock unchanged check: got available=%d reserved=%d, want 2/0", s.Available, s.Reserved)
	}
}

// TestPGRepo_ReleaseStock_HappyPath releases 2 of 5 reserved and
// asserts available is restored and reserved is decremented.
//
// SAGA-3: the test seeds a stock_reservations row (via
// ReserveStock on a fresh reservation_id) and releases by that
// reservation_id. Without the SAGA-3 fix, ReleaseStock keyed on
// sku+qty only and could decrement reserved for a stock_items row
// the saga never reserved.
func TestPGRepo_ReleaseStock_HappyPath(t *testing.T) {
	pool := testDB(t)
	repo := NewPGRepo(pool)
	ctx := context.Background()

	if err := ensureStockReservationsTable(ctx, pool); err != nil {
		t.Fatalf("ensure stock_reservations table: %v", err)
	}
	seedStock(t, pool, "SKU-R3", 5, 5)
	const reservationID = "res-R3"
	if _, err := pool.Exec(ctx,
		`INSERT INTO stock_reservations (reservation_id, sku, quantity)
		 VALUES ($1, $2, 2) ON CONFLICT (reservation_id) DO NOTHING`,
		reservationID, "SKU-R3"); err != nil {
		t.Fatalf("seed reservation: %v", err)
	}
	ev := sampleRecord("SKU-R3", "StockReleased")

	if err := repo.ReleaseStock(context.Background(), reservationID, "SKU-R3", 2, ev); err != nil {
		t.Fatalf("ReleaseStock: %v", err)
	}

	s, err := repo.GetStock(context.Background(), "SKU-R3")
	if err != nil {
		t.Fatalf("GetStock: %v", err)
	}
	if s.Available != 7 {
		t.Errorf("available: got %d want 7", s.Available)
	}
	if s.Reserved != 3 {
		t.Errorf("reserved: got %d want 3", s.Reserved)
	}

	var n int
	if err := pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM inventory_outbox WHERE event_id = $1`, ev.EventID,
	).Scan(&n); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	if n != 1 {
		t.Errorf("outbox rows for event %s: got %d want 1", ev.EventID, n)
	}
}

// TestPGRepo_ReleaseStock_RejectsOverRelease is the v1.1.2
// regression net for P0-#4: a release larger than `reserved`
// must NOT decrement reserved (which would inflate available
// forever). Pre-fix, the UPDATE matched any row and drove
// reserved negative. The fix added `AND reserved >= qty` to
// the WHERE clause; RowsAffected=0 surfaces as ErrNotFound.
func TestPGRepo_ReleaseStock_RejectsOverRelease(t *testing.T) {
	pool := testDB(t)
	repo := NewPGRepo(pool)
	ctx := context.Background()

	if err := ensureStockReservationsTable(ctx, pool); err != nil {
		t.Fatalf("ensure stock_reservations table: %v", err)
	}
	seedStock(t, pool, "SKU-OVER", 5, 2)
	const reservationID = "res-OVER"
	if _, err := pool.Exec(ctx,
		`INSERT INTO stock_reservations (reservation_id, sku, quantity)
		 VALUES ($1, $2, 5) ON CONFLICT (reservation_id) DO NOTHING`,
		reservationID, "SKU-OVER"); err != nil {
		t.Fatalf("seed reservation: %v", err)
	}
	ev := sampleRecord("SKU-OVER", "StockReleased")

	err := repo.ReleaseStock(context.Background(), reservationID, "SKU-OVER", 5, ev)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("ReleaseStock(5, reserved=2): got %v want ErrNotFound", err)
	}

	s, err := repo.GetStock(context.Background(), "SKU-OVER")
	if err != nil {
		t.Fatalf("GetStock: %v", err)
	}
	if s.Available != 5 {
		t.Errorf("available unchanged: got %d want 5", s.Available)
	}
	if s.Reserved != 2 {
		t.Errorf("reserved unchanged: got %d want 2", s.Reserved)
	}

	var n int
	if err := pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM inventory_outbox WHERE event_id = $1`, ev.EventID,
	).Scan(&n); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	if n != 0 {
		t.Errorf("outbox after failed release: got %d rows want 0", n)
	}
}

// TestPGRepo_ReleaseStock_RejectsNonPositiveQty is the unit-level
// guard for the qty<=0 pre-check: ReleaseStock(0) and
// ReleaseStock(-1) must return an error before any DB write.
func TestPGRepo_ReleaseStock_RejectsNonPositiveQty(t *testing.T) {
	pool := testDB(t)
	repo := NewPGRepo(pool)

	seedStock(t, pool, "SKU-ZERO", 5, 5)

	for _, qty := range []int{0, -1, -100} {
		ev := sampleRecord("SKU-ZERO", "StockReleased")
		err := repo.ReleaseStock(context.Background(), "res-ZERO", "SKU-ZERO", qty, ev)
		if err == nil {
			t.Errorf("ReleaseStock(%d): got nil want error", qty)
		}
	}

	s, err := repo.GetStock(context.Background(), "SKU-ZERO")
	if err != nil {
		t.Fatalf("GetStock: %v", err)
	}
	if s.Available != 5 || s.Reserved != 5 {
		t.Errorf("stock unchanged check: got %d/%d want 5/5", s.Available, s.Reserved)
	}
}

// ensureStockReservationsTable creates the stock_reservations table
// if it doesn't exist yet. The inventory test DB shares the
// order's PG in some test configurations; running the migration is
// idempotent because of CREATE TABLE IF NOT EXISTS.
func ensureStockReservationsTable(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx,
		`CREATE TABLE IF NOT EXISTS stock_reservations (
			reservation_id TEXT NOT NULL PRIMARY KEY,
			sku            TEXT NOT NULL,
			quantity       INTEGER NOT NULL CHECK (quantity > 0),
			order_id       TEXT,
			created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`)
	return err
}

// TestPGRepo_ReserveStock_TracksReservation is the SAGA-3
// regression guard: ReserveStock must INSERT a stock_reservations
// row so a later ReleaseStock can match by reservation_id (rather
// than blindly decrementing any reserved counter that happens to
// be >= qty for the SKU). Pre-fix, ReleaseStock keyed on sku+qty
// only and could decrement another order's reservation — cross-
// order stock theft.
func TestPGRepo_ReserveStock_TracksReservation(t *testing.T) {
	pool := testDB(t)
	repo := NewPGRepo(pool)
	ctx := context.Background()

	if err := ensureStockReservationsTable(ctx, pool); err != nil {
		t.Fatalf("ensure stock_reservations table: %v", err)
	}
	seedStock(t, pool, "SKU-TRACK", 10, 0)
	ev := sampleRecord("res-track", "StockReserved")
	ev.AggregateID = "res-track"

	if err := repo.ReserveStock(ctx, "SKU-TRACK", 2, ev); err != nil {
		t.Fatalf("ReserveStock: %v", err)
	}

	var qty int
	if err := pool.QueryRow(ctx,
		`SELECT quantity FROM stock_reservations WHERE reservation_id = $1`,
		"res-track",
	).Scan(&qty); err != nil {
		t.Fatalf("query stock_reservations: %v", err)
	}
	if qty != 2 {
		t.Errorf("reservation quantity: got %d want 2", qty)
	}
}

// TestPGRepo_ReleaseStock_RefusesUnknownReservation is the SAGA-3
// regression guard for cross-order stock theft: ReleaseStock for a
// reservation_id that doesn't exist returns ErrNotFound and leaves
// stock_items unchanged. Pre-fix, ReleaseStock keyed on sku+qty
// only and would decrement reserved even when the release didn't
// match the saga's own reservation.
func TestPGRepo_ReleaseStock_RefusesUnknownReservation(t *testing.T) {
	pool := testDB(t)
	repo := NewPGRepo(pool)
	ctx := context.Background()

	if err := ensureStockReservationsTable(ctx, pool); err != nil {
		t.Fatalf("ensure stock_reservations table: %v", err)
	}
	seedStock(t, pool, "SKU-X", 5, 0)
	ev := sampleRecord("res-does-not-exist", "StockReleased")

	err := repo.ReleaseStock(ctx, "res-does-not-exist", "SKU-X", 2, ev)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("ReleaseStock for unknown reservation: got %v want ErrNotFound", err)
	}

	var reserved int
	if err := pool.QueryRow(ctx,
		`SELECT reserved FROM stock_items WHERE sku = $1`, "SKU-X",
	).Scan(&reserved); err != nil {
		t.Fatalf("query stock_items: %v", err)
	}
	if reserved != 0 {
		t.Errorf("reserved after refused release: got %d want 0 (SAGA-3: cross-order theft prevented)", reserved)
	}
}
