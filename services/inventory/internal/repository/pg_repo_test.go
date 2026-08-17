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

	seedStock(t, pool, "SKU-R1", 10, 0)
	ev := sampleRecord("SKU-R1", "StockReserved")

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
func TestPGRepo_ReleaseStock_HappyPath(t *testing.T) {
	pool := testDB(t)
	repo := NewPGRepo(pool)

	seedStock(t, pool, "SKU-R3", 5, 5)
	ev := sampleRecord("SKU-R3", "StockReleased")

	if err := repo.ReleaseStock(context.Background(), "SKU-R3", 2, ev); err != nil {
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
