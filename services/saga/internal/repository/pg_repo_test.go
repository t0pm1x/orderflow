// Package repository: PGRepo is the saga runtime's data-access
// layer over the order_sagas table. The integration tests skip when
// DATABASE_URL is unset (developer laptops without Postgres) and
// run end-to-end in CI via the harness.
package repository

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// testDB returns a connected pgxpool against the database referenced
// by DATABASE_URL, with the Saga Service's migrations applied first
// (0001_init.sql + 0002_saga_outbox.sql). If DATABASE_URL is empty,
// the test is skipped — the package must remain buildable without
// a local Postgres.
func testDB(t *testing.T) *pgxpool.Pool {
	t.Helper()

	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set; skipping PGRepo integration test")
	}

	if err := applyMigrations(t, url); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)

	if _, err := pool.Exec(context.Background(),
		`TRUNCATE TABLE order_sagas, saga_outbox RESTART IDENTITY`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return pool
}

// applyMigrations opens a connection to url and runs every *.sql
// file in services/saga/migrations/ in lexical order. Mirrors what
// tests/harness does but stays self-contained so this package's
// tests don't import the harness.
func applyMigrations(t *testing.T, url string) error {
	t.Helper()

	repoRoot, err := findRepoRoot()
	if err != nil {
		return err
	}
	migDir := filepath.Join(repoRoot, "services", "saga", "migrations")

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
		return errRepoRoot
	}

	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		return err
	}
	defer pool.Close()

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
// go.work, matching tests/harness/harness.go's strategy.
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

var errRepoRoot = errString("repository: cannot locate go.work")

type errString string

func (e errString) Error() string { return string(e) }

// TestPGRepo_InsertGetRoundTrip persists a saga via Insert and
// reads it back via Get, asserting every field survives the round
// trip including the JSONB items blob.
func TestPGRepo_InsertGetRoundTrip(t *testing.T) {
	pool := testDB(t)
	repo := NewPGRepo(pool)

	s := &Saga{
		OrderID:       "11111111-1111-1111-1111-111111111111",
		State:         "initiated",
		Items:         []byte(`[{"sku":"A","quantity":2}]`),
		TotalCents:    1500,
		ReservationID: "res-1",
	}
	if err := repo.Insert(context.Background(), s); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	got, err := repo.Get(context.Background(), s.OrderID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.OrderID != s.OrderID {
		t.Errorf("OrderID: got %q want %q", got.OrderID, s.OrderID)
	}
	if got.State != s.State {
		t.Errorf("State: got %q want %q", got.State, s.State)
	}
	if got.TotalCents != s.TotalCents {
		t.Errorf("TotalCents: got %d want %d", got.TotalCents, s.TotalCents)
	}
	if got.ReservationID != s.ReservationID {
		t.Errorf("ReservationID: got %q want %q", got.ReservationID, s.ReservationID)
	}
	if string(got.Items) != string(s.Items) {
		t.Errorf("Items: got %s want %s", got.Items, s.Items)
	}
}

// TestPGRepo_UpdateState_TransitionsAndReturnsErrNotFound: UpdateState
// must mutate the row and report ErrNotFound when the order_id
// doesn't exist (so the handler can decide whether to retry or
// skip the event).
func TestPGRepo_UpdateState_TransitionsAndReturnsErrNotFound(t *testing.T) {
	pool := testDB(t)
	repo := NewPGRepo(pool)

	s := &Saga{
		OrderID: "22222222-2222-2222-2222-222222222222",
		State:   "initiated",
		Items:   []byte(`[]`),
	}
	if err := repo.Insert(context.Background(), s); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := repo.UpdateState(context.Background(), s.OrderID, "stock_reserved"); err != nil {
		t.Fatalf("UpdateState: %v", err)
	}
	got, err := repo.Get(context.Background(), s.OrderID)
	if err != nil {
		t.Fatalf("Get after update: %v", err)
	}
	if got.State != "stock_reserved" {
		t.Errorf("State after UpdateState: got %q want stock_reserved", got.State)
	}

	if err := repo.UpdateState(context.Background(), "00000000-0000-0000-0000-000000000000", "completed"); err != ErrNotFound {
		t.Errorf("UpdateState on missing row: got %v want ErrNotFound", err)
	}
}

// TestPGRepo_SetReservationID_Overwrites: SetReservationID updates
// the reservation_id column in-place. Used when the saga already
// exists (e.g. on a retry of OrderCreated).
func TestPGRepo_SetReservationID_Overwrites(t *testing.T) {
	pool := testDB(t)
	repo := NewPGRepo(pool)

	s := &Saga{
		OrderID:       "33333333-3333-3333-3333-333333333333",
		State:         "initiated",
		Items:         []byte(`[]`),
		ReservationID: "res-original",
	}
	if err := repo.Insert(context.Background(), s); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := repo.SetReservationID(context.Background(), s.OrderID, "res-updated"); err != nil {
		t.Fatalf("SetReservationID: %v", err)
	}
	got, err := repo.Get(context.Background(), s.OrderID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ReservationID != "res-updated" {
		t.Errorf("ReservationID: got %q want res-updated", got.ReservationID)
	}
}

// TestPGRepo_Get_MissingReturnsErrNotFound verifies Get's error
// contract for non-existent sagas (vs. real DB errors).
func TestPGRepo_Get_MissingReturnsErrNotFound(t *testing.T) {
	pool := testDB(t)
	repo := NewPGRepo(pool)

	_, err := repo.Get(context.Background(), "99999999-9999-9999-9999-999999999999")
	if err != ErrNotFound {
		t.Errorf("Get missing: got %v want ErrNotFound", err)
	}
}