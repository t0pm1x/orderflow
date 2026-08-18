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

	"github.com/t0pm1x/orderflow/services/saga"
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

// TestPGRepo_InsertTx_RollbackUndoesInsert: when a caller wraps
// InsertTx in pgx.BeginFunc and the surrounding tx is rolled back,
// the row must NOT persist. This is the contract the saga consumer
// handlers rely on to keep state-update and outbox-Append atomic —
// without it, a transient failure between UpdateState and Append
// would leave the saga advanced without the matching event.
func TestPGRepo_InsertTx_RollbackUndoesInsert(t *testing.T) {
	pool := testDB(t)
	repo := NewPGRepo(pool)
	ctx := context.Background()

	s := &Saga{
		OrderID: "88888888-8888-8888-8888-888888888888",
		State:   "initiated",
		Items:   []byte(`[]`),
	}

	// InsertTx inside a transaction we deliberately rollback.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := repo.InsertTx(ctx, tx, s); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("InsertTx: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	if _, err := repo.Get(ctx, s.OrderID); err != ErrNotFound {
		t.Errorf("after rollback: Get should return ErrNotFound; got %v", err)
	}
}

// TestPGRepo_UpdateStateTx_RollbackUndoesUpdate: same contract for
// UpdateStateTx — the saga's state column must not change if the
// caller rolls back. Without this, the saga could reach StateCompleted
// while the corresponding OrderConfirmed event never gets emitted.
func TestPGRepo_UpdateStateTx_RollbackUndoesUpdate(t *testing.T) {
	pool := testDB(t)
	repo := NewPGRepo(pool)
	ctx := context.Background()

	s := &Saga{
		OrderID: "99999999-9999-9999-9999-999999999999",
		State:   "initiated",
		Items:   []byte(`[]`),
	}
	if err := repo.Insert(ctx, s); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := repo.UpdateStateTx(ctx, tx, s.OrderID, "completed"); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("UpdateStateTx: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	got, err := repo.Get(ctx, s.OrderID)
	if err != nil {
		t.Fatalf("Get after rollback: %v", err)
	}
	if got.State != "initiated" {
		t.Errorf("after rollback: state must remain initiated; got %q", got.State)
	}
}
// TTL sweep contract: ListExpired must return only sagas whose
// expires_at is in the past AND whose state is neither "completed"
// nor "compensated". Sagas that expired but are already terminal
// (e.g. crash-compensated before) must NOT be returned — otherwise
// the sweep would re-emit events for an already-clean saga.
func TestPGRepo_ListExpired_ReturnsOnlyExpiredAndNonTerminal(t *testing.T) {
	pool := testDB(t)
	repo := NewPGRepo(pool)
	ctx := context.Background()

	expiredID := "44444444-4444-4444-4444-444444444444"
	freshID := "55555555-5555-5555-5555-555555555555"
	expiredTerminalID := "66666666-6666-6666-6666-666666666666"
	expiredCompensatedID := "77777777-7777-7777-7777-777777777777"

	insertWithExpires := func(id string, state saga.State, expiresAt string) {
		t.Helper()
		_, err := pool.Exec(ctx,
			`INSERT INTO order_sagas (order_id, state, items, total_cents, reservation_id, expires_at)
			 VALUES ($1, $2, '[]'::jsonb, 0, '', $3::timestamptz)`,
			id, string(state), expiresAt)
		if err != nil {
			t.Fatalf("insert %s: %v", id, err)
		}
	}

	insertWithExpires(expiredID, "initiated", "NOW() - INTERVAL '1 minute'")
	insertWithExpires(freshID, "initiated", "NOW() + INTERVAL '5 minutes'")
	insertWithExpires(expiredTerminalID, "completed", "NOW() - INTERVAL '1 minute'")
	insertWithExpires(expiredCompensatedID, "compensated", "NOW() - INTERVAL '1 minute'")

	got, err := repo.ListExpired(ctx, 100)
	if err != nil {
		t.Fatalf("ListExpired: %v", err)
	}

	var gotIDs []string
	for _, s := range got {
		gotIDs = append(gotIDs, s.OrderID)
	}

	if len(gotIDs) != 1 {
		t.Fatalf("ListExpired returned %d rows, want 1 (got=%v)", len(gotIDs), gotIDs)
	}
	if gotIDs[0] != expiredID {
		t.Errorf("ListExpired first id: got %q want %q", gotIDs[0], expiredID)
	}
	if got[0].State != "initiated" {
		t.Errorf("ListExpired state: got %q want initiated", got[0].State)
	}
}
