// Package repository contains the Order Service's data-access
// implementations. The only production implementation today is
// PGRepo, which uses a real pgxpool against the schema defined in
// services/order/migrations/0001_init.sql.
//
// The tests in this file skip when DATABASE_URL is empty so they
// remain runnable on developer machines without a local Postgres.
// CI runs them with the URL provided by the test harness.
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

	"github.com/t0pm1x/orderflow/platform/outbox"
	"github.com/t0pm1x/orderflow/platform/types"

	"github.com/t0pm1x/orderflow/services/order/internal/domain"
)

// testDB returns a connected pgxpool against the database referenced
// by DATABASE_URL, with the Order Service's migrations applied first.
// If DATABASE_URL is empty, the test is skipped (the package is
// expected to remain buildable in that mode).
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
		`TRUNCATE TABLE orders, order_outbox RESTART IDENTITY`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return pool
}

// applyMigrations opens a connection to url and runs every *.sql
// file in services/order/migrations/ in lexical order. It mirrors
// what the test harness does in tests/harness/harness.go, but is
// self-contained so this package's tests don't have to import the
// harness (which would pull in testcontainers).
func applyMigrations(t *testing.T, url string) error {
	t.Helper()

	repoRoot, err := findRepoRoot()
	if err != nil {
		return err
	}
	migDir := filepath.Join(repoRoot, "services", "order", "migrations")

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
		return err
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

// TestPGRepo_InsertGetRoundTrip persists an order via Insert and
// reads it back via Get, asserting every field survives the round
// trip including the JSONB items array.
func TestPGRepo_InsertGetRoundTrip(t *testing.T) {
	pool := testDB(t)
	repo := NewPGRepo(pool)

	orderID := types.NewOrderID()
	custID := types.NewCustomerID()
	o := &domain.Order{
		ID:         orderID,
		CustomerID: custID,
		Items: []domain.OrderItem{
			{SKU: "SKU-1", Quantity: 2, UnitPriceCents: 250},
			{SKU: "SKU-2", Quantity: 1, UnitPriceCents: 999},
		},
		State:      domain.StatePending,
		TotalCents: types.NewMoneyFromCents(2*250 + 999),
	}
	if err := repo.Insert(context.Background(), o); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	got, err := repo.Get(context.Background(), orderID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ID != orderID {
		t.Errorf("ID: got %v want %v", got.ID, orderID)
	}
	if got.CustomerID != custID {
		t.Errorf("CustomerID: got %v want %v", got.CustomerID, custID)
	}
	if got.State != domain.StatePending {
		t.Errorf("State: got %q want %q", got.State, domain.StatePending)
	}
	if int64(got.TotalCents) != int64(o.TotalCents) {
		t.Errorf("TotalCents: got %d want %d", got.TotalCents, o.TotalCents)
	}
	if len(got.Items) != 2 {
		t.Fatalf("Items: got %d want 2", len(got.Items))
	}
	if got.Items[0].SKU != "SKU-1" || got.Items[1].UnitPriceCents != 999 {
		t.Errorf("Items mismatch: %+v", got.Items)
	}
}

// TestPGRepo_InsertWithOutboxEvent verifies that Insert atomically
// writes both the orders row and the outbox row in the same
// transaction. The outbox row must carry the fields from the
// supplied Record so the poller (3.7) can publish it.
func TestPGRepo_InsertWithOutboxEvent(t *testing.T) {
	pool := testDB(t)
	repo := NewPGRepo(pool)

	orderID := types.NewOrderID()
	o := &domain.Order{
		ID:         orderID,
		CustomerID: types.NewCustomerID(),
		Items:      []domain.OrderItem{{SKU: "S", Quantity: 1, UnitPriceCents: 100}},
		State:      domain.StatePending,
		TotalCents: types.NewMoneyFromCents(100),
	}
	rec := outbox.Record{
		EventID:       "00000000-0000-0000-0000-000000000001",
		EventType:     "OrderCreated",
		AggregateID:   orderID.String(),
		AggregateType: "Order",
		SchemaVersion: "1.0",
		Topic:         "order-events",
		Payload:       []byte(`{"order_id":"` + orderID.String() + `"}`),
	}
	if err := repo.Insert(context.Background(), o, rec); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	var (
		gotEventID, gotType, gotStatus, gotTopic, gotSchema string
	)
	err := pool.QueryRow(context.Background(),
		`SELECT event_id::text, event_type, status, topic, schema_version
		   FROM order_outbox WHERE aggregate_id = $1`, orderID,
	).Scan(&gotEventID, &gotType, &gotStatus, &gotTopic, &gotSchema)
	if err != nil {
		t.Fatalf("query outbox: %v", err)
	}
	if gotEventID != rec.EventID {
		t.Errorf("event_id: got %q want %q", gotEventID, rec.EventID)
	}
	if gotType != "OrderCreated" {
		t.Errorf("event_type: got %q want OrderCreated", gotType)
	}
	if gotStatus != "PENDING" {
		t.Errorf("status: got %q want PENDING", gotStatus)
	}
	if gotTopic != "order-events" {
		t.Errorf("topic: got %q want order-events", gotTopic)
	}
	if gotSchema != "1.0" {
		t.Errorf("schema_version: got %q want 1.0", gotSchema)
	}
}

// TestPGRepo_ListFiltersByState inserts two orders in different
// states and asserts List returns only the ones matching the filter.
func TestPGRepo_ListFiltersByState(t *testing.T) {
	pool := testDB(t)
	repo := NewPGRepo(pool)

	pending := &domain.Order{
		ID:         types.NewOrderID(),
		CustomerID: types.NewCustomerID(),
		Items:      []domain.OrderItem{{SKU: "P", Quantity: 1, UnitPriceCents: 10}},
		State:      domain.StatePending,
		TotalCents: types.NewMoneyFromCents(10),
	}
	confirmed := &domain.Order{
		ID:         types.NewOrderID(),
		CustomerID: types.NewCustomerID(),
		Items:      []domain.OrderItem{{SKU: "C", Quantity: 1, UnitPriceCents: 20}},
		State:      domain.StateConfirmed,
		TotalCents: types.NewMoneyFromCents(20),
	}
	if err := repo.Insert(context.Background(), pending); err != nil {
		t.Fatalf("Insert pending: %v", err)
	}
	if err := repo.Insert(context.Background(), confirmed); err != nil {
		t.Fatalf("Insert confirmed: %v", err)
	}

	got, err := repo.List(context.Background(), domain.StatePending, 50)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("List(PENDING) returned %d orders, want 1", len(got))
	}
	if got[0].ID != pending.ID {
		t.Errorf("ID: got %v want %v", got[0].ID, pending.ID)
	}
	if got[0].State != domain.StatePending {
		t.Errorf("State: got %q want PENDING", got[0].State)
	}
}
