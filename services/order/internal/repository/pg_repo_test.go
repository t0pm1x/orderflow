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
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

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
// trip including the JSONB items array and the timestamps from
// the orders row (Task 9 P0.5). The seed uses LastFour="4242" so
// the column is also exercised.
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
		LastFour:   "4242",
	}
	before := time.Now().UTC().Add(-time.Second)
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
	if got.LastFour != "4242" {
		t.Errorf("LastFour: got %q want %q", got.LastFour, "4242")
	}
	// CreatedAt must be set to NOW() (≈ time of insert) and UpdatedAt
	// must equal it on the pending row. Pre-Task-9 Get never scanned
	// these columns, so both surfaced as the zero time. CompletedAt
	// stays NULL on a pending order.
	if got.CreatedAt.IsZero() {
		t.Errorf("CreatedAt: got zero time, want NOW()-like")
	}
	if got.CreatedAt.UTC().Before(before) {
		t.Errorf("CreatedAt %v is before test start %v", got.CreatedAt.UTC(), before)
	}
	if !got.UpdatedAt.Equal(got.CreatedAt) {
		t.Errorf("UpdatedAt: got %v want CreatedAt %v", got.UpdatedAt, got.CreatedAt)
	}
	if got.CompletedAt != nil {
		t.Errorf("CompletedAt: got %v want nil for pending order", got.CompletedAt)
	}
}

// TestPGRepo_Get_TimestampsAfterCancel covers the CompletedAt half
// of the Task 9 fix: after a Cancel the row must surface a
// non-nil CompletedAt on Get, and UpdatedAt must be ≥ CreatedAt.
// Pre-Task-9 the column was never SELECTed, so CompletedAt always
// came back as nil even when the DB row had a value.
func TestPGRepo_Get_TimestampsAfterCancel(t *testing.T) {
	pool := testDB(t)
	repo := NewPGRepo(pool)

	id := types.NewOrderID()
	seedOrderForCancel(t, pool, id, domain.StatePending, 100)

	before := time.Now().UTC().Add(-time.Second)
	if err := repo.Cancel(context.Background(), id); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	got, err := repo.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.CompletedAt == nil {
		t.Fatal("CompletedAt: got nil want non-nil after Cancel")
	}
	if got.CompletedAt.UTC().Before(before) {
		t.Errorf("CompletedAt %v is before test start %v", got.CompletedAt.UTC(), before)
	}
	if got.UpdatedAt.Before(got.CreatedAt) {
		t.Errorf("UpdatedAt %v is before CreatedAt %v", got.UpdatedAt, got.CreatedAt)
	}
}

// TestPGRepo_Get_LastFourEmptyMapsToEmptyString pins the
// sql.NullString → "" mapping on the read path. The seed inserts
// the row directly (bypassing Insert) so the last_four column is
// genuinely NULL, mirroring a v1.x order that was inserted before
// the 0007 migration.
func TestPGRepo_Get_LastFourEmptyMapsToEmptyString(t *testing.T) {
	pool := testDB(t)
	repo := NewPGRepo(pool)

	id := types.NewOrderID()
	itemsJSON := []byte(`[{"sku":"X","quantity":1,"unit_price_cents":100}]`)
	if _, err := pool.Exec(context.Background(),
		`INSERT INTO orders (id, customer_id, items, state, total_cents, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, NOW(), NOW())`,
		id, types.NewCustomerID(), itemsJSON, string(domain.StatePending), 100,
	); err != nil {
		t.Fatalf("seed order: %v", err)
	}

	got, err := repo.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.LastFour != "" {
		t.Errorf("LastFour: got %q want empty string when column is NULL", got.LastFour)
	}
}

// TestPGRepo_Insert_LastFourEmptyPersistsAsNull pins the write
// side of the last_four fix: an empty Order.LastFour must persist
// as a SQL NULL (so the omitempty JSON tag on the domain type keeps
// the field out of the wire payload), and a re-read surfaces "".
func TestPGRepo_Insert_LastFourEmptyPersistsAsNull(t *testing.T) {
	pool := testDB(t)
	repo := NewPGRepo(pool)

	id := types.NewOrderID()
	o := &domain.Order{
		ID:         id,
		CustomerID: types.NewCustomerID(),
		Items:      []domain.OrderItem{{SKU: "X", Quantity: 1, UnitPriceCents: 100}},
		State:      domain.StatePending,
		TotalCents: types.NewMoneyFromCents(100),
		// LastFour deliberately left empty.
	}
	if err := repo.Insert(context.Background(), o); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	var lastFour *string
	if err := pool.QueryRow(context.Background(),
		`SELECT last_four FROM orders WHERE id = $1`, id,
	).Scan(&lastFour); err != nil {
		t.Fatalf("scan last_four: %v", err)
	}
	if lastFour != nil {
		t.Errorf("last_four column: got %q want NULL for empty Order.LastFour", *lastFour)
	}

	got, err := repo.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.LastFour != "" {
		t.Errorf("Get.LastFour: got %q want empty string", got.LastFour)
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

// TestPGRepo_ListEmptyStateReturnsAll guards the v1.1.3 inventory-
// page regression: when the web service calls
// PageInventory → Order.List(ctx, "", 50) to derive the SKU list,
// the empty state must be treated as "no filter" rather than
// "WHERE state = ”" (which matches zero rows). Prior to the fix
// the inventory page rendered "No stock items yet" even when
// confirmed orders existed in stock_items, because the list call
// returned {"items":null}. Also asserts that CreatedAt /
// UpdatedAt come back populated so /v1/orders timestamps are
// not the Go zero-time.
func TestPGRepo_ListEmptyStateReturnsAll(t *testing.T) {
	pool := testDB(t)
	repo := NewPGRepo(pool)

	a := &domain.Order{
		ID:         types.NewOrderID(),
		CustomerID: types.NewCustomerID(),
		Items:      []domain.OrderItem{{SKU: "A", Quantity: 1, UnitPriceCents: 10}},
		State:      domain.StatePending,
		TotalCents: types.NewMoneyFromCents(10),
	}
	b := &domain.Order{
		ID:         types.NewOrderID(),
		CustomerID: types.NewCustomerID(),
		Items:      []domain.OrderItem{{SKU: "B", Quantity: 1, UnitPriceCents: 20}},
		State:      domain.StateConfirmed,
		TotalCents: types.NewMoneyFromCents(20),
	}
	if err := repo.Insert(context.Background(), a); err != nil {
		t.Fatalf("Insert a: %v", err)
	}
	if err := repo.Insert(context.Background(), b); err != nil {
		t.Fatalf("Insert b: %v", err)
	}

	got, err := repo.List(context.Background(), "", 50)
	if err != nil {
		t.Fatalf("List(\"\", 50): %v", err)
	}
	// Both orders must be present (a, b). Other orders from the
	// pool may also exist (test isolation is by tagless skip on
	// no DB), so check at-least-2 and that both IDs are in the
	// result set.
	ids := map[types.OrderID]bool{}
	for _, o := range got {
		ids[o.ID] = true
	}
	if !ids[a.ID] || !ids[b.ID] {
		t.Errorf("empty-state List missing seeds: got %d orders, missing at least one of a=%v b=%v",
			len(got), a.ID, b.ID)
	}

	// Spot-check that CreatedAt/UpdatedAt are populated for at
	// least the seeded orders (regression on the timeline "all
	// zeros" symptom: prior to fix, List scanned no time columns
	// and the JSON returned "0001-01-01T00:00:00Z").
	for _, o := range got {
		if _, ok := ids[a.ID]; !ok && o.ID != a.ID && o.ID != b.ID {
			continue
		}
		if o.CreatedAt.IsZero() {
			t.Errorf("order %v: CreatedAt is zero (regression on timeline-display bug)", o.ID)
		}
		if o.UpdatedAt.IsZero() {
			t.Errorf("order %v: UpdatedAt is zero", o.ID)
		}
	}
}

// seedOrderForCancel is a small helper used by the cancel tests to
// insert an order directly via the pool (bypassing Insert so the
// test can control state/columns Insert doesn't set, like completed_at).
func seedOrderForCancel(t *testing.T, pool *pgxpool.Pool, id types.OrderID, state domain.OrderState, total int64) {
	t.Helper()
	itemsJSON := []byte(`[{"sku":"X","quantity":1,"unit_price_cents":100}]`)
	if _, err := pool.Exec(context.Background(),
		`INSERT INTO orders (id, customer_id, items, state, total_cents, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, NOW(), NOW())`,
		id, types.NewCustomerID(), itemsJSON, string(state), total,
	); err != nil {
		t.Fatalf("seed order: %v", err)
	}
}

// TestPGRepo_Cancel_TransitionsAndEmitsEvent covers the happy path:
// a pending order gets state='cancelled', completed_at is set, and an
// OrderCancelled event row is appended to order_outbox with the
// (reason, source) payload so the saga/inventory can release stock.
func TestPGRepo_Cancel_TransitionsAndEmitsEvent(t *testing.T) {
	pool := testDB(t)
	repo := NewPGRepo(pool)

	id := types.NewOrderID()
	seedOrderForCancel(t, pool, id, domain.StatePending, 100)

	before := time.Now().UTC().Add(-time.Second)
	if err := repo.Cancel(context.Background(), id); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	// Row must be cancelled with completed_at set.
	var (
		state       string
		completedAt *time.Time
	)
	if err := pool.QueryRow(context.Background(),
		`SELECT state, completed_at FROM orders WHERE id = $1`, id,
	).Scan(&state, &completedAt); err != nil {
		t.Fatalf("query order: %v", err)
	}
	if state != "cancelled" {
		t.Errorf("state: got %q want cancelled", state)
	}
	if completedAt == nil {
		t.Error("completed_at: got NULL want non-NULL")
	} else if completedAt.UTC().Before(before) {
		t.Errorf("completed_at %v is before test start %v", completedAt.UTC(), before)
	}

	// Outbox row must be written.
	var (
		eventType string
		topic     string
		payload   []byte
		status    string
	)
	if err := pool.QueryRow(context.Background(),
		`SELECT event_type, topic, payload, status
		   FROM order_outbox WHERE aggregate_id = $1`, id,
	).Scan(&eventType, &topic, &payload, &status); err != nil {
		t.Fatalf("query outbox: %v", err)
	}
	if eventType != "OrderCancelled" {
		t.Errorf("event_type: got %q want OrderCancelled", eventType)
	}
	if topic != "order-events" {
		t.Errorf("topic: got %q want order-events", topic)
	}
	if status != "PENDING" {
		t.Errorf("status: got %q want PENDING", status)
	}
	if !strings.Contains(string(payload), `"reason":"user_request"`) {
		t.Errorf("payload missing reason=user_request: %s", payload)
	}
	if !strings.Contains(string(payload), `"source":"user"`) {
		t.Errorf("payload missing source=user: %s", payload)
	}
	if !strings.Contains(string(payload), `"order_id":"`+id.String()+`"`) {
		t.Errorf("payload missing order_id: %s", payload)
	}
}

// TestPGRepo_Cancel_AlreadyTerminalReturnsErrNotFound covers the
// idempotency contract: a cancel against an order already in a
// terminal state (confirmed/cancelled/failed) MUST return ErrNotFound
// and MUST NOT emit a fresh outbox row. This is the regression net
// for the v1.1.2 P1-#2 consumer-side guard lifted to the user-driven
// cancel path.
func TestPGRepo_Cancel_AlreadyTerminalReturnsErrNotFound(t *testing.T) {
	pool := testDB(t)
	repo := NewPGRepo(pool)

	for _, st := range []domain.OrderState{domain.StateConfirmed, domain.StateCancelled, domain.StateFailed} {
		id := types.NewOrderID()
		seedOrderForCancel(t, pool, id, st, 100)

		err := repo.Cancel(context.Background(), id)
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("Cancel(%s): got %v want ErrNotFound", st, err)
		}

		// State must be unchanged.
		var gotState string
		if err := pool.QueryRow(context.Background(),
			`SELECT state FROM orders WHERE id = $1`, id,
		).Scan(&gotState); err != nil {
			t.Fatalf("query state for %s: %v", st, err)
		}
		if gotState != string(st) {
			t.Errorf("state for %s order: got %q want %q", st, gotState, st)
		}

		// No outbox row may be appended — would cause inventory to
		// release stock that was already (or never) reserved.
		var n int
		if err := pool.QueryRow(context.Background(),
			`SELECT COUNT(*) FROM order_outbox WHERE aggregate_id = $1`, id,
		).Scan(&n); err != nil {
			t.Fatalf("count outbox for %s: %v", st, err)
		}
		if n != 0 {
			t.Errorf("outbox for %s: got %d rows want 0 (double-emit would corrupt downstream)", st, n)
		}
	}
}

// TestPGRepo_Cancel_UnknownIDReturnsErrNotFound covers the
// not-found path: a Cancel against an id that doesn't exist must
// surface as ErrNotFound (mapped to 404 by the handler) without a
// 500 leak.
func TestPGRepo_Cancel_UnknownIDReturnsErrNotFound(t *testing.T) {
	pool := testDB(t)
	repo := NewPGRepo(pool)

	err := repo.Cancel(context.Background(), types.NewOrderID())
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Cancel(unknown): got %v want ErrNotFound", err)
	}
}
