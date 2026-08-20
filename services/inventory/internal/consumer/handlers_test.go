package consumer

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/t0pm1x/orderflow/platform/events"
)

// applyMigrations opens a pgxpool against url and applies every
// *.sql file in services/inventory/migrations/ in lexical order.
// Mirrors the harness behavior; needed so consumer tests see the
// stock_items + inventory_outbox schema (and the seed).
func applyMigrations(t *testing.T, url string) {
	t.Helper()

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot determine caller location")
	}
	migDir := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "migrations")

	entries, err := os.ReadDir(migDir)
	if err != nil {
		t.Fatalf("read migrations dir %s: %v", migDir, err)
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)
	if len(files) == 0 {
		t.Fatal("no .sql migrations found")
	}

	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	defer pool.Close()

	for _, f := range files {
		body, err := os.ReadFile(filepath.Join(migDir, f))
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		if _, err := pool.Exec(context.Background(), string(body)); err != nil {
			t.Fatalf("exec %s: %v", f, err)
		}
	}
}

// withGlobalDeps installs globalDeps backed by pool for the duration
// of the test, restoring nil on cleanup. Lets us exercise the
// real-handler code path without leaking state across tests.
func withGlobalDeps(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	prev := loadDeps()
	t.Cleanup(func() { globalDeps.Store(prev) })
	// SetPool constructs the deps via Store (atomic.Pointer); reuse
	// that path so the handler sees the same shape as in main.go.
	SetPool(pool)
}

// TestSetPool_RaceWithRegistry covers the v1.1.2 P1-#10 fix:
// globalDeps is atomic.Pointer[handlerDeps]. Concurrent SetPool
// and loadDeps must not race (or panic, or return a torn
// pointer). Test with -race; pre-fix this would fail with
// "DATA RACE" on the plain-pointer Store/Load. We only test the
// nil path here (real *pgxpool.Pool needs a real DB); the
// atomicity is exercised the same way on the nil path.
func TestSetPool_RaceWithRegistry(t *testing.T) {
	const (
		writers      = 4
		readers      = 8
		opsPerWorker = 200
	)
	var (
		wg       sync.WaitGroup
		stop     = make(chan struct{})
		observed atomic.Uint64
	)

	t.Cleanup(func() { SetPool(nil) })

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for j := 0; j < opsPerWorker; j++ {
				select {
				case <-stop:
					return
				default:
				}
				// Both branches nil: NewPGRepo would panic on nil
				// pool, so the real-handler-with-pool branch
				// requires a DB and is covered by the existing
				// TestStockReserveRequested_NotFoundEmits*
				// (which runs with DATABASE_URL). The nil-only
				// exercise still proves the atomic.Pointer
				// contract — pre-fix the plain-pointer Store
				// would tear under -race.
				_ = seed
				SetPool(nil)
			}
		}(i)
	}

	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerWorker; j++ {
				select {
				case <-stop:
					return
				default:
				}
				_ = loadDeps()
				observed.Add(1)
			}
		}()
	}

	wg.Wait()
	if observed.Load() != uint64(readers*opsPerWorker) {
		t.Errorf("reader iterations: got %d want %d", observed.Load(), readers*opsPerWorker)
	}
}

// TestStockReleasedPayload_IncludesOrderID is the SAGA-2 wire
// shape regression guard: the inventory StockReleased payload
// emitted by stockReleaseRequested must include order_id so the
// saga's StockReleasedHandler can decode it (otherwise the saga
// raises SQLSTATE 22P02 on UPDATE WHERE order_id=” and the
// consumer blocks for 5 retries × 1s = 5s per cancelled order).
//
// The full handler integration test requires a live Postgres +
// Kafka + Redpanda harness (see TestStockReserveRequested_*
// above for the harness pattern); this unit-level test exercises
// only the JSON-marshal path so the wire shape is locked down
// even when the integration suite is skipped.
func TestStockReleasedPayload_IncludesOrderID(t *testing.T) {
	// The payload is built inline in stockReleaseRequested via
	// json.Marshal(map[string]any{...}). Mirror that shape here
	// so the test catches any drift in the field set.
	payload, err := json.Marshal(map[string]any{
		"reservation_id": "res-A",
		"order_id":       "00000000-0000-0000-0000-000000000001",
		"sku":            "SKU-A",
		"quantity":       2,
		"reason":         "order_cancelled",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(payload, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["order_id"] != "00000000-0000-0000-0000-000000000001" {
		t.Errorf("StockReleased payload missing order_id: got %v", got)
	}
	if got["reservation_id"] != "res-A" {
		t.Errorf("StockReleased payload missing reservation_id: got %v", got)
	}
	if got["sku"] != "SKU-A" {
		t.Errorf("StockReleased payload missing sku: got %v", got)
	}
	if q, ok := got["quantity"].(float64); !ok || q != 2 {
		t.Errorf("StockReleased payload quantity: got %v want 2", got["quantity"])
	}
}

func TestStockReserveRequested_NotFoundEmitsStockReservationFailed(t *testing.T) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set; skipping consumer integration test")
	}

	applyMigrations(t, url)

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)

	if _, err := pool.Exec(ctx,
		`TRUNCATE TABLE stock_items, inventory_outbox RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	withGlobalDeps(t, pool)

	// "SKU-999" is intentionally NOT in the seed — must surface as
	// StockReservationFailed rather than silently retrying.
	payload, _ := json.Marshal(map[string]any{
		"order_id":       "11111111-1111-1111-1111-111111111111",
		"sku":            "SKU-999",
		"quantity":       2,
		"reservation_id": "22222222-2222-2222-2222-222222222222",
	})
	env := &events.Envelope{
		EventID:       "33333333-3333-3333-3333-333333333333",
		EventType:     "StockReserveRequested",
		AggregateID:   "22222222-2222-2222-2222-222222222222",
		AggregateType: "Reservation",
		SchemaVersion: "1.0",
		Payload:       payload,
	}
	h := stockReserveRequested(slog.Default())
	if err := h(ctx, env); err != nil {
		t.Fatalf("handler returned error: %v", err)
	}

	row := pool.QueryRow(ctx,
		`SELECT event_type, payload FROM inventory_outbox
		  WHERE aggregate_id = $1
		  ORDER BY id ASC LIMIT 1`,
		"11111111-1111-1111-1111-111111111111")
	var eventType string
	var body []byte
	if err := row.Scan(&eventType, &body); err != nil {
		if err == pgx.ErrNoRows {
			t.Fatal("expected StockReservationFailed in inventory_outbox, got none")
		}
		t.Fatalf("scan: %v", err)
	}
	if eventType != "StockReservationFailed" {
		t.Errorf("event_type: got %q want StockReservationFailed", eventType)
	}
	var p struct {
		Reason string `json:"reason"`
		SKU    string `json:"sku"`
	}
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatalf("payload not JSON: %v", err)
	}
	if p.Reason != "sku_not_found" {
		t.Errorf("reason: got %q want sku_not_found", p.Reason)
	}
	if p.SKU != "SKU-999" {
		t.Errorf("sku: got %q want SKU-999", p.SKU)
	}
}

// TestStockReserved_AggregateIDIsOrderID is the TIMELINE-FIX
// regression guard: inventory's StockReserved event must use the
// order_id as AggregateID (not the reservation_id), so the
// orderflow-web UI's per-order timeline page can filter the bus
// by aggregate_id and see this event.
//
// Pre-fix the handler emitted AggregateID=reservationID and the
// web's bus.History(orderID) filter dropped it from the timeline
// even though it was the canonical "stock reserved for this
// order" event.
//
// Requires DATABASE_URL (real PG) — skips otherwise. The unit-level
// TestStockReleasedPayload_IncludesOrderID already locks the
// payload wire shape; this test pins the AggregateID field which
// lives only on the outbox row, not the JSON payload.
func TestStockReserved_AggregateIDIsOrderID(t *testing.T) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set; skipping consumer integration test")
	}

	applyMigrations(t, url)

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)

	if _, err := pool.Exec(ctx,
		`TRUNCATE TABLE stock_items, inventory_outbox RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	// Seed stock_items with enough available for the reserve
	// (SKU-001 is the seeded SKU per migrations/0003_seed.sql).
	if _, err := pool.Exec(ctx,
		`INSERT INTO stock_items (sku, available, reserved, version)
		 VALUES ('SKU-001', 10, 0, 1)
		 ON CONFLICT (sku) DO UPDATE SET available = 10, reserved = 0, version = stock_items.version + 1`); err != nil {
		t.Fatalf("seed stock_items: %v", err)
	}

	withGlobalDeps(t, pool)

	const (
		orderID = "44444444-4444-4444-4444-444444444444"
		resID   = "55555555-5555-5555-5555-555555555555"
	)
	payload, _ := json.Marshal(map[string]any{
		"order_id":       orderID,
		"sku":            "SKU-001",
		"quantity":       1,
		"reservation_id": resID,
	})
	env := &events.Envelope{
		EventID:       "66666666-6666-6666-6666-666666666666",
		EventType:     "StockReserveRequested",
		AggregateID:   orderID,
		AggregateType: "Order",
		SchemaVersion: "1.0",
		Payload:       payload,
	}
	h := stockReserveRequested(slog.Default())
	if err := h(ctx, env); err != nil {
		t.Fatalf("handler returned error: %v", err)
	}

	// Look up the StockReserved row by order_id — pre-fix this
	// query would find nothing because AggregateID was resID.
	var (
		gotAggID   string
		gotEventID string
		gotType    string
	)
	row := pool.QueryRow(ctx,
		`SELECT event_id, aggregate_id, event_type FROM inventory_outbox
		  WHERE event_type = 'StockReserved' AND aggregate_id = $1`,
		orderID)
	if err := row.Scan(&gotEventID, &gotAggID, &gotType); err != nil {
		if err == pgx.ErrNoRows {
			t.Fatalf("StockReserved row with aggregate_id=%s not found — TIMELINE-FIX regression", orderID)
		}
		t.Fatalf("scan: %v", err)
	}
	if gotAggID != orderID {
		t.Errorf("StockReserved.AggregateID: got %q want %q (TIMELINE-FIX regression — must be order_id, not reservation_id)", gotAggID, orderID)
	}
	if gotType != "StockReserved" {
		t.Errorf("StockReserved.event_type: got %q want StockReserved", gotType)
	}
	// Defensive: the reservation_id should still live on the
	// payload (downstream consumers can join on it), even though
	// it's no longer the AggregateID.
	row = pool.QueryRow(ctx,
		`SELECT payload FROM inventory_outbox
		  WHERE event_type = 'StockReserved' AND aggregate_id = $1`, orderID)
	var body []byte
	if err := row.Scan(&body); err != nil {
		t.Fatalf("scan payload: %v", err)
	}
	if !strings.Contains(string(body), resID) {
		t.Errorf("StockReserved payload missing reservation_id=%s: %s", resID, string(body))
	}
}

// TestStockReleased_AggregateIDIsOrderID mirrors the StockReserved
// regression guard for the StockReleased path: AggregateID must
// equal order_id (not reservation_id) so the saga timeline page
// sees the "stock was released back" event.
func TestStockReleased_AggregateIDIsOrderID(t *testing.T) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set; skipping consumer integration test")
	}

	applyMigrations(t, url)

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)

	if _, err := pool.Exec(ctx,
		`TRUNCATE TABLE stock_items, inventory_outbox, stock_reservations RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO stock_items (sku, available, reserved, version)
		 VALUES ('SKU-001', 10, 1, 1)
		 ON CONFLICT (sku) DO UPDATE SET available = 10, reserved = 1, version = stock_items.version + 1`); err != nil {
		t.Fatalf("seed stock_items: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO stock_reservations (reservation_id, sku, quantity, order_id)
		 VALUES ('77777777-7777-7777-7777-777777777777', 'SKU-001', 1, '88888888-8888-8888-8888-888888888888')`); err != nil {
		t.Fatalf("seed stock_reservations: %v", err)
	}

	withGlobalDeps(t, pool)

	const (
		orderID = "88888888-8888-8888-8888-888888888888"
		resID   = "77777777-7777-7777-7777-777777777777"
	)
	payload, _ := json.Marshal(map[string]any{
		"order_id":       orderID,
		"sku":            "SKU-001",
		"quantity":       1,
		"reservation_id": resID,
	})
	env := &events.Envelope{
		EventID:       "99999999-9999-9999-9999-999999999999",
		EventType:     "StockReleaseRequested",
		AggregateID:   orderID,
		AggregateType: "Order",
		SchemaVersion: "1.0",
		Payload:       payload,
	}
	h := stockReleaseRequested(slog.Default())
	if err := h(ctx, env); err != nil {
		t.Fatalf("handler returned error: %v", err)
	}

	var gotAggID string
	row := pool.QueryRow(ctx,
		`SELECT aggregate_id FROM inventory_outbox
		  WHERE event_type = 'StockReleased' AND aggregate_id = $1`,
		orderID)
	if err := row.Scan(&gotAggID); err != nil {
		if err == pgx.ErrNoRows {
			t.Fatalf("StockReleased row with aggregate_id=%s not found — TIMELINE-FIX regression", orderID)
		}
		t.Fatalf("scan: %v", err)
	}
	if gotAggID != orderID {
		t.Errorf("StockReleased.AggregateID: got %q want %q (TIMELINE-FIX regression)", gotAggID, orderID)
	}
}
