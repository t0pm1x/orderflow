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
	prev := globalDeps
	t.Cleanup(func() { globalDeps = prev })
	globalDeps = &handlerDeps{
		pool:   pool,
		repo:   nil, // constructed lazily by SetPool; mirror that here
		writer: nil,
	}
	// SetPool constructs the deps; reuse that path so the handler
	// sees the same shape as in main.go.
	SetPool(pool)
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
