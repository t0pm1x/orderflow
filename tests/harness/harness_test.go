package harness

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestHarness_StartsAllContainers asserts that New brings up every
// dependency (3 PostgreSQL databases, Redis, Kafka) and exposes
// connection details to the test.
//
// Regression net (audit TEST-5, P1): in addition to the URL-populated
// checks, query the order PG to confirm the v1.1.5 saga-migration
// auto-apply actually ran (harness.go:273-277 calls
// applyMigrations(order, "saga") so order_sagas and saga_outbox
// tables exist on the order DB). A future regression that breaks the
// saga-migration branch would otherwise silently pass the URL checks
// and stall the e2e happy-path test on "relation order_sagas does not
// exist".
//
// Skipped under -short because the harness requires Docker and pulls
// ~1 GB of images on first run.
func TestHarness_StartsAllContainers(t *testing.T) {
	if testing.Short() {
		t.Skip("harness requires docker; skipped under -short")
	}

	h := New(t)

	if len(h.KafkaBrokers) == 0 {
		t.Fatal("no kafka brokers exposed")
	}
	if len(h.PostgresURLs) != 3 {
		t.Fatalf("want 3 pg URLs, got %d", len(h.PostgresURLs))
	}
	for _, name := range []string{"order", "payment", "inventory"} {
		if h.PostgresURLs[name] == "" {
			t.Fatalf("missing pg URL for %q", name)
		}
	}
	if h.RedisURL == "" {
		t.Fatal("no redis URL exposed")
	}
	if h.OrderURL == "" || h.PaymentURL == "" || h.InventoryURL == "" {
		t.Fatal("missing one or more service-specific pg URLs")
	}
	if h.KafkaTopics == nil {
		t.Fatal("KafkaTopics map must be non-nil (empty by default)")
	}

	assertOrderSagasTable(t, h.PostgresURLs["order"])
}

// assertOrderSagasTable opens a pgx pool against the order PG and
// confirms the order_sagas table exists. The saga service shares
// the order PG in the e2e test (services/saga uses the order
// service's DB), so the v1.1.5 harness fix (harness.go:273-277) is
// what creates the table — a missing table means the saga branch of
// mustPostgres is broken and every chain test will stall on
// "pending" with `relation "order_sagas" does not exist (SQLSTATE 42P01)`.
func assertOrderSagasTable(t *testing.T, orderPGURL string) {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, orderPGURL)
	if err != nil {
		t.Fatalf("harness self-test: pgxpool.New(%q): %v", orderPGURL, err)
	}
	defer pool.Close()

	rows, err := pool.Query(ctx,
		`SELECT 1 FROM pg_tables WHERE tablename = 'order_sagas'`)
	if err != nil {
		t.Fatalf("harness self-test: query pg_tables for order_sagas: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("harness self-test: iterate pg_tables rows: %v", err)
	}
	if count == 0 {
		t.Fatalf("harness self-test: order_sagas table missing on order PG; " +
			"saga migrations were not applied (mustPostgres(order) failed to " +
			"apply the saga schema; see tests/harness/harness.go:273-277)")
	}
}

// TestPinIPv4Broker pins "localhost" and "[::1]" to "127.0.0.1" so
// franz-go on Windows doesn't dial [::1] first (regression net for
// the WIN-1 finding in audit/FINAL_AUDIT.md).
func TestPinIPv4Broker(t *testing.T) {
	cases := []struct{ in, want string }{
		{"localhost:9092", "127.0.0.1:9092"},
		{"[::1]:9092", "127.0.0.1:9092"},
		{"127.0.0.1:9092", "127.0.0.1:9092"},
		{"kafka.local:9092", "kafka.local:9092"},
		{"", ""},
	}
	for _, c := range cases {
		if got := pinIPv4Broker(c.in); got != c.want {
			t.Errorf("pinIPv4Broker(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
