package harness

import (
	"testing"
)

// TestHarness_StartsAllContainers asserts that New brings up every
// dependency (3 PostgreSQL databases, Redis, Kafka) and exposes
// connection details to the test.
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
}
