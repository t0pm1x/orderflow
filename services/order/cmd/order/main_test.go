package order_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/t0pm1x/orderflow/services/order/cmd/order"
)

// TestVersionConstant pins the public Version identifier so a
// release-tooling change accidentally renaming it gets caught here.
func TestVersionConstant(t *testing.T) {
	if order.Version == "" {
		t.Error("Version constant must not be empty")
	}
}

// TestTableNameConstant pins the outbox table identifier.
func TestTableNameConstant(t *testing.T) {
	if order.TableName != "order_outbox" {
		t.Errorf("TableName: got %q want order_outbox", order.TableName)
	}
}

// TestRun_ServesHealthzAndMetrics verifies Run starts the chi
// middleware stack + HTTP server even when DATABASE_URL and
// KAFKA_BROKER are unset (disabled mode), so /healthz and /metrics
// remain reachable for smoke tests.
func TestRun_ServesHealthzAndMetrics(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("KAFKA_BROKER", "")
	t.Setenv("HTTP_ADDR", "127.0.0.1:0")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- order.Run(ctx)
	}()

	addr := waitForAddr(t, 3*time.Second)
	t.Cleanup(func() { cancel() })

	resp, err := http.Get("http://" + addr + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/healthz status: got %d want 200", resp.StatusCode)
	}
	if !strings.Contains(string(body), `"status":"ok"`) {
		t.Errorf("/healthz body: got %q want to contain %q", body, `"status":"ok"`)
	}

	resp, err = http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/metrics status: got %d want 200", resp.StatusCode)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run returned: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return within 3s after cancel")
	}
}

// TestEnvOrDefault_NotExported is a placeholder reminding future
// readers that envOrDefault is intentionally unexported: env wiring
// lives in this package and shouldn't leak.
func TestEnvOrDefault_NotExported(t *testing.T) {
	// Compile-time reminder: if envOrDefault is exported, this
	// test (and only this test) can be replaced by an external
	// caller. Keeping it unexported keeps the surface tight.
}

func waitForAddr(t *testing.T, max time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(max)
	for time.Now().Before(deadline) {
		if a := order.ListenAddr(); a != "" {
			return a
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("server did not bind within %s", max)
	return ""
}
