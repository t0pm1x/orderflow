package inventory_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/t0pm1x/orderflow/services/inventory/cmd/inventory"
)

func TestVersionConstant(t *testing.T) {
	if inventory.Version == "" {
		t.Error("Version constant must not be empty")
	}
}

func TestTableNameConstant(t *testing.T) {
	if inventory.TableName != "inventory_outbox" {
		t.Errorf("TableName: got %q want inventory_outbox", inventory.TableName)
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
	// Run() now calls platform.InitTracing; route spans to stdout
	// so the test doesn't try to dial localhost:4317 on shutdown.
	t.Setenv("OTEL_EXPORTER", "stdout")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- inventory.Run(ctx)
	}()

	addr := waitForReady(t, inventory.ListenAddr, 3*time.Second)
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

// waitForReady polls until the HTTP server actually accepts a
// /healthz request. The package-level boundAddr is set before
// httpSrv.Serve is called, so polling it alone is racy: it can
// return an address whose listener is bound but not yet
// accepting. Copy the pattern services/saga/cmd/saga/main_test.go
// uses (waitForFreshReadyAddr).
func waitForReady(t *testing.T, getAddr func() string, maxWait time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(maxWait)
	var lastErr error
	for time.Now().Before(deadline) {
		addr := getAddr()
		if addr != "" {
			resp, err := http.Get("http://" + addr + "/healthz")
			if err == nil {
				_ = resp.Body.Close()
				return addr
			}
			lastErr = err
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("server did not become ready within %s (last err: %v)", maxWait, lastErr)
	return ""
}
