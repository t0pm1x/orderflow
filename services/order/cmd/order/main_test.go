package order_test

import (
	"context"
	"fmt"
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
	// Run() now calls platform.InitTracing; route spans to stdout
	// so the test doesn't try to dial localhost:4317 on shutdown.
	t.Setenv("OTEL_EXPORTER", "stdout")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- order.Run(ctx)
	}()

	addr := waitForReady(t, order.ListenAddr, 3*time.Second)
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
func TestEnvOrDefault_NotExported(_ *testing.T) {
	// Compile-time reminder: if envOrDefault is exported, this
	// test (and only this test) can be replaced by an external
	// caller. Keeping it unexported keeps the surface tight.
}

// waitForReady polls until the HTTP server service is actually
// accepting a /healthz request and returning 200 OK. The
// package-level boundAddr is set before httpSrv.Serve is called,
// so polling it alone is racy: it can return an address whose
// listener is bound but not yet accepting. Copy the pattern
// services/saga/cmd/saga/main_test.go uses (waitForFreshReadyAddr).
//
// The 100ms per-request timeout prevents a wedged server from
// blowing past the outer maxWait deadline on a single hung GET.
// The 200-OK check guards against a misconfigured /healthz
// handler returning 5xx with an empty body, which would
// otherwise satisfy "no transport error" and falsely report
// ready.
func waitForReady(t *testing.T, getAddr func() string, maxWait time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(maxWait)
	var lastErr error
	for time.Now().Before(deadline) {
		addr := getAddr()
		if addr != "" {
			client := &http.Client{Timeout: 100 * time.Millisecond}
			resp, err := client.Get("http://" + addr + "/healthz")
			if err == nil {
				_ = resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					return addr
				}
				lastErr = fmt.Errorf("/healthz returned %d", resp.StatusCode)
			} else {
				lastErr = err
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("server did not become ready within %s (last err: %v)", maxWait, lastErr)
	return ""
}
