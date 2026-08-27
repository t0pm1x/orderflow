package saga_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/t0pm1x/orderflow/services/saga/cmd/saga"
)

func TestVersionConstant(t *testing.T) {
	if saga.Version == "" {
		t.Error("Version constant must not be empty")
	}
}

func TestTableNameConstant(t *testing.T) {
	if saga.TableName != "saga_outbox" {
		t.Errorf("TableName: got %q want saga_outbox", saga.TableName)
	}
}

// TestRun_ServesHealthzAndMetrics verifies Run starts the chi
// middleware stack + HTTP server, so /healthz and /metrics are
// reachable. The saga binary is a stub at this stage — no DB or
// Kafka wiring — so we only need to exercise the HTTP seam.
func TestRun_ServesHealthzAndMetrics(t *testing.T) {
	t.Setenv("HTTP_ADDR", "127.0.0.1:0")
	// Run() now calls platform.InitTracing; route spans to stdout
	// so the test doesn't try to dial localhost:4317 on shutdown.
	t.Setenv("OTEL_EXPORTER", "stdout")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- saga.Run(ctx)
	}()

	addr := waitForFreshReadyAddr(t, 3*time.Second)
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

// TestRun_ServesReadyzInDisabledMode is the OBS-1 regression net
// for the saga binary. /readyz must be reachable in disabled mode
// (DATABASE_URL/KAFKA_BROKERS unset) and must return 200 because
// no dependencies are wired. Before the fix the endpoint did not
// exist at all and a /readyz probe would 404.
func TestRun_ServesReadyzInDisabledMode(t *testing.T) {
	t.Setenv("HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("OTEL_EXPORTER", "stdout")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- saga.Run(ctx)
	}()

	addr := waitForFreshReadyAddr(t, 3*time.Second)
	t.Cleanup(func() { cancel() })

	resp, err := http.Get("http://" + addr + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/readyz status: got %d want 200; body=%s", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), `"status":"ok"`) {
		t.Errorf("/readyz body: got %q want to contain %q", body, `"status":"ok"`)
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

// TestRun_GoroutinesExitOnCancel verifies that Run returns within a
// bounded shutdown timeout after ctx is cancelled. This is a
// regression net for the WaitGroup-based shutdown contract: the
// integration paths (TTL sweep, outbox poller) are exercised by the
// orderflow E2E suite against services/order/cmd/order/main.go's
// startOutbox + WaitGroup pattern, which saga mirrors here. In
// disabled mode (no DB, no Kafka), the only registered goroutine is
// the HTTP server, whose existing httpSrv.Shutdown already returned
// promptly before this fix; this test pins that contract.
func TestRun_GoroutinesExitOnCancel(t *testing.T) {
	t.Setenv("HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("OTEL_EXPORTER", "stdout")
	// DATABASE_URL and KAFKA_BROKER deliberately unset so the
	// runtime stays in disabled mode — no DB or Kafka needed.

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	runStart := time.Now()
	go func() {
		errCh <- saga.Run(ctx)
	}()

	// waitForAddr polls the package-level boundAddr, which holds the
	// last listener address set by any TestRun_* in this process. The
	// listener from a previous test in the same package is already
	// closed, so we cannot just GET that address — we must wait for
	// a fresh bind + an accept-ready server before the pre-cancel
	// smoke call. Poll the GET until it succeeds or we time out.
	addr := waitForFreshReadyAddr(t, 3*time.Second)

	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(7 * time.Second):
		t.Fatal("Run did not return within 7s after cancel — goroutine leak")
	}

	if elapsed := time.Since(runStart); elapsed > 8*time.Second {
		t.Errorf("Run took too long to return after cancel: %s", elapsed)
	}

	_ = addr
}

// waitForFreshReadyAddr waits until the HTTP server is actually
// accepting connections. The package-level boundAddr carries over
// from earlier TestRun_* invocations in the same `go test` process,
// so waitForAddr alone can return a stale address whose listener
// has already been closed. Polling the GET handles both cases
// (stale address, and listener bound before Serve has started).
func waitForFreshReadyAddr(t *testing.T, maxWait time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(maxWait)
	var lastErr error
	for time.Now().Before(deadline) {
		addr := saga.ListenAddr()
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

func waitForAddr(t *testing.T, maxWait time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(maxWait)
	for time.Now().Before(deadline) {
		if a := saga.ListenAddr(); a != "" {
			return a
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("server did not bind within %s", maxWait)
	return ""
}
