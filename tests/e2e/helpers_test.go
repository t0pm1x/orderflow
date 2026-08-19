// Shared E2E test helpers used by both order_confirmed_test.go and
// compensation_test.go. Lives in package e2e_test so anything
// declared here is available to any _test.go file in this dir.
//
// Liveness vs. readiness:
//
//	The orderflow backend services expose /healthz as a *liveness*
//	probe — it returns {"status":"ok"} whenever the binary is alive
//	and the HTTP listener is bound, regardless of whether the
//	database pool is wired or the outbox poller is healthy. Only
//	the REST handler returning 200 proves the DB pool is wired.
//
//	waitForHealth (deprecated): pings /healthz. Use this only when
//	you genuinely want liveness, not readiness.
//
//	waitForReady: GETs the REST handler. Stronger signal — the
//	handler only mounts when the dependency pool is wired.
//
// Concurrency / ctx discipline:
//
//	Every HTTP request and every sleep observes the supplied ctx
//	deadline. A hung service cannot outlive the budget. The HTTP
//	client has a Timeout safety net on top of the per-request ctx.
package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/t0pm1x/orderflow/tests/harness"
)

// Tunables. Document the budget so a future change to the
// chain's worst-case latency doesn't have to guess the magic
// numbers.
const (
	pollInterval     = 250 * time.Millisecond
	perRequestBudget = 5 * time.Second
	startupBudget    = 60 * time.Second // binary boot + first-poll latency
	overallBudget    = 120 * time.Second
	postStartBudget  = 30 * time.Second // readiness + first POST retries
)

// httpClient builds a single client per test. Timeout is a safety
// net on top of the per-request ctx.
func httpClient() *http.Client { return &http.Client{Timeout: perRequestBudget} }

// httpDo performs req with per-request ctx (parent + perRequestBudget)
// and closes Body on the way out. Returns (status, body, err).
// Body is always read+discarded so the connection can be reused
// for keep-alive. The caller MUST close Body — httpDo does that.
func httpDo(parent context.Context, client *http.Client, req *http.Request) (status int, body []byte, err error) {
	pctx, cancel := context.WithTimeout(parent, perRequestBudget)
	defer cancel()
	req = req.WithContext(pctx)
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, rerr := io.ReadAll(resp.Body)
	if rerr != nil {
		return resp.StatusCode, nil, fmt.Errorf("read body: %w", rerr)
	}
	return resp.StatusCode, b, nil
}

// sleepCtx blocks for d or until ctx is cancelled.
func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// pickFreePort returns a free TCP port on loopback. The listener
// is closed before returning, so a tiny race window exists
// between close and the service bind. Tests don't run in
// parallel (no t.Parallel) so the race is a non-issue in
// practice.
func pickFreePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pickFreePort: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// startDeadline returns min(parentCtx deadline, now+budget). Use
// over time.Now().Add(budget) so the per-stage budget cannot
// outlive the test's overall deadline.
func startDeadline(parentCtx context.Context, base time.Time, budget time.Duration) time.Time {
	d := base.Add(budget)
	if dl, ok := parentCtx.Deadline(); ok && d.After(dl) {
		d = dl
	}
	return d
}

// orderStateResponse mirrors the public Order shape documented in
// api/openapi.yaml:257-289. Extra fields on the wire are
// tolerated (the JSON decoder ignores them).
type orderStateResponse struct {
	ID    string `json:"id"`
	State string `json:"state"`
}

// waitForHealth is the legacy /healthz pinger kept around so
// compensation_test.go and similar tests that want a pure
// liveness signal still compile. /healthz on the orderflow
// backends returns 200 even when the REST handler isn't mounted,
// so this is necessary-but-not-sufficient to prove the
// service can serve requests. Prefer waitForReady for new
// code.
func waitForHealth(t *testing.T, url string, budget time.Duration) {
	t.Helper()
	client := httpClient()
	deadline := time.Now().Add(budget)
	for {
		if time.Now().After(deadline) {
			t.Fatalf("service at %s did not become healthy within %s", url, budget)
		}
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			t.Fatalf("build request %s: %v", url, err)
		}
		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(pollInterval)
	}
}

// waitForReady polls GET /v1/orders until it returns 200 (REST
// handler mounted, DB pool wired) or ctx deadline elapses. We
// deliberately do NOT use /healthz here — see the package doc
// for the liveness vs readiness rationale.
func waitForReady(ctx context.Context, t *testing.T, client *http.Client, baseURL string) {
	t.Helper()
	deadline := startDeadline(ctx, time.Now(), startupBudget)
	url := baseURL + "/v1/orders"
	for {
		if time.Now().After(deadline) {
			t.Fatalf("waitForReady(%s): GET /v1/orders did not return 200 by %s", baseURL, deadline)
		}
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		status, body, err := httpDo(ctx, client, req)
		switch {
		case err != nil:
			t.Logf("waitForReady(%s): GET /v1/orders err=%v", baseURL, err)
		case status == http.StatusOK:
			return
		case status >= 500:
			t.Logf("waitForReady(%s): GET /v1/orders status=%d body=%s", baseURL, status, body)
		default:
			t.Logf("waitForReady(%s): GET /v1/orders status=%d body=%s", baseURL, status, body)
		}
		sleepCtx(ctx, pollInterval)
	}
}

// readRepoFile resolves a path relative to the repo root (via
// harness.FindRepoRoot) so the test does not depend on the
// working directory the user invokes `go test` from.
func readRepoFile(t *testing.T, parts ...string) []byte {
	t.Helper()
	root, err := harness.FindRepoRoot()
	if err != nil {
		t.Fatalf("FindRepoRoot: %v", err)
	}
	p := filepath.Join(append([]string{root}, parts...)...)
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read %s: %v", p, err)
	}
	return b
}

// retryPost posts body once and retries on transient failures
// (network blip, 5xx while the order-events topic is auto-created
// on first publish). 4xx is fatal — the body is wrong or the
// API contract was broken. ctx deadline aborts the loop.
func retryPost(ctx context.Context, t *testing.T, client *http.Client, baseURL string, body []byte) orderStateResponse {
	t.Helper()
	deadline := startDeadline(ctx, time.Now(), postStartBudget)
	url := baseURL + "/v1/orders"
	var attempts int
	var lastStatus int
	var lastBody []byte
	var lastErr error
	for {
		attempts++
		if ctx.Err() != nil {
			t.Fatalf("retryPost aborted: %v (after %d attempts, last status=%d, body=%s, last err=%v)",
				ctx.Err(), attempts, lastStatus, lastBody, lastErr)
		}
		if time.Now().After(deadline) {
			t.Fatalf("retryPost deadline exceeded: %d attempts, last status=%d, body=%s, last err=%v",
				attempts, lastStatus, lastBody, lastErr)
		}
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		status, b, err := httpDo(ctx, client, req)
		lastStatus = status
		lastBody = b
		lastErr = err
		switch {
		case err != nil:
			t.Logf("POST %s: err=%v (retry %d)", url, err, attempts)
			sleepCtx(ctx, pollInterval)
		case status == http.StatusCreated:
			var sr orderStateResponse
			if uerr := json.Unmarshal(b, &sr); uerr != nil {
				t.Fatalf("POST %s: decode 201: %v body=%s", url, uerr, b)
			}
			if sr.ID == "" {
				t.Fatalf("POST %s: 201 but empty id, body=%s", url, b)
			}
			return sr
		case status >= 500:
			t.Logf("POST %s: status=%d body=%s (retry %d)", url, status, b, attempts)
			sleepCtx(ctx, pollInterval)
		default:
			t.Fatalf("POST %s: status=%d body=%s", url, status, b)
		}
	}
}

// waitForState polls GET /v1/orders/{id} on a ctx-cancellable
// interval and returns the slice of every response observed (in
// order). Stops when ctx deadline elapses, when the state
// matches any of done, when the state is "cancelled" or "failed"
// (regression indicator — surfaced as a hard failure), or when
// GET returns a status that won't self-heal (404 stays 404).
func waitForState(ctx context.Context, t *testing.T, client *http.Client, baseURL, orderID string, done ...string) []orderStateResponse {
	t.Helper()
	deadline, _ := ctx.Deadline()
	url := baseURL + "/v1/orders/" + orderID
	doneSet := make(map[string]struct{}, len(done))
	for _, s := range done {
		doneSet[s] = struct{}{}
	}
	var observed []orderStateResponse
	for {
		select {
		case <-ctx.Done():
			return observed
		default:
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			return observed
		}
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		status, body, err := httpDo(ctx, client, req)
		if err != nil {
			t.Logf("GET %s: %v", url, err)
			sleepCtx(ctx, pollInterval)
			continue
		}
		if status != http.StatusOK {
			if status == http.StatusNotFound {
				t.Fatalf("GET %s: 404 (order %s vanished)", url, orderID)
			}
			t.Logf("GET %s: status=%d body=%s", url, status, body)
			sleepCtx(ctx, pollInterval)
			continue
		}
		var sr orderStateResponse
		if derr := json.Unmarshal(body, &sr); derr != nil {
			t.Logf("GET %s: decode: %v body=%s", url, derr, body)
			sleepCtx(ctx, pollInterval)
			continue
		}
		observed = append(observed, sr)
		if _, ok := doneSet[sr.State]; ok {
			return observed
		}
		// Regression indicators — happy path must never cancel or
		// fail an order without operator action.
		if sr.State == "cancelled" || sr.State == "failed" {
			t.Fatalf("GET %s: order %s reached terminal state %q (chain regression). observed=%s",
				url, orderID, sr.State, formatStates(observed))
		}
		sleepCtx(ctx, pollInterval)
	}
}

// formatStates renders the observed state slice for diagnostic
// logs, e.g. "pending → reserved → confirmed".
func formatStates(observed []orderStateResponse) string {
	states := make([]string, len(observed))
	for i, o := range observed {
		states[i] = o.State
	}
	return strings.Join(states, " → ")
}
