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
//	the order service additionally exposes GET /v1/orders (200
//	only when its REST handler is mounted, which the main.go
//	guarantees means the DB pool is wired).
//
//	There is NO /readyz endpoint in the project's architecture —
//	inspecting services/{order,payment,inventory,saga}/cmd/*/main.go
//	confirms this. We do not invent one. The chain's actual
//	end-to-end readiness is proven implicitly by the order
//	state-machine outcome (POST /v1/orders followed by state
//	transitions through saga). See the test call sites for the
//	split between the two probe types:
//
//	  waitForOrderReady()  — order REST handler mounted (200 on /v1/orders).
//	  waitForServiceUp()   — binary listening (200 on /healthz) —
//	                        only signal we have for payment / inventory / saga.
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
	defer func() { _ = ln.Close() }()
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

// waitUntil polls URL until it returns wantStatus, the ctx
// deadline elapses, or the deadline budget expires — whichever
// comes first. Errors and unexpected statuses log t.Logf and
// retry; the loop exits only on a clean wantStatus or a real
// timeout.
//
// waitUntil is the lowest-level shared helper. Prefer the
// purpose-built wrappers below (waitForOrderReady,
// waitForServiceUp) at the test call sites — they document the
// intent of the probe.
func waitUntil(ctx context.Context, t *testing.T, client *http.Client, url string, wantStatus int, budget time.Duration) {
	t.Helper()
	deadline := startDeadline(ctx, time.Now(), budget)
	for {
		if time.Now().After(deadline) {
			t.Fatalf("waitUntil(%s): status %d not reached by %s", url, wantStatus, deadline)
		}
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		status, body, err := httpDo(ctx, client, req)
		switch {
		case err != nil:
			t.Logf("waitUntil(%s): err=%v", url, err)
		case status == wantStatus:
			return
		case status >= 500:
			t.Logf("waitUntil(%s): status=%d body=%s", url, status, body)
		default:
			t.Logf("waitUntil(%s): status=%d body=%s", url, status, body)
		}
		sleepCtx(ctx, pollInterval)
	}
}

// waitForOrderReady polls GET /v1/orders until 200. The order
// service's REST handler is mounted only after its DB pool is
// wired; non-REST binaries return 404 because /v1/orders isn't
// registered. Only call this against the order service.
func waitForOrderReady(ctx context.Context, t *testing.T, client *http.Client, orderBase string) {
	waitUntil(ctx, t, client, orderBase+"/v1/orders", http.StatusOK, startupBudget)
}

// waitForServiceUp polls GET /healthz until 200. payment,
// inventory, and saga expose ONLY /healthz as an HTTP probe —
// there is no REST readiness endpoint in their command trees.
// This is therefore a *liveness* probe (binary up, listener
// bound) — not readiness. The chain's actual readiness is proven
// implicitly by the order's state-machine outcome (POST /v1/orders
// followed by state transitions through saga).
//
// Per the project's architecture (services/{payment,inventory,
// saga}/cmd/<svc>/main.go), no /readyz exists. We do NOT
// invent one here.
func waitForServiceUp(ctx context.Context, t *testing.T, client *http.Client, baseURL string) {
	waitUntil(ctx, t, client, baseURL+"/healthz", http.StatusOK, startupBudget)
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

// waitForStateFn polls GET /v1/orders/{id} on a ctx-cancellable
// interval and returns every response observed (in order). Stops
// when ctx deadline elapses, when isDone returns true, or when
// GET returns a 404 that won't self-heal. isFailure is consulted
// on every observed state — if it returns true the test fails
// with a regression-style message. Both callbacks receive the
// current state string.
//
// Tests pass concrete expected-state predicates:
//
//	waitForStateFn(ctx, t, client, base, id,
//	    func(s string) bool { return s == "confirmed" },     // done = confirmed
//	    func(s string) bool { return s == "cancelled" },     // fail on cancelled
//	)
//
// isDone returning nil false-y and isFailure returning nil on a
// pending row is fine — failure is what stops the test.
func waitForStateFn(ctx context.Context, t *testing.T, client *http.Client, baseURL, orderID string, isDone, isFailure func(string) bool) []orderStateResponse {
	t.Helper()
	deadline, _ := ctx.Deadline()
	url := baseURL + "/v1/orders/" + orderID
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
		switch status {
		case http.StatusOK:
			// happy path — fall through to decode
		case http.StatusNotFound:
			t.Fatalf("GET %s: 404 (order %s vanished)", url, orderID)
		default:
			t.Logf("GET %s: status=%d body=%s (retry)", url, status, body)
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
		if isFailure != nil && isFailure(sr.State) {
			t.Fatalf("GET %s: order %s reached failure state %q. observed=%s",
				url, orderID, sr.State, formatStates(observed))
		}
		if isDone != nil && isDone(sr.State) {
			return observed
		}
		sleepCtx(ctx, pollInterval)
	}
}

// waitForState is a thin convenience wrapper around waitForStateFn
// that asserts the order reaches any of `done` and never hits
// "cancelled" or "failed" (a chain-regression signature on the
// happy path). Use waitForStateFn directly when your test
// expects those terminals (e.g. the compensation test).
func waitForState(ctx context.Context, t *testing.T, client *http.Client, baseURL, orderID string, done ...string) []orderStateResponse {
	doneSet := make(map[string]struct{}, len(done))
	for _, s := range done {
		doneSet[s] = struct{}{}
	}
	return waitForStateFn(ctx, t, client, baseURL, orderID,
		func(s string) bool { _, ok := doneSet[s]; return ok },
		func(s string) bool { return s == "cancelled" || s == "failed" },
	)
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
