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
// POST /v1/orders retry safety:
//
//	The order service's POST /v1/orders handler has no Idempotency-Key
//	header support. domain.NewOrder calls uuid.New() for the order id,
//	so a retry after the server committed but the client didn't see
//	the response (network blip / timeout) would create a duplicate
//	order. The handler's 5xx path returns BEFORE repo.Insert in the
//	buildErr / InsertErr branches — those are safe to retry — but
//	post-Insert failures (rare json.Encode path) are NOT safe.
//	postOrder is therefore a single attempt: 201 → continue, anything
//	else is fatal with full diagnostic. A test that retries a
//	non-idempotent POST is a test that hides a real production bug.
//
// Stage budgets as real hard deadlines:
//
//	Every helper that takes a budget derives a stageCtx via
//	context.WithDeadline(parentCtx, deadline). stageCtx cancels at
//	exactly min(parentCtx deadline, now+budget). All HTTP, sleep,
//	and JSON-parse work inside the loop uses stageCtx — a hung
//	request cannot outlive the budget even if the per-iteration
//	check never runs.
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
	postStartBudget  = 30 * time.Second // readiness + first POST
	// pollingBudget caps the GET /v1/orders/{id} wait. A
	// healthy cold-cache chain produces a state transition
	// within seconds; 60 s gives a wide safety margin while
	// still failing fast on a real chain hang. The
	// separate overallBudget covers startup + POST retry;
	// any leftover time goes to polling.
	pollingBudget = 60 * time.Second
)

// httpClient builds a single client per test. Timeout is a safety
// net on top of the per-request ctx.
func httpClient() *http.Client { return &http.Client{Timeout: perRequestBudget} }

// httpDo performs req with per-request ctx (parent +
// perRequestBudget). The response Body is fully drained and
// closed before returning. Returns (status, body, err).
//
// The body is read+discarded so the connection can be reused for
// keep-alive. The caller does NOT need to close Body.
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

// stageContext derives a child context whose deadline is
// min(parentCtx deadline, now+budget). All work inside a polling
// loop should use the stage context so a hung request or a slow
// sleep cannot outlive the per-stage budget. The cancel func is
// returned for the caller to defer.
//
// The parent context is left untouched; the stage context is
// derived from it and is cancelled at the stage deadline.
func stageContext(parentCtx context.Context, budget time.Duration) (context.Context, context.CancelFunc, time.Time) {
	deadline := time.Now().Add(budget)
	if dl, ok := parentCtx.Deadline(); ok && deadline.After(dl) {
		deadline = dl
	}
	stage, cancel := context.WithDeadline(parentCtx, deadline)
	return stage, cancel, deadline
}

// orderStateResponse mirrors the public Order shape documented in
// api/openapi.yaml:257-289. Extra fields on the wire are
// tolerated (the JSON decoder ignores them).
type orderStateResponse struct {
	ID    string `json:"id"`
	State string `json:"state"`
}

// waitUntil polls URL until it returns wantStatus, or until the
// stage context (derived from parentCtx + budget) is cancelled.
// Errors and unexpected statuses log t.Logf and retry; the loop
// exits only on a clean wantStatus or a real timeout.
//
// The stage context's deadline is enforced by context — a hung
// HTTP request or sleep returns immediately when the stage
// deadline elapses, no matter where the loop is currently
// parked. The post-deadline `time.Now().After(deadline)` is a
// belt-and-suspenders check for log clarity.
//
// waitUntil is the lowest-level shared helper. Prefer the
// purpose-built wrappers below (waitForOrderReady,
// waitForServiceUp) at the test call sites — they document the
// intent of the probe.
func waitUntil(ctx context.Context, t *testing.T, client *http.Client, url string, wantStatus int, budget time.Duration) {
	t.Helper()
	stage, cancel, deadline := stageContext(ctx, budget)
	defer cancel()
	for {
		if stage.Err() != nil {
			t.Fatalf("waitUntil(%s): status %d not reached by %s: %v", url, wantStatus, deadline, stage.Err())
		}
		req, err := http.NewRequestWithContext(stage, http.MethodGet, url, nil)
		if err != nil {
			t.Fatalf("waitUntil(%s): build request: %v", url, err)
		}
		status, body, err := httpDo(stage, client, req)
		switch {
		case err != nil:
			t.Logf("waitUntil(%s): err=%v", url, err)
		case status == wantStatus:
			return
		default:
			t.Logf("waitUntil(%s): status=%d body=%s", url, status, body)
		}
		sleepCtx(stage, pollInterval)
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

// dumpServiceLogs t.Logfs the last maxLines of every
// tests/logs/<svc>.log file the harness wrote. Call this on
// chain-stall timeout so the next CI run shows the spawned
// services' diagnostic output in the test's stdout (in addition
// to the e2e-service-logs GitHub Actions artifact). Each log is
// delimited with a header so multiple services' output stays
// readable in the test log.
//
// Safe to call on a fresh checkout (no logs) — it no-ops in that
// case.
func dumpServiceLogs(t *testing.T, maxLines int) {
	t.Helper()
	root, err := harness.FindRepoRoot()
	if err != nil {
		return
	}
	dir := filepath.Join(root, "tests", "logs")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".log") {
			continue
		}
		path := filepath.Join(dir, e.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		t.Logf("========== tests/logs/%s (last %d lines) ==========", e.Name(), maxLines)
		t.Logf("%s", tailLines(string(data), maxLines))
	}
}

// tailLines returns the last n lines of s, prefixed by "..."
// when truncated.
func tailLines(s string, n int) string {
	lines := strings.Split(s, "\n")
	if len(lines) <= n {
		return s
	}
	return "...[truncated " + strconvItoa(len(lines)-n) + " lines]...\n" +
		strings.Join(lines[len(lines)-n:], "\n")
}

// strconvItoa is a tiny wrapper so the tailLines log message
// uses fmt.Sprintf without dragging in strconv for one call.
func strconvItoa(n int) string {
	return fmt.Sprintf("%d", n)
}

// postOrder submits body to POST baseURL+"/v1/orders" and returns
// the parsed order. Single attempt — see the package doc for
// the POST /v1/orders retry-safety rationale. On anything but
// 201 the test fails with a diagnostic including the full body
// and the request's context error.
func postOrder(ctx context.Context, t *testing.T, client *http.Client, baseURL string, body []byte) orderStateResponse {
	t.Helper()
	stage, cancel, _ := stageContext(ctx, postStartBudget)
	defer cancel()
	url := baseURL + "/v1/orders"
	req, err := http.NewRequestWithContext(stage, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("postOrder(%s): build request: %v", url, err)
	}
	req.Header.Set("Content-Type", "application/json")
	status, b, err := httpDo(stage, client, req)
	if err != nil {
		t.Fatalf("postOrder(%s): %v (POST is non-idempotent — server may have committed; not retrying)", url, err)
	}
	switch status {
	case http.StatusCreated:
		var sr orderStateResponse
		if uerr := json.Unmarshal(b, &sr); uerr != nil {
			t.Fatalf("postOrder(%s): decode 201: %v body=%s", url, uerr, b)
		}
		if sr.ID == "" {
			t.Fatalf("postOrder(%s): 201 but empty id, body=%s", url, b)
		}
		return sr
	default:
		t.Fatalf("postOrder(%s): status=%d body=%s (POST is non-idempotent — not retrying)", url, status, b)
		// Unreachable: t.Fatalf calls runtime.Goexit, but the
		// compiler doesn't know that. Returning a zero value keeps
		// the function total.
		return orderStateResponse{}
	}
}

// validOrderState lists every legal Order state, derived from
// services/order/internal/domain/state.go. Any observed state
// outside this set is a chain-regression signature (a real
// schema or producer bug, not a test flake).
func validOrderState(s string) bool {
	switch s {
	case "pending", "reserved", "confirmed", "cancelled", "failed":
		return true
	}
	return false
}

// waitForStateFn polls GET /v1/orders/{id} until isDone returns
// true, the per-stage budget elapses, or the parent context is
// cancelled. Returns every response observed (in order) so the
// caller can print the full state trace on timeout.
//
// A stageCtx is derived from parentCtx + budget via
// context.WithDeadline. The per-stage budget is hard — a hung
// HTTP request or sleep cannot outlive it.
//
// isFailure is consulted on every observed state — if it returns
// true the test fails with a regression-style message. The
// validators in the package doc for the GET 404 contract apply
// unchanged.
//
// Tests pass concrete expected-state predicates:
//
//	waitForStateFn(ctx, t, client, base, id, pollingBudget,
//	    func(s string) bool { return s == "confirmed" },     // done = confirmed
//	    func(s string) bool { return s == "cancelled" },     // fail on cancelled
//	)
//
// On timeout (stage budget exhausted) the function returns the
// observed slice AND emits a t.Logf diagnostic with the full
// state trace; the caller decides whether t.Fatalf is warranted
// (the wrappers waitForState / waitForCompensation do that).
func waitForStateFn(ctx context.Context, t *testing.T, client *http.Client, baseURL, orderID string, budget time.Duration, isDone, isFailure func(string) bool) []orderStateResponse {
	t.Helper()
	stage, cancel, deadline := stageContext(ctx, budget)
	defer cancel()
	url := baseURL + "/v1/orders/" + orderID
	var observed []orderStateResponse
	for {
		if stage.Err() != nil {
			t.Logf("waitForStateFn(%s) timed out at %s; observed=%s",
				url, deadline, formatStates(observed))
			return observed
		}
		req, err := http.NewRequestWithContext(stage, http.MethodGet, url, nil)
		if err != nil {
			t.Fatalf("waitForStateFn: build GET request: %v", err)
		}
		status, body, err := httpDo(stage, client, req)
		if err != nil {
			t.Logf("GET %s: %v", url, err)
			sleepCtx(stage, pollInterval)
			continue
		}
		switch status {
		case http.StatusOK:
			// happy path — fall through to decode
		case http.StatusNotFound:
			t.Fatalf("GET %s: 404 (order %s vanished; row was committed in same tx as 201 response, so this is a real bug, not eventual consistency)", url, orderID)
		default:
			t.Logf("GET %s: status=%d body=%s (retry)", url, status, body)
			sleepCtx(stage, pollInterval)
			continue
		}
		var sr orderStateResponse
		if derr := json.Unmarshal(body, &sr); derr != nil {
			t.Logf("GET %s: decode: %v body=%s", url, derr, body)
			sleepCtx(stage, pollInterval)
			continue
		}
		observed = append(observed, sr)
		if !validOrderState(sr.State) {
			t.Errorf("GET %s: order %s reached unknown state %q (chain schema regression). observed=%s",
				url, orderID, sr.State, formatStates(observed))
		}
		if isFailure != nil && isFailure(sr.State) {
			t.Fatalf("GET %s: order %s reached failure state %q. observed=%s",
				url, orderID, sr.State, formatStates(observed))
		}
		if isDone != nil && isDone(sr.State) {
			return observed
		}
		sleepCtx(stage, pollInterval)
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
	return waitForStateFn(ctx, t, client, baseURL, orderID, pollingBudget,
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
