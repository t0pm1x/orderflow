# orderflow v1.2 — Senior-Go Adversarial Re-Audit Findings

**Audit started:** 2026-08-27
**Starting HEAD:** caba51f (spec) → ea41dd1 (plan)
**Mode:** Inline execution (subagent-driven skipped per user direction — too slow)
**Environment:** Windows, Go 1.25.4 host with `GOTOOLCHAIN=auto` (downloads 1.25.13), Docker available, kind/k8s not available.

## Format

Every finding uses this template:

### <ID> [<Sev>] — <one-line title>

- **Component**: <service/package>
- **File**: `<path>:<line>` (or `<path>` if multi-file)
- **Category**: bug | race | leak | security | observability | error-handling | config | doc-drift
- **Reproduction**: command or test name
- **Root cause**: one paragraph
- **Fix**: one paragraph summary
- **Regression test**: `<file>:<test name>` (red → green)
- **Commit**: `<sha>`

## Findings

### F-001 [P0] — SvelteKit SPA rewrite fails to compile (webroot.appFS unexported)

- **Component**: services/web (SvelteKit SPA rewrite)
- **File**: `services/web/spa.go:53,61,67` and `services/web/internal/server/server.go:202,221,234,243`
- **Category**: bug
- **Reproduction**: `make build` exits 1 with "undefined: webroot.appFS (but have AppFS)" × 4 sites
- **Root cause**: Uncommitted SvelteKit SPA rewrite (commit 96755b3 + 3 dirty files) declared `indexHTML`/`appFS`/`faviconSVG` unexported in `package web`, while `server.go` imports it as `webroot` and references the unexported names. Symbol mismatch blocks the entire `make build`.
- **Fix**: Capitalize the three vars in `spa.go` (IndexHTML, AppFS, FaviconSVG); update 4 call sites in `server.go`. No semantic change.
- **Regression test**: `make build` produces all 5 binaries with the correct LDFLAGS versions.
- **Commit**: `f4f3083`

### F-002 [P1] — Flaky `TestRun_ServesHealthzAndMetrics` in services/saga/cmd/saga

- **Component**: services/saga
- **File**: `services/saga/cmd/saga/main_test.go:44`
- **Category**: race
- **Reproduction**: `go test -short -race -count=3 ./services/saga/cmd/saga/...` — fails ~2/3 with `connectex: No connection could be made`
- **Root cause**: `waitForAddr` polls `saga.ListenAddr()` and returns as soon as the address is non-empty. The listener may be bound but `httpSrv.Serve(ln)` not yet accepting; the test's `http.Get` races against `Accept()`. The companion helper `waitForFreshReadyAddr` already polls the GET until success — but this test never used it. The v1.1.3 audit flagged this exact race (P0 in order/payment/inventory); saga was missed.
- **Fix**: Switch `TestRun_ServesHealthzAndMetrics` to `waitForFreshReadyAddr`. One-line change.
- **Regression test**: `go test -short -race -count=5 ./services/saga/cmd/saga/...` now 5/5 PASS.
- **Commit**: `c739d69`

---

## Baseline (pre-audit state)

- `make build` FAILED (F-001 blocks all 5 binaries).
- `make test` PASS for all 15 workspace modules (short mode).
- `go vet ./...` clean.
- `go test -race -short -count=3` on `pkg/outbox`, `pkg/consumer`, `services/order`, `services/web` — PASS.
- `go test -race -short -count=3` on `services/saga` — FLAKY (F-002), now fixed.

## Verification progress

| Step | Result |
|------|--------|
| `make build` (GOTOOLCHAIN=auto) | PASS (5 binaries, correct LDFLAGS) |
| `make test` per module | PASS (15/15) |
| `go vet ./...` per module | PASS (15/15) |
| `go test -race -short -count=3` on pkg/outbox, pkg/consumer, services/order, services/web | PASS |
| `go test -race -short -count=5` on services/saga | PASS (after F-002 fix) |
| `make e2e-happy` (Docker) | TBD |
| `make e2e-compensation` (Docker) | TBD |
