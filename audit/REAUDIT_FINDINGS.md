# orderflow v1.2 — Senior-Go Adversarial Re-Audit Findings

**Audit started:** 2026-08-27
**Starting HEAD:** ea41dd1 (plan) on `main`
**Mode:** Inline execution (subagent-driven skipped per user direction — too slow)
**Environment:** Windows host with Go 1.25.4, `GOTOOLCHAIN=auto` downloads 1.25.13 toolchain.
Docker available; kind/k8s NOT available.

## Summary

| Severity | Count | Status |
|----------|-------|--------|
| P0       | 1     | FIXED (F-001 — SvelteKit SPA rewrite uncompilable) |
| P1       | 1     | FIXED (F-002 — flaky TestRun_ServesHealthzAndMetrics) |
| P2       | 0     | — |
| P3       | 0     | — |

**Outcome:** Project now passes `make build` (all 5 binaries, correct LDFLAGS versions),
`make test` (15/15 modules), `go vet` (15/15 clean), `-race -short -count=10/20`
on hot paths, `make e2e-happy` (43.30s), `make e2e-compensation` (44.69s).

## Findings

### F-001 [P0] — SvelteKit SPA rewrite fails to compile (webroot.appFS unexported)

- **Component**: services/web (SvelteKit SPA rewrite)
- **File**: `services/web/spa.go:53,61,67` and `services/web/internal/server/server.go:202,221,234,243`
- **Category**: bug
- **Reproduction**: `make build` exits 1 with `undefined: webroot.appFS (but have AppFS)` × 4 sites
- **Root cause**: Uncommitted SvelteKit SPA rewrite (commit 96755b3 + 3 dirty files) declared `indexHTML`/`appFS`/`faviconSVG` unexported in `package web`, while `server.go` imports it as `webroot` and references the unexported names. Symbol mismatch blocks the entire `make build`.
- **Fix**: Capitalize the three vars in `spa.go` (`IndexHTML`, `AppFS`, `FaviconSVG`); update 4 call sites in `server.go`. No semantic change.
- **Regression test**: `make build` produces all 5 binaries with the correct LDFLAGS versions.
- **Commit**: `f4f3083`

### F-002 [P1] — Flaky `TestRun_ServesHealthzAndMetrics` in services/saga/cmd/saga

- **Component**: services/saga
- **File**: `services/saga/cmd/saga/main_test.go:44`
- **Category**: race
- **Reproduction**: `go test -short -race -count=3 ./services/saga/cmd/saga/...` — fails ~2/3 with `connectex: No connection could be made`
- **Root cause**: `waitForAddr` polls `saga.ListenAddr()` and returns as soon as the address is non-empty. The listener may be bound but `httpSrv.Serve(ln)` not yet accepting; the test's `http.Get` races against `Accept()`. The companion helper `waitForFreshReadyAddr` already polls the GET until success — but this test never used it. The v1.1.3 audit flagged this exact race (P0 in order/payment/inventory); saga was missed.
- **Fix**: Switch `TestRun_ServesHealthzAndMetrics` to `waitForFreshReadyAddr`. One-line change.
- **Regression test**: `go test -short -race -count=5 ./services/saga/cmd/saga/...` now 5/5 PASS; `-count=10` PASS.
- **Commit**: `c739d69`

---

## Items confirmed pre-audit (NOT new findings)

The following are documented in the prior FINAL_AUDIT.md and verified to still hold:

- **P3-SAGA-16 (state.go dead code)**: `services/saga/state.go` declares an in-memory `transitionTable` that is **never used by the consumer handlers** — they go directly to `repository.TransitionStateTx` with SQL state guards. Verified: `grep "transitionTable" services/saga/...` shows zero production callers; only `state_test.go` (the dead-code test). The runtime is correct; the documentation is misleading. Pre-existing; not a regression.
- **`services/web/internal/redis/doc.go`** (deleted in v1.1.4) and **`services/order/internal/saga/doc.go`** (also deleted) — confirmed deleted.
- **State guards on consumer `updateState`**: confirmed `state NOT IN ('confirmed','cancelled','failed')` in `services/order/internal/consumer/handlers.go:138`.
- **`atomic.Pointer` for `globalHandler`/`globalDeps`**: confirmed in `payment` and `inventory`; saga/order have no global state.
- **PaymentRefundRequested handler** wired in `services/payment/internal/consumer/handlers.go:77` — NEW-P0-2 fix from prior audit confirmed.
- **`make e2e-happy` regex** (`-run TestE2E_OrderReachesConfirmed`) — TEST-1 fix confirmed.

## Items NOT verified (env-blocked)

- **kind cluster smoke** (`make smoke-k8s`) — no kind binary.
- **`helm template` against real cluster** — no cluster.
- **Real-PK integration of v1.1.5 OBX-002 path** — no `DATABASE_URL`; integration tests skip cleanly.
- **`golangci-lint run`** — not installed locally.

## Verification matrix (final)

| Command | Result | Notes |
|---------|--------|-------|
| `make build` (GOTOOLCHAIN=auto) | PASS | 5 binaries, LDFLAGS inject `v1.1.4-131-gc739d69` |
| `make test` per module (15 modules) | PASS | 15/15 |
| `go vet ./...` per module (15 modules) | PASS | 15/15 |
| `go test -race -short -count=3` on `pkg/outbox`, `pkg/consumer`, `services/order`, `services/saga` | PASS | all clean |
| `go test -race -short -count=20` on `pkg/outbox`, `pkg/consumer` | PASS | stress clean |
| `go test -race -short -count=10` on `services/order` | PASS | stress clean |
| `make e2e-happy` (Docker) | PASS | TestE2E_OrderReachesConfirmed, 43.30s |
| `make e2e-compensation` (Docker) | PASS | TestE2E_Compensation_PaymentDeclined_CancelsOrder, 44.69s |
| `make e2e-chaos` (Docker) | SKIPPED | time-budget; known PASS in CI per prior audit |

## Commits added during this audit

| SHA | Summary |
|-----|---------|
| `caba51f` | docs(spec): v1.2 senior-go re-audit design (already committed before audit start) |
| `ea41dd1` | docs(plan): v1.2 senior-go re-audit implementation plan (already committed before audit start) |
| `f4f3083` | fix(web): export spa.go embed symbols (AppFS/IndexHTML/FaviconSVG) — F-001 |
| `c739d69` | fix(saga/test): use waitForFreshReadyAddr in TestRun_ServesHealthzAndMetrics — F-002 |
| `929e6dd` | audit: record REAUDIT_FINDINGS.md baseline + F-001/F-002 status |
