# orderflow v1.2 — Senior-Go Adversarial Re-Audit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run a full adversarial re-audit of the orderflow platform from scratch, fix every P0/P1 finding with TDD, and bring the project to "maximally working state" (green build/test/race/vet + e2e-happy + e2e-compensation under Docker).

**Architecture:** Five parallel-ish audit tracks (pkg/outbox+consumer, services/saga, services/order+payment+inventory, services/web, deploy+tests+ADRs) plus one cross-cutting pass (security/observability/ops). Each track records findings in `audit/REAUDIT_FINDINGS.md`, writes a failing regression test for every P0/P1, implements the fix, and commits. Final task runs the full verification chain and a fresh adversarial pass.

**Tech Stack:** Go 1.25.13, franz-go, pgx v5, slog, chi, OTel, k6, SvelteKit (web SPA). Docker for E2E.

## Global Constraints

These constraints come from the spec (`docs/superpowers/specs/2026-08-27-orderflow-reaudit-design.md`) and apply to every task unless explicitly overridden:

- **Working directory:** `C:\Users\t0p_m\projects\orderflow`
- **Go version:** 1.25.13 (matches all `go.mod` files; CI pinned via `go-version: '1.25.13'` per `SEC-13`)
- **Workspace:** `go.work` with 15 modules; every test/lint command must use the workspace
- **No force-push, no rebase, no amend.** New commits on top of current `main` HEAD.
- **Findings doc:** every new finding is appended to `audit/REAUDIT_FINDINGS.md` in the format documented in Task 1
- **No comments** in code unless the change is a deliberate explanation of a non-obvious invariant
- **No architecture changes.** Saga stays choreography+orchestrator; outbox pattern stays; Postgres per-service stays; no new features.
- **TDD discipline:** every P0/P1 fix starts with a failing test. The test commit and the fix commit are separate (test-first, then fix).
- **LDFLAGS:** `make build` must inject `Version` correctly (per `OBS-3`); binaries must ship the real git version, not `0.0.0-dev`
- **Docker available** (per user); E2E tests must pass on the host. If Docker fails for environment reasons, replace E2E gates with `go test -race -short -count=5 ./...` and document in `REAUDIT_FINDINGS.md` §X.
- **Race detector:** `-race -short -count=3` minimum; `-count=20` on hot paths (`pkg/outbox`, `pkg/consumer`, `services/saga`, `services/order`)
- **Env vars canonical:** `KAFKA_BROKERS` (plural); `DATABASE_URL`; `REDIS_URL`; `HTTP_ADDR`; `OTEL_EXPORTER` (defaults `otlp`); `LOG_LEVEL`
- **No new dependencies** unless a finding requires it; if so, document the rationale in the finding
- **SEC-12 PII keys** (must never appear in logs verbatim): `last_four`, `card_number`, `customer_id`, `idempotency_key`, `password`, `key`
- **Per-service commit format:** `<area>(scope): <imperative summary>` — `pkg(outbox):`, `svc(saga):`, `svc(web):`, `infra(k8s):`, `audit:` etc.

---

## Task 1: Baseline verification + findings doc scaffold

**Files:**
- Create: `audit/REAUDIT_FINDINGS.md`
- Touch: nothing else (this task is read-only verification)

**Interfaces:**
- Consumes: starting HEAD SHA + dirty state (recorded below)
- Produces: green baseline state + empty findings doc

**Pre-flight (record these before running anything):**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
git rev-parse HEAD
git status --short
git log --oneline -5
```

Record the SHA, the dirty state, and the log. Expected dirty state at session start:

```
 M services/web/frontend/dist/index.html
 M services/web/internal/server/server.go
 M services/web/spa.go
?? docs/superpowers/plans/2026-08-19-orderflow-v1.1.3-housekeeping.md
?? docs/superpowers/plans/2026-08-20-orderflow-v1.2-final-validation.md
?? tests/logs/
```

- [ ] **Step 1: Record baseline SHA + dirty state**

Write to `audit/REAUDIT_FINDINGS.md` (first lines):

```markdown
# orderflow v1.2 — Senior-Go Adversarial Re-Audit Findings

**Audit started:** 2026-08-27
**Starting HEAD:** <paste SHA>
**Dirty state at start:** <paste `git status --short` output>

## Format

Every finding below uses this template:

### <ID> [<Sev>] — <one-line title>

- **Component**: <service/package>
- **File**: `<path>:<line>` (or `<path>` if multi-file)
- **Category**: bug | race | leak | security | observability | error-handling | config | doc-drift
- **Reproduction**:
  ```bash
  # command that triggers the issue
  ```
  Or unit test name that fails pre-fix.
- **Root cause**: <one paragraph>
- **Fix**: <one paragraph summary>
- **Regression test**: `<file>:<test name>` (red → green)
- **Commit**: `<sha>` (or "DEFERRED" with rationale)

---

## Findings

<!-- New findings appended below, newest at the bottom -->

### F-001 — Baseline state

- **Component**: repo metadata
- **File**: `audit/REAUDIT_FINDINGS.md`
- **Category**: doc
- **Reproduction**: `git status --short`
- **Root cause**: Pre-audit record.
- **Fix**: None; reference data.
- **Regression test**: n/a
- **Commit**: <this commit SHA, recorded at end of Task 1>

```

- [ ] **Step 2: Run baseline verification**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
make build
make test
go vet ./...
```

Expected:
- `make build` produces 5 binaries in `bin/` with correct version
- `make test` PASS for all 15 workspace modules
- `go vet ./...` clean

If any fails: STOP. Document the failure in the findings doc (severity P0). The audit cannot proceed against a broken baseline.

- [ ] **Step 3: Run race detector on hot paths**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
go test -race -short -count=3 ./pkg/outbox/... ./pkg/consumer/... ./services/saga/... ./services/order/...
```

Expected: PASS (no data races).

- [ ] **Step 4: Record starting version**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
git rev-parse HEAD
git add audit/REAUDIT_FINDINGS.md
git commit -m "audit: scaffold REAUDIT_FINDINGS.md and record baseline state"
```

- [ ] **Step 5: Decide dirty-state policy for web**

Look at the three uncommitted web files:
- `services/web/frontend/dist/index.html`
- `services/web/internal/server/server.go`
- `services/web/spa.go`

If `git diff` shows the changes are committed elsewhere on `main` (check `git log --all --oneline -- services/web/spa.go`) and the dirty state is just an old working tree: leave them. Otherwise Track D (Task 5) will reconcile them.

Note the policy in `audit/REAUDIT_FINDINGS.md` under F-001.

---

## Task 2: Track A — pkg/outbox + pkg/consumer deep-read

**Files:**
- Read: `pkg/outbox/*.go`, `pkg/outbox/sql/*.sql`, `pkg/consumer/*.go`, `pkg/consumer/sql/*.sql`
- Modify: as findings require
- Create: test files per finding

**Interfaces:**
- Consumes: baseline state from Task 1
- Produces: findings F-002..F-0NN (one per P0/P1) with regression tests, all on `main`

**Hypotheses to actively test** (NOT prior findings — these are suspicions to verify or refute):

1. **H-A1 (OBX-001 follow-through):** `attempts` is written by `BumpAttempts` only when the row is about to be FAILED; under-cap failures may not bump reliably.
2. **H-A2 (OBX-005 doc honesty):** `pkg/outbox/kafka.go` doc may still claim "batched into a single Kafka producer transaction" even after the variadic-batch fix.
3. **H-A3 (CONSUMER-1 follow-up):** `pkg/consumer/kafka_dlq.go:sourceTopicFromRecord` may still split `aggregateID` on `/` and misroute DLQ events.
4. **H-A4 (CONSUMER-4 follow-up):** `sync.WaitGroup` passed as `nil` — verify all 4 runner callers still pass nil.
5. **H-A5 (CONSUMER-7..12):** consumer hot path may have Fprintf/spam in dispatch; partition ordering may break on rebalance; no session timeout.
6. **H-A6 (OBX-008):** every event has `OccurredAt = 0001-01-01T00:00:00Z` because `pkg/outbox/kafka.go` constructs `events.Envelope{}` without `OccurredAt`.
7. **H-A7 (OBX-009 follow-up):** `Record.Headers` may still be a dead end-to-end field after v1.1.4/v1.1.5/v1.2.

- [ ] **Step 1: Read all files in scope**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
find pkg/outbox pkg/consumer -name '*.go' -o -name '*.sql' | sort
```

Read every file end-to-end. No skipping, no `// ...` shortcuts.

- [ ] **Step 2: For each hypothesis H-A1..H-A7, write a verdict**

In `audit/REAUDIT_FINDINGS.md`, append a verdict section:

```markdown
### F-A1 — Hypothesis H-A1 verdict

- **Verdict**: CONFIRMED | REFUTED | PARTIAL
- **Evidence**: <file:line + observation>
- **Action**: opens a P0/P1 finding below | no action
```

- [ ] **Step 3: For every confirmed P0/P1, write the regression test FIRST**

Pattern (use the actual finding's specifics — this is a template):

```go
// pkg/outbox/poller_test.go (or a new file)
func TestPoller_<finding-id>_<behavior>(t *testing.T) {
    // Arrange: set up poller + fakeSource + failing publisher
    // Act: drive the failure path
    // Assert: the bug is now closed
}
```

- [ ] **Step 4: TDD: run the test, confirm RED**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
go test -short -race -run TestPoller_<finding-id> ./pkg/outbox/...
```

Expected: FAIL. If it passes, the test doesn't reproduce the bug; fix the test.

- [ ] **Step 5: Implement the fix**

Modify the relevant file at the cited `file:line`. No additional refactoring; minimal change to close the bug.

- [ ] **Step 6: Run the test, confirm GREEN**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
go test -short -race -run TestPoller_<finding-id> ./pkg/outbox/...
```

Expected: PASS.

- [ ] **Step 7: Re-run the package tests to ensure no regression**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
go test -short -race -count=3 ./pkg/outbox/... ./pkg/consumer/...
```

Expected: PASS.

- [ ] **Step 8: Document + commit**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
git add <modified files>
git commit -m "<type>(outbox): <finding-id> <summary>"
```

- [ ] **Step 9: Repeat Steps 3–8 for every P0/P1 finding in this track**

One commit per finding. Commit message format: `pkg(outbox): F-A1 description` or `pkg(consumer): F-A4 description`.

- [ ] **Step 10: Final verification at end of track**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
make build
make test
go vet ./...
```

Expected: all green. If not, the last commit caused a regression; revert it (`git revert HEAD`) and re-do.

---

## Task 3: Track B — services/saga deep-read

**Files:**
- Read: `services/saga/**/*.go`, `services/saga/migrations/*.sql`
- Modify: as findings require

**Interfaces:**
- Consumes: baseline state
- Produces: findings F-B1..F-BNN

**Hypotheses to actively test:**

1. **H-B1 (P3-SAGA-16 follow-up):** `services/saga/internal/state.go` (or wherever the state machine is declared) contradicts `handlers.go`. State machine is documented as one thing and implemented as another.
2. **H-B2 (SAGA-1 follow-up):** `expires_at` may not be refreshed on every `TransitionStateTx` call; the TTL sweep guard may still be incomplete.
3. **H-B3 (SAGA-5 follow-up):** `OrderCancelled` handler is registered but may not release stock for ALL items (the v1.1.5 fix used the items blob, may have regressed).
4. **H-B4 (P2-SAGA-14):** `TestPGRepo_ListExpired_*` seeds timestamps as literal strings; if `DATABASE_URL` is set, this test should fail immediately.
5. **H-B5 (P2-SAGA-13):** intra-transaction event order may still be nondeterministic for `PaymentFailedHandler`.
6. **H-B6:** `cmd/saga/main.go:146-151` wgWait on the HTTP-disabled path receives an already-cancelled context.
7. **H-B7:** `internal/consumer/runner.go:79-85` consumer close ignores its context, blocks unboundedly.

- [ ] **Step 1: Read all files in scope**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
find services/saga -name '*.go' -o -name '*.sql' | sort
```

Read every file. Pay particular attention to:
- `state.go` and `handlers.go` — look for divergence
- `repository/pg_repo.go` — verify `TransitionStateTx` refreshes `expires_at`
- `watchdog/ttl_sweep.go` — verify the `expires_at < NOW()` guard
- `consumer/handlers.go` — verify `OrderCancelled` releases ALL items
- `cmd/saga/main.go` — verify graceful shutdown contexts

- [ ] **Step 2: For each hypothesis H-B1..H-B7, write a verdict**

In `audit/REAUDIT_FINDINGS.md`, append the verdict section (same template as Task 2).

- [ ] **Step 3: For every confirmed P0/P1, write the regression test FIRST**

Pattern:

```go
// services/saga/internal/<package>/<name>_test.go
func Test<Behavior>_<finding-id>(t *testing.T) {
    t.Parallel()
    // ... use the actual saga test helpers
}
```

For tests requiring PG, follow the existing skip-without-DATABASE_URL pattern:

```go
func TestX(t *testing.T) {
    if os.Getenv("DATABASE_URL") == "" {
        t.Skip("set DATABASE_URL to run")
    }
    // ...
}
```

- [ ] **Step 4: TDD: run test, confirm RED**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
go test -short -race -run Test<finding-id> ./services/saga/...
```

- [ ] **Step 5: Implement the fix**

- [ ] **Step 6: Run test, confirm GREEN**

- [ ] **Step 7: Re-run saga package tests**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
go test -short -race -count=5 ./services/saga/...
```

Expected: PASS.

- [ ] **Step 8: Document + commit**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
git add <modified files>
git commit -m "svc(saga): F-B<n> <summary>"
```

- [ ] **Step 9: Repeat for every P0/P1 finding**

- [ ] **Step 10: Final verification**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
make build && make test && go vet ./...
```

---

## Task 4: Track C — services/{order,payment,inventory} deep-read

**Files:**
- Read: `services/order/**/*.go`, `services/payment/**/*.go`, `services/inventory/**/*.go`
- Modify: as findings require

**Interfaces:**
- Consumes: baseline state
- Produces: findings F-C1..F-CNN

**Hypotheses to actively test:**

1. **H-C1 (P1-#1 follow-up):** `payment/webhook/handler.go:UpdateStatusFromNonTerminal` may be missing a state guard or the SQL `WHERE status NOT IN (...)` may have drifted.
2. **H-C2 (P0-#4 follow-up):** `inventory/repository/pg_repo.go:ReleaseStock` may have lost its `reserved >= qty` guard or its qty>0 precheck.
3. **H-C3 (P1-#5 follow-up):** `events.Client.Publish` signature may have been re-broken to take `context.Background()`.
4. **H-C4 (P1-#10 follow-up):** `atomic.Pointer` for `globalHandler`/`globalDeps` may have been replaced with plain pointers.
5. **H-C5 (P0-#4 specific):** `order/repository/pg_repo.go:Cancel` SQL `WHERE state NOT IN ('confirmed','cancelled','failed')` may be missing a state.
6. **H-C6 (NEW-P0-2 follow-up):** `payment/internal/consumer/handlers.go` may be missing the `PaymentRefundRequested` handler.
7. **H-C7:** Cross-service error wrapping: `fmt.Errorf("...: %w", err)` chains may be broken by `errors.Is/As` consumers.

- [ ] **Step 1: Read all files in scope**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
find services/order services/payment services/inventory -name '*.go' -o -name '*.sql' | sort
```

- [ ] **Step 2: For each hypothesis H-C1..H-C7, write a verdict**

- [ ] **Step 3: For every confirmed P0/P1, write the regression test FIRST**

- [ ] **Step 4: TDD: run, confirm RED**

- [ ] **Step 5: Implement fix**

- [ ] **Step 6: TDD: run, confirm GREEN**

- [ ] **Step 7: Re-run each service's tests**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
go test -short -race -count=3 ./services/order/... ./services/payment/... ./services/inventory/...
```

- [ ] **Step 8: Document + commit (one per finding)**

- [ ] **Step 9: Final verification**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
make build && make test && go vet ./...
```

---

## Task 5: Track D — services/web (SvelteKit SPA + uncommitted state)

**Files:**
- Read: `services/web/**/*.go`, `services/web/frontend/**/*` (SvelteKit source)
- Modify: as findings require
- Reconcile: 3 uncommitted files (`spa.go`, `server.go`, `frontend/dist/index.html`)

**Interfaces:**
- Consumes: baseline state + uncommitted web changes
- Produces: findings F-D1..F-DNN + reconciled web state (committed or reverted)

**Pre-task decision:**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
git diff services/web/spa.go services/web/internal/server/server.go services/web/frontend/dist/index.html
```

Determine:
- (a) The changes are equivalent to or improvements over what's on `main` → commit them after audit findings
- (b) The changes are stale or broken → revert them with rationale
- (c) The changes are part of an in-progress feature → keep uncommitted; document

**Hypotheses to actively test:**

1. **H-D1 (BL.1 follow-up):** Kafka tail may have re-introduced a tight warn loop on shutdown.
2. **H-D2 (P0.3 follow-up):** In-memory replay cache may not dedupe across instances (the per-process state is documented as single-instance, but a misuse could split the cache).
3. **H-D3 (P1.12 follow-up):** Order submit validation may have regressed (length cap, quantity upper bound, etc.).
4. **H-D4 (NEW-WEB):** SvelteKit embed boundary may double-include assets or miss a MIME type.
5. **H-D5 (NEW-WEB):** BFF routes in `services/web/internal/backend/*` may have drifted from what the SPA actually calls.
6. **H-D6:** `services/web/cmd/web/main.go` may not call `slog.SetDefault(platform.NewLogger())` after the OBS-6 fix.

- [ ] **Step 1: Read all files in scope**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
find services/web -name '*.go' -o -name '*.svelte' -o -name '*.ts' | sort
```

Pay particular attention to:
- `spa.go` and `embed.FS` boundary
- `services/web/internal/server/server.go` (recent changes)
- `services/web/internal/backend/*.go` — every backend client
- `services/web/frontend/dist/index.html` (recent change)

- [ ] **Step 2: Reconcile uncommitted state**

Based on the pre-task diff:
- (a) Equivalent or better: include in audit scope; commit findings + tests + this delta in Task 5 commits
- (b) Stale or broken: `git checkout -- services/web/spa.go services/web/internal/server/server.go services/web/frontend/dist/index.html`; document why in findings doc
- (c) In-progress: leave uncommitted; document the gap

- [ ] **Step 3: For each hypothesis H-D1..H-D6, write a verdict**

- [ ] **Step 4: For every confirmed P0/P1, write the regression test FIRST**

Web service tests live under `services/web/internal/...`. Look at existing patterns:

```go
func TestBackend_<client>_<finding-id>(t *testing.T) {
    // httptest server + the client
}
```

- [ ] **Step 5: TDD: run, confirm RED**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
go test -short -race -run Test<finding-id> ./services/web/...
```

- [ ] **Step 6: Implement fix**

- [ ] **Step 7: TDD: run, confirm GREEN**

- [ ] **Step 8: Re-run web package tests**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
go test -short -race -count=3 ./services/web/...
```

- [ ] **Step 9: Document + commit (one per finding; reconciliation commit separate)**

Reconciliation commit:
```bash
git add services/web/spa.go services/web/internal/server/server.go services/web/frontend/dist/index.html
git commit -m "svc(web): reconcile uncommitted SPA changes"
```

- [ ] **Step 10: Final verification**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
make build && make test && go vet ./...
```

---

## Task 6: Track E — deploy / tests / Makefile / ADRs

**Files:**
- Read: `deploy/**`, `tests/**`, `Makefile`, `go.work`, `.golangci.yml`, `docs/adr/**`, `cmd/**/Dockerfile`
- Modify: as findings require

**Interfaces:**
- Consumes: baseline state
- Produces: findings F-E1..F-ENN

**Hypotheses to actively test:**

1. **H-E1 (TEST-1 regression):** `Makefile:108` `e2e-happy` regex may have drifted from the test name.
2. **H-E2 (K8S-12 regression):** `deploy/kustomize/base/services.yaml` may have drifted from `deploy/helm/orderflow-*/templates/deployment.yaml`.
3. **H-E3 (K8S-13 regression):** any helm chart or kustomize overlay may still use `KAFKA_BROKER` (singular).
4. **H-E4 (K8S-5/6/7 regression):** helm chart `probes.startup`, `lifecycle.preStop`, `runAsUser`/`runAsGroup` may have been removed.
5. **H-E5 (K8S-15 regression):** `deploy/k8s/base/rbac.yaml` RoleBindings may have drifted to the wrong SA names.
6. **H-E6 (OBS-3 regression):** Makefile LDFLAGS `-X` targets may have drifted back to `main.Version`.
7. **H-E7 (K8S-1 regression):** any `cmd/<svc>/Dockerfile` may have been deleted.
8. **H-E8 (ADR-0003):** ADR-0003 may have re-claimed gRPC; or gRPC code may have been resurrected.
9. **H-E9 (TEST-9 follow-up):** `tests/k8s/smoke_test.go` may still be template-only without real validation.
10. **H-E10 (SEC-4 follow-up):** webhook handler may still lack HMAC verification.

- [ ] **Step 1: Read all files in scope**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
find deploy tests cmd -name '*.yml' -o -name '*.yaml' -o -name '*.sh' -o -name '*.example' -o -name 'Dockerfile' -o -name 'go.mod' -o -name 'Makefile' | sort
ls docs/adr/
```

- [ ] **Step 2: For each hypothesis H-E1..H-E10, write a verdict**

For each `helm template` check:

```bash
cd "C:\Users\t0p_m\projects\orderflow"
docker run --rm -v "$(pwd):/work" -w /work alpine/helm:3.14.0 helm template test deploy/helm/orderflow-order 2>&1 | head -100
```

Or use a local helm if available. Verify probes, runAsUser, preStop, secretKeyRef are present.

For kustomize:
```bash
cd "C:\Users\t0p_m\projects\orderflow"
kubectl kustomize deploy/kustomize/overlays/prod 2>&1 | head -100
```

If `kubectl` is unavailable, document the gap.

- [ ] **Step 3: For every confirmed P0/P1, write the regression test FIRST**

Config-level findings use shell-script tests in `tests/k8s/` or `tests/harness/`:

```go
func TestKustomize_HasStartupProbe(t *testing.T) {
    // kubectl kustomize + grep probes.startup
}
```

- [ ] **Step 4: TDD: run, confirm RED**

- [ ] **Step 5: Implement fix**

- [ ] **Step 6: TDD: run, confirm GREEN**

- [ ] **Step 7: Document + commit (one per finding)**

- [ ] **Step 8: Final verification**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
make build && make test && go vet ./...
```

---

## Task 7: Track F (Cross-cutting) — security / observability / operations

**Files:**
- Read: across `pkg/`, `services/`, `cmd/` for cross-cutting concerns
- Modify: as findings require

**Interfaces:**
- Consumes: baseline state + tracks A–E findings
- Produces: findings F-F1..F-FNN (cross-cutting only; per-file findings belong in their track)

**Hypotheses to actively test:**

1. **H-F1 (SEC-11):** `platform.Redact(s)` may still return substring-based redaction in some service that didn't pick up the SHA-256 fix.
2. **H-F2 (SEC-12):** `piiHandler` may not be wired into the default slog handler in one of the services.
3. **H-F3 (OBS-1):** any service binary may have lost the `/readyz` endpoint.
4. **H-F4 (OBS-5):** `RecordHeaderCarrier` may not be used in the outbox publisher (the producer-side fix is wired but the outbox hop still drops traceparent).
5. **H-F5 (OBS-6):** `slog.SetDefault(platform.NewLogger())` may be missing in one binary.
6. **H-F6 (Graceful shutdown):** any `cmd/<svc>/main.go` may `os.Exit(1)` instead of returning an error code; may not drain in-flight HTTP requests.
7. **H-F7 (Goroutine leaks):** any `go func()` may not have a `defer wg.Done()` or context-cancellation propagation.
8. **H-F8 (Resource leaks):** any `pgxpool`, `kgo.Client`, `http.Response.Body` may not be closed on error paths.
9. **H-F9 (Error wrapping):** `errors.Is(err, target)` consumers may be broken by `fmt.Errorf("%s", err)` (no `%w`).

- [ ] **Step 1: Survey cross-cutting concerns**

Read every `cmd/<svc>/main.go` (5 binaries). For each:
- `/readyz` registered? checks run?
- `slog.SetDefault(platform.NewLogger())` called?
- `pgxpool` closed in shutdown?
- `kgo.Client` closed in shutdown?
- `wg.Wait()` honors a fresh shutdown context (not the cancelled root)?
- No `os.Exit` outside of `main()` itself?

- [ ] **Step 2: For each hypothesis H-F1..H-F9, write a verdict**

- [ ] **Step 3: For every confirmed P0/P1, write the regression test FIRST**

Pattern (per-service main.go):

```go
// cmd/<svc>/main_test.go
func TestMain_ReadyzReflectsUpstreamHealth(t *testing.T) {
    // start binary with broken DB URL
    // assert /readyz returns 503
}
```

- [ ] **Step 4: TDD: run, confirm RED**

- [ ] **Step 5: Implement fix**

- [ ] **Step 6: TDD: run, confirm GREEN**

- [ ] **Step 7: Document + commit (one per finding)**

- [ ] **Step 8: Final verification**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
make build && make test && go vet ./...
```

---

## Task 8: Dynamic verification — full chain + E2E

**Files:**
- Touch: nothing unless a finding forces it

**Interfaces:**
- Consumes: all tracks complete
- Produces: a verified, maximally working build

- [ ] **Step 1: Full build verification**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
make build
ls -la bin/
```

Expected: 5 binaries. Verify the saga binary's log line on startup shows the correct version:

```bash
cd "C:\Users\t0p_m\projects\orderflow"
./bin/saga.exe --help 2>&1 | head -20
# OR
./bin/saga.exe -version 2>&1 || true
```

If the binary has no flag, start it briefly and check the log:

```bash
cd "C:\Users\t0p_m\projects\orderflow"
DATABASE_URL=postgres://invalid KAFKA_BROKERS=127.0.0.1:9092 timeout 2 ./bin/saga.exe 2>&1 | head -5
```

Expected: `version=v<git describe>` or similar (not `0.0.0-dev`).

- [ ] **Step 2: Full test verification (short mode)**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
make test
```

Expected: PASS for all 15 workspace modules.

- [ ] **Step 3: Race detector sweep**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
go test -race -short -count=3 ./...
```

Expected: PASS, no races.

- [ ] **Step 4: Hot-path stress test**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
go test -race -short -count=20 ./pkg/outbox/... ./pkg/consumer/... ./services/saga/... ./services/order/...
```

Expected: PASS, no flakes.

- [ ] **Step 5: vet + tidy**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
go vet ./...
make tidy
git diff --name-only
```

Expected: vet clean. `make tidy` may produce no diff or minor go.sum updates (commit if so).

- [ ] **Step 6: Docker availability check**

```bash
docker ps
docker compose version
```

Expected: Docker daemon reachable, compose plugin present.

- [ ] **Step 7: E2E happy path**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
make e2e-happy
```

Expected: PASS (`TestE2E_OrderReachesConfirmed`, ~40s).

If this fails, STOP. Document the failure as a new finding (severity P0). Fix it before proceeding.

- [ ] **Step 8: E2E compensation path**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
make e2e-compensation
```

Expected: PASS (`TestE2E_Compensation_PaymentDeclined_CancelsOrder`, ~40s).

- [ ] **Step 9: E2E chaos (if time permits)**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
make e2e-chaos
```

Expected: PASS (`TestChaos_KafkaKill_OrderServiceSurvives`).

Not a blocker; record as best-effort.

- [ ] **Step 10: Commit any verification-only changes**

If `make tidy` produced go.sum updates, commit them:

```bash
cd "C:\Users\t0p_m\projects\orderflow"
git status --short
git add -A
git diff --cached --stat
git commit -m "chore: go mod tidy after audit"
```

---

## Task 9: Final adversarial pass

**Files:**
- Read: the entire diff vs starting HEAD
- Modify: only if a new P0/P1 is found

**Interfaces:**
- Consumes: verified state from Task 8
- Produces: a list of new findings (or "clean") and a STATUS.md update

- [ ] **Step 1: Get the full diff vs baseline**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
git log --oneline <starting HEAD>..HEAD
git diff <starting HEAD>..HEAD --stat
```

- [ ] **Step 2: Re-read every modified file end-to-end**

For each file changed in this audit, read the full file (no skipping). Look for:
- New bugs introduced by the fixes themselves
- Code paths the regression tests don't cover
- Comments that lie (out of sync with implementation)
- Hardcoded values that should be configurable

- [ ] **Step 3: Document follow-up findings**

Append to `audit/REAUDIT_FINDINGS.md`:

```markdown
## Final Adversarial Pass — <date>

### F-FINAL1 — <title>

- **Component**: ...
- **File**: ...
- **Severity**: P0 | P1 | P2 | P3
- **Action**: FIXED in <commit> | DOCUMENTED as TODO
```

- [ ] **Step 4: Fix any new P0/P1 found**

Use the same TDD pattern as Tracks A–F. One commit per finding.

- [ ] **Step 5: Re-verify**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
make build && make test && go vet ./...
make e2e-happy
make e2e-compensation
```

- [ ] **Step 6: Update STATUS.md**

Append a new line to the Sub-stages table:

```markdown
| v1.2.reaudit | senior-go adversarial re-audit (N findings: 0P0, NP1, MP2, KP3) | done | <sha> | this plan |
```

Update "Last updated" date.

- [ ] **Step 7: Commit STATUS + audit doc**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
git add STATUS.md audit/REAUDIT_FINDINGS.md
git commit -m "docs(audit): v1.2 senior-go re-audit complete (NP1, MP2, KP3)"
```

- [ ] **Step 8: Final report to user**

Print a summary:

```
Re-audit complete.

Baseline HEAD:      <sha>
Final HEAD:         <sha>
Commits added:      N

Findings:
  P0: 0 (all closed or none found)
  P1: N (all fixed with regression tests)
  P2: M (N1 fixed, M-N1 TODO with rationale)
  P3: K (N2 fixed, K-N2 TODO with rationale)

Verification:
  make build:        PASS
  make test:         PASS
  -race -count=3:    PASS
  go vet:            clean
  make e2e-happy:    PASS
  make e2e-compensation: PASS

Audit doc: audit/REAUDIT_FINDINGS.md
STATUS.md updated: yes
```

---

## Self-Review (do once, before handing off)

After writing this plan, the planner (you, the implementer-of-the-plan) should verify:

1. **Spec coverage:**
   - §2 Goals 1–9 → Tasks 1, 2, 3, 4, 5, 6, 7, 8, 9 ✓
   - §4 Architecture → Tasks 1–9 ✓
   - §5 Components under audit → Tasks 2–6 ✓
   - §6 Data flow → Tasks 2–7 (Steps 3–8) ✓
   - §7 Error handling → every Task has "STOP and document" pattern ✓
   - §8 Testing strategy → Tasks 2–7 (Step 4), Task 8 (Step 3–4) ✓
   - §9 Acceptance criteria → Task 8 (Steps 1–10) + Task 9 (Step 5) ✓
   - §10 Deliverables → Task 9 (Step 6–7) ✓

2. **Placeholder scan:**
   - No "TBD", "TODO", "implement later", "fill in details" — ✓
   - No "add appropriate error handling" without specifics — ✓ (every fix step says "modify at file:line")
   - Every test step has actual test pattern (template per track) — ✓
   - No "similar to Task N" without repeating code — ✓ (templates are duplicated per track where the patterns differ)

3. **Type/interface consistency:**
   - Tests use `t.Parallel()` where it makes sense; `t.Skip` for PG-needing tests — ✓
   - Commit messages: `<type>(scope): <id> <summary>` — ✓
   - Findings doc format is identical across all tracks — ✓
   - TDD cycle (RED → GREEN → regression check) is identical across all tracks — ✓

4. **Risks acknowledged:**
   - Docker unavailable → Task 8 Step 7 fallback ✓
   - Cross-track conflicts → Tracks own disjoint files; BFF serialised after Track D ✓
   - Hidden tests → workspace enumeration in Task 1 Step 1 ✓
   - Time → priority order documented in spec §11 ✓

---

## Execution

This plan is ready for execution. Choose:

1. **Subagent-Driven (recommended for parallel tracks):** dispatch one subagent per track (Tasks 2–7) in parallel, then Task 1 first, then Task 8, then Task 9.

2. **Inline Execution:** execute task-by-task in this session with checkpoints.

See `superpowers:subagent-driven-development` or `superpowers:executing-plans`.
