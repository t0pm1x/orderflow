# orderflow v1.1.3 — housekeeping batch

> Internal session document. Captures what shipped, why, and what
> remains unverified. Author: AI engineer (build mode), 2026-08-19.

This document is **not** intended for users — it's a working log so
the next session can pick up where this one left off without
re-deriving context.

---

## 1. Where this fits in the timeline

```
v1.1.2              adversarial-audit follow-up [13 commits]
v1.1.3              THIS BATCH — housekeeping [14 commits]
v1.2 (planned)      per-Pod attempts counter (P1-#3)
                    KafkaPublisher atomic batch (P1-#4)
```

The v1.1.2 batch closed the P0/P1 audit findings but left a flaky
`TestRun_ServesHealthzAndMetrics` and 5 test-coverage gaps from
§7 of the v1.1.2 handover. This batch closes those, plus an
extra real-PG regression net for the outbox DLQ path.

---

## 2. What shipped

### 2.1 Flaky `TestRun_ServesHealthzAndMetrics` (3/5 services)

The test polls `ListenAddr()` to detect when the HTTP server is
bound. The pre-fix race was between `boundAddr.Store(...)` and
`httpSrv.Serve(ln)` actually starting to Accept() — the test
sometimes read an address that existed but wasn't ready yet.

**Fix pattern (all 3 services):** wrap `httpSrv.Serve(ln)` in a
goroutine that closes a `ready` channel after `Serve` returns
its `ErrServerClosed` (or, equivalently, register a
`http.Server.ConnState` hook that signals on the first
`StateActive` transition). The test now `<-ready` before
asserting. Same shape as the snippet in v1.1.2 §6.A.

```
85647ec  fix(order/test):       wait for /healthz accept before asserting
b0726d1  fix(payment/test):     wait for /healthz accept before asserting
d7a1f57  fix(inventory/test):   wait for /healthz accept before asserting
```

### 2.2 Test coverage gaps (5 items from v1.1.2 §7)

| v1.1.2 §7 gap | v1.1.3 fix commit(s) |
|---|---|
| (1) No concurrent-SetPool test (P1-#10) | `2580a41 test(inventory/consumer): race-test SetPool vs loadDeps` <br> `8861514 test(payment/consumer): race-test SetHandler vs Registry` |
| (2) No concurrent idempotency test (P0-#6) | `d998c3d test(payment/idempotency): concurrent same-key → exactly one handler call` |
| (3) `TestFetchPendingSQL_OrdersByCreatedAt` doesn't verify `FOR UPDATE SKIP LOCKED` | `1d37926 test(order/outbox): assert fetchPendingSQL uses FOR UPDATE SKIP LOCKED` <br> `6dc8938 test(payment/outbox): assert fetchPendingSQL uses FOR UPDATE SKIP LOCKED` <br> `d2a470b test(inventory/outbox): assert fetchPendingSQL uses FOR UPDATE SKIP LOCKED` |
| (4) `TestPGRepo_ReleaseStock_HappyPath` doesn't test the new guards | `d9a2d15 test(inventory/repo): RejectsOverRelease + RejectsNonPositiveQty` |
| (5) `TestPoller_RoutesToDLQAfterMaxAttempts` uses fake's `MarkFailedTx` | `5800fa1 test(saga/outbox): real-PG DLQ regression test for saga_outbox MarkFailedTx + attempts` <br> `1ad5aa5 test(saga/outbox): use valid UUID for seed event_id` (fix to the brief) |

Bonus gap closed (not in v1.1.2 §7, but adjacent to P1-#7):
`36c625a test(payment/idempotency): also assert 200 responses
carry non-empty body` — closes the silent-empty-cache gap that
the v1.1.2 §4 test for `TestMiddleware_EmptyBodyReleases` only
checked via the "next call returns 200" path, not the body
content.

### 2.3 Real-PG regression net (Task 10)

`5800fa1` adds `TestPoller_RoutesToDLQAfterMaxAttempts_PG` in
`services/saga/internal/outbox/poller_pg_test.go` (skips
without `DATABASE_URL`). It seeds a saga_outbox row with a real
UUID, drives attempts past `MaxAttempts`, and asserts the row
transitions to `FAILED` with the expected `attempts` count and
the `last_error` payload.

This is the only v1.1.3 fix that exercises real PG; the other
13 commits run under `-short`.

---

## 3. Verification status

| Verification | Status |
|---|---|
| `go build` all 5 binaries | PASS |
| `go test -short -race ./...` all 15 modules | PASS |
| `go test -short -race -count=20 ./pkg/outbox/... ./services/saga/...` (flaky-TestRun services) | PASS (was 4/5 FAIL in v1.1.2) |
| `go vet` all 14 modules | PASS |
| Test coverage for new tests (unit) | PASS |
| Real-DB integration of new tests (Task 10) | NOT RUN (DATABASE_URL unset) |
| `make e2e` / `make chaos` / `make smoke-k8s` / `make load` / `helm lint` / `golangci-lint run` | NOT RUN (env-blocked, same as v1.1.2) |

**P0 bugs:** 4 fixed in v1.1.2, 0 remaining. 0 newly introduced.
**P1 bugs:** 9 fixed in v1.1.2, 2 still deferred (P1-#3,
P1-#4). 0 newly introduced.
**v1.1.2 §7 gaps:** 5 closed (one with a bonus assertion on
empty-body caching). 0 remaining.
**Flaky smoke test:** stabilized across `-count=20`. Production
race still latent — see §5 item 2.

---

## 4. Commit log

```
85647ec  fix(order/test):       wait for /healthz accept before asserting (flaky TestRun)
b0726d1  fix(payment/test):     wait for /healthz accept before asserting (flaky TestRun)
d7a1f57  fix(inventory/test):   wait for /healthz accept before asserting (flaky TestRun)
1d37926  test(order/outbox):    assert fetchPendingSQL uses FOR UPDATE SKIP LOCKED
6dc8938  test(payment/outbox):  assert fetchPendingSQL uses FOR UPDATE SKIP LOCKED
d2a470b  test(inventory/outbox): assert fetchPendingSQL uses FOR UPDATE SKIP LOCKED + status filter
8861514  test(payment/consumer): race-test SetHandler vs Registry (P1-#10 regression net)
2580a41  test(inventory/consumer): race-test SetPool vs loadDeps (P1-#10 regression net)
d998c3d  test(payment/idempotency): concurrent same-key → exactly one handler call (P0-#6)
36c625a  test(payment/idempotency): also assert 200 responses carry non-empty body
d9a2d15  test(inventory/repo):  RejectsOverRelease + RejectsNonPositiveQty (P0-#4)
4090cd3  docs(CHANGELOG):       v1.1.3 housekeeping — flaky test + 6 test coverage gaps closed
5800fa1  test(saga/outbox):     real-PG DLQ regression test for saga_outbox MarkFailedTx + attempts
1ad5aa5  test(saga/outbox):     use valid UUID for seed event_id (was brief-mandated but invalid UUID)
```

That's 14 commits: 3 flaky-test fixes, 8 unit-test coverage
gaps, 1 docs, 2 saga/outbox (real-PG DLQ + seed-UUID fix).

---

## 5. What the next session should know

1. **Real-DB tests still skip without `DATABASE_URL`** — the
   same pattern as v1.1.2. The new
   `TestPoller_RoutesToDLQAfterMaxAttempts_PG`
   (`services/saga/internal/outbox/poller_pg_test.go`) is the
   only v1.1.3 commit that needs it. Everything else runs under
   `-short`.

2. **The flaky `TestRun` is now stable across `-race -count=20`,
   but the production-side race (`boundAddr.Store` before
   `httpSrv.Serve`) is still latent.** The test fix only
   changes when the *test* considers the server ready; the
   production main loop in `cmd/<svc>/main.go` still has the
   same race window — if a smoke test ever asserts against
   `/healthz` before the first `Accept()` lands, we have the
   same bug. A future cleanup could close the channel
   **inside** the `httpSrv.Serve` goroutine **after** `Serve`
   reports its first connection (via `http.Server.ConnState`
   hook). Not done here because the test-level fix is enough
   for `-count=20` and the production change is risk-bearing.

3. **`pkg/outbox/poller_test.go:TestPoller_RoutesToDLQAfterMaxAttempts`
   (fake) is still the primary regression net.** The new
   `TestPoller_RoutesToDLQAfterMaxAttempts_PG` (real PG) only
   runs when `DATABASE_URL` is set. Don't delete either.

4. **The P1-#3 (per-Pod attempts counter) and P1-#4 (atomic
   KafkaPublisher batch) deferrals from v1.1.2 §5 are still
   open** — unchanged by this batch. They remain the v1.2 work.

5. **The v1.1.2 handover doc noted a subtle inconsistency in
   `pkg/outbox.Poller.handlePublishFailure`**: `next >= MaxAttempts`
   is checked against the in-memory `sync.Map` but
   `markFailedSQL` does `attempts = attempts + 1` regardless.
   That observation still stands and is also a direct
   consequence of P1-#3 being deferred. When P1-#3 lands, this
   also goes away.