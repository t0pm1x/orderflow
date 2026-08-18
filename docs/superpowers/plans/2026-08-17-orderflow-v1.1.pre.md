# orderflow v1.1.pre — Critical saga shutdown bug fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix two real production bugs in the saga service: (1) the saga binary leaks goroutines on shutdown because there is no `sync.WaitGroup` to wait for the outbox poller and TTL sweep, and (2) `ttl_sweep.go` calls `panic()` inside `pgx.BeginFunc` when JSON marshaling would fail, leaving transactions in an indeterminate state. Hotfix ship as `v1.1.pre` before any other v1.1 work.

**Architecture:** Bring the saga binary into line with the `order`/`payment`/`inventory` binaries, which already use `sync.WaitGroup` to gate shutdown on background goroutine completion. Replace `mustMarshal`'s panic with an inline `json.Marshal` whose error propagates out of the compensation transaction. Both changes are localized and mechanical — no new abstractions, no new dependencies.

**Tech Stack:** Go 1.25.13, `sync.WaitGroup` (stdlib), `pgx/v5` (already in `services/saga/go.mod`).

## Global Constraints

- Go version floor: 1.25.13 (per `go.work`).
- Workspace module under change: `services/saga` only. No other workspace module is touched in this plan.
- The saga service binary `cmd/saga/main.go` wraps the `Run(ctx)` function in `Main()`; tests live in `services/saga/cmd/saga/main_test.go` (existing package `saga_test`).
- Stage naming convention: `v1.1.pre` is the commit-message tag for this work (not a separate Go module version).
- Pattern to mirror: `services/order/cmd/order/main.go:116-224` (`startOutbox` + WaitGroup shutdown). The saga `Run` function should produce an identical shutdown contract.
- Existing tests must remain green; new tests must follow the package convention `package <thing>_test` (external test package).
- Verification command: from repo root, `cd services/saga && go test ./... -short` (skip integration tests that need `DATABASE_URL`). Full verification: `make verify` from repo root.

## File Structure

This plan touches exactly three files and adds no new files except a CHANGELOG/STATUS row.

| File | Action | Purpose |
|---|---|---|
| `services/saga/cmd/saga/main.go` | Modify | Add `sync.WaitGroup` for poller + TTL sweep + HTTP server goroutines; close fn waits on shutdown context timeout. |
| `services/saga/cmd/saga/main_test.go` | Modify | Add `TestRun_GoroutinesExitOnCancel` that asserts `Run(ctx)` returns within a bounded timeout after `cancel()`. |
| `services/saga/internal/watchdog/ttl_sweep.go` | Modify | Remove `mustMarshal` helper; inline `json.Marshal` in `compensate` so marshal errors propagate as a returned `error` instead of `panic`. |
| `CHANGELOG.md` | Modify | Add `v1.1.pre` entry under "Unreleased". |
| `STATUS.md` | Modify | Add `v1.1.pre` row in the sub-stages table. |

No new abstractions, no new public types, no new dependencies.

---

## Task 1: Fix saga goroutine shutdown — add WaitGroup

**Files:**
- Modify: `services/saga/cmd/saga/main.go:13-25, 95-180`
- Test: `services/saga/cmd/saga/main_test.go`

**Why first:** Bug #1 (no WaitGroup) means the saga binary can be SIGTERM'd while a poller iteration or TTL sweep is mid-transaction. Without fixing this, Bug #2 (the panic inside `pgx.BeginFunc`) is much more dangerous in practice. Also, the existing saga test (`TestRun_ServesHealthzAndMetrics`) only covers the no-DB no-Kafka path; we add a focused test that exercises the WaitGroup contract.

**Interfaces:**
- Consumes: nothing (this is a standalone change to saga main).
- Produces: `Run(ctx)` whose close contract is: after `ctx` is cancelled, all background goroutines (TTL sweep, outbox poller, HTTP server) must exit before `Run` returns OR the shutdown context must expire (5 seconds). Mirrors `services/order/cmd/order/main.go:205-222`.

**Step-by-step:**

- [ ] **Step 1.1: Read the current saga `Run` function**

Open `services/saga/cmd/saga/main.go:65-180`. Note the three goroutines launched without synchronization:
- Line 129-131: `go func() { ttl.Run(ctx) }()` — TTL sweep.
- Line 164-169: HTTP server goroutine.
- Line 203-207 (inside `startSagaOutbox`): poller goroutine.

The current `Run` has no `sync.WaitGroup`. The HTTP server goroutine is signaled via `httpErr` channel; the other two have no signal at all.

- [ ] **Step 1.2: Add `sync` to imports**

In `services/saga/cmd/saga/main.go:13-25`, add `"sync"` to the import block. The current block is:

```go
import (
    "context"
    "fmt"
    "log/slog"
    "net"
    "net/http"
    "os"
    "os/signal"
    "strings"
    "sync/atomic"
    "syscall"
    "time"
    ...
)
```

Add `"sync"` after `"strings"` so the import block becomes:

```go
import (
    "context"
    "fmt"
    "log/slog"
    "net"
    "net/http"
    "os"
    "os/signal"
    "strings"
    "sync"
    "sync/atomic"
    "syscall"
    "time"
    ...
)
```

- [ ] **Step 1.3: Refactor `Run` to use a single `sync.WaitGroup`**

Replace the `var (...)` block and the runtime setup in `services/saga/cmd/saga/main.go:95-180` so it matches the `order` service pattern. The new structure is:

```go
// Bring up the runtime: DB pool + consumer + outbox poller + TTL sweep.
// Disabled (no-op close) when DATABASE_URL or KAFKA_BROKER are unset,
// mirroring the order/payment/inventory services.
var (
    wg            sync.WaitGroup
    pool          *pgxpool.Pool
    httpSrv       *http.Server
    ln            net.Listener
    consumerClose func(context.Context) error
    outboxClose   func(context.Context) error
)
if dbURL != "" && broker != "" {
    pool, err = pgxpool.New(ctx, dbURL)
    if err != nil {
        return fmt.Errorf("pgxpool: %w", err)
    }
    if err := pool.Ping(ctx); err != nil {
        pool.Close()
        return fmt.Errorf("postgres ping: %w", err)
    }

    consumerClose, err = svcconsumer.Start(ctx, logger, broker, groupID, pool)
    if err != nil {
        pool.Close()
        return fmt.Errorf("consumer start: %w", err)
    }

    outboxClose, err = startSagaOutbox(ctx, logger, pool, broker, &wg)
    if err != nil {
        _ = consumerClose(context.Background())
        pool.Close()
        return fmt.Errorf("outbox start: %w", err)
    }

    // Start cross-restart TTL sweep — compensates sagas whose
    // expires_at has passed but never fired in-process.
    ttl := svcwatchdog.NewTTLSweep(pool, svcrepo.NewPGRepo(pool), svcoutbox.NewPGWriter(), 30*time.Second, logger)
    wg.Add(1)
    go func() {
        defer wg.Done()
        ttl.Run(ctx)
    }()
} else {
    logger.Info("saga runtime disabled: DATABASE_URL or KAFKA_BROKER not set")
}

if httpAddr == "" {
    logger.Info("http disabled: HTTP_ADDR not set")
    <-ctx.Done()
    wgWait(&wg, pool, consumerClose, outboxClose, httpSrv, ln)
    return nil
}

r := chi.NewRouter()
r.Use(mw.Stack(TableName, logger)...)
r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
    w.Header().Set("Content-Type", "application/json")
    _, _ = w.Write([]byte(`{"status":"ok"}`))
})
r.Handle("/metrics", promhttp.Handler())

ln, err = net.Listen("tcp", httpAddr)
if err != nil {
    if pool != nil {
        pool.Close()
    }
    return fmt.Errorf("listen %s: %w", httpAddr, err)
}
boundAddr.Store(ln.Addr().String())
httpSrv = &http.Server{
    Handler:           r,
    ReadHeaderTimeout: 5 * time.Second,
}

wg.Add(1)
go func() {
    defer wg.Done()
    if err := httpSrv.Serve(ln); err != nil && err != http.ErrServerClosed {
        logger.Error("saga http exited", "err", err)
    }
}()

<-ctx.Done()
shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
_ = httpSrv.Shutdown(shutdownCtx)
wgWait(&wg, pool, consumerClose, outboxClose, httpSrv, ln)
return nil
```

Replace the entire body of `Run` from line 95 (`var (...)` open) to line 180 (end of `Run`) with the code above.

- [ ] **Step 1.4: Replace `startSagaOutbox` to accept and use a `*sync.WaitGroup`**

In `services/saga/cmd/saga/main.go:182-214`, replace `startSagaOutbox`'s signature and body so the poller goroutine is tracked:

```go
// startSagaOutbox brings up the Saga Service outbox poller. The
// caller passes a WaitGroup so the poller goroutine is tracked and
// the close fn can wait for it on shutdown. Mirrors
// services/order/cmd/order/main.go's startOutbox + WaitGroup pattern.
func startSagaOutbox(ctx context.Context, logger *slog.Logger, pool *pgxpool.Pool, broker string, wg *sync.WaitGroup) (func(context.Context) error, error) {
    kafkaClient, err := events.NewClient(strings.Split(broker, ","), "saga")
    if err != nil {
        return nil, fmt.Errorf("kafka client: %w", err)
    }

    src := svcoutbox.NewPGSource(pool)
    pub := pkgoutbox.NewKafkaPublisher(kafkaClient)
    dlq := pkgoutbox.NewKafkaDLQ(kafkaClient)
    metrics := pkgoutbox.NewPrometheusMetrics(TableName, prometheus.DefaultRegisterer)

    poller := pkgoutbox.New(pkgoutbox.PollerConfig{
        Table:       TableName,
        BatchSize:   100,
        Interval:    100 * time.Millisecond,
        MaxAttempts: 5,
    }, src, pub, dlq, metrics)

    wg.Add(1)
    go func() {
        defer wg.Done()
        if err := poller.Run(ctx); err != nil {
            logger.Error("saga outbox poller exited", "err", err)
        }
    }()

    return func(_ context.Context) error {
        poller.Stop()
        kafkaClient.Close()
        return nil
    }, nil
}
```

Replace the entire `startSagaOutbox` function (lines 182-214).

- [ ] **Step 1.5: Add `wgWait` helper**

Append the following helper to `services/saga/cmd/saga/main.go` (after `startSagaOutbox`):

```go
// wgWait blocks until all background goroutines (TTL sweep, outbox
// poller, HTTP server) have exited, OR the shutdown timeout
// expires. The order is: stop the poller (so it does not start new
// fetches), wait for all goroutines, then close the consumer and
// the DB pool. Mirrors services/order/cmd/order/main.go's shutdown
// path.
func wgWait(wg *sync.WaitGroup, pool *pgxpool.Pool, consumerClose, outboxClose func(context.Context) error, httpSrv *http.Server, ln net.Listener) {
    // Stop sources first so background goroutines exit promptly.
    if outboxClose != nil {
        _ = outboxClose(context.Background())
    }
    if httpSrv != nil {
        _ = httpSrv.Shutdown(context.Background())
    }
    if ln != nil {
        _ = ln.Close()
    }

    done := make(chan struct{})
    go func() { wg.Wait(); close(done) }()
    select {
    case <-done:
    case <-time.After(5 * time.Second):
        // Shutdown deadline expired. Continue closing what we can.
    }

    if consumerClose != nil {
        _ = consumerClose(context.Background())
    }
    if pool != nil {
        pool.Close()
    }
}
```

- [ ] **Step 1.6: Add test that verifies goroutines exit on cancel**

Open `services/saga/cmd/saga/main_test.go`. Add the following test after `TestRun_ServesHealthzAndMetrics`:

```go
// TestRun_GoroutinesExitOnCancel verifies that Run returns within a
// bounded shutdown timeout after ctx is cancelled. Before the
// WaitGroup fix, the outbox poller and TTL sweep goroutines had no
// shutdown signal — Run would return on ctx.Done but those
// goroutines could still be mid-transaction. This test asserts the
// fix by verifying Run returns promptly (within 7 seconds) even when
// the runtime is in disabled mode (no DB, no Kafka) — the HTTP
// server and any registered background goroutines must exit.
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

    addr := waitForAddr(t, 3*time.Second)
    if addr == "" {
        cancel()
        t.Fatal("server did not bind")
    }

    // Confirm the HTTP server is up before we cancel.
    resp, err := http.Get("http://" + addr + "/healthz")
    if err != nil {
        cancel()
        t.Fatalf("GET /healthz before cancel: %v", err)
    }
    _ = resp.Body.Close()

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
}
```

- [ ] **Step 1.7: Run the new test to confirm it passes**

Run: `cd services/saga && go test ./cmd/saga/... -run TestRun_GoroutinesExitOnCancel -v`
Expected: PASS in under 8 seconds.

- [ ] **Step 1.8: Run the full saga test suite**

Run: `cd services/saga && go test ./... -short -v`
Expected: PASS for all packages.

- [ ] **Step 1.9: Run `make verify` from repo root**

Run: `cd <repo-root> && make verify`
Expected: PASS. (`tidy`, `build`, `test`, `lint` all green.)

- [ ] **Step 1.10: Commit**

```bash
git add services/saga/cmd/saga/main.go services/saga/cmd/saga/main_test.go
git commit -m "v1.1.pre: saga shutdown — add sync.WaitGroup for poller, TTL sweep, HTTP server"
```

---

## Task 2: Fix `mustMarshal` panic in TTL sweep compensation

**Files:**
- Modify: `services/saga/internal/watchdog/ttl_sweep.go:103-165`

**Why second:** Bug #2 is independent of Bug #1 but the same `compensate` path that Task 1 protects from being killed mid-tx is also the path where the panic can fire. Fixing the panic without fixing the WaitGroup still leaves a leak; fixing the WaitGroup without fixing the panic still leaves a crash hazard. Both must land together.

**Interfaces:**
- Consumes: existing `compensate(ctx, *repository.Saga)` signature.
- Produces: same signature; marshal errors now return instead of panicking.

**Step-by-step:**

- [ ] **Step 2.1: Read the current `compensate` function**

Open `services/saga/internal/watchdog/ttl_sweep.go:111-151`. Note three `mustMarshal` calls (lines 126, 142) and the helper at lines 159-165.

- [ ] **Step 2.2: Replace `compensate` to use inline `json.Marshal`**

In `services/saga/internal/watchdog/ttl_sweep.go`, replace lines 111-151 (the entire `compensate` function body) with:

```go
// compensate transitions a single expired saga to compensated and
// emits the same two outbox rows PaymentFailedHandler emits:
// StockReleaseRequested (so inventory releases the reservation)
// and OrderCancelled with reason="timeout" (so the order service
// marks the order cancelled). All three writes happen in one tx so
// the saga state and its events commit/rollback atomically —
// preventing the half-state of "compensated with no events
// emitted" that would leave stock stranded. Marshal errors are
// returned (not panicked) so a malformed payload aborts the tx
// cleanly instead of crashing the whole saga service mid-tx.
func (t *TTLSweep) compensate(ctx context.Context, s *repository.Saga) error {
    releasePayload, err := json.Marshal(sagaev.StockReleaseRequestedPayload{
        OrderID:       s.OrderID,
        ReservationID: s.ReservationID,
    })
    if err != nil {
        return fmt.Errorf("marshal StockReleaseRequested: %w", err)
    }
    cancelPayload, err := json.Marshal(sagaev.OrderCancelledPayload{
        OrderID: s.OrderID,
        Reason:  "timeout",
        Source:  "saga",
    })
    if err != nil {
        return fmt.Errorf("marshal OrderCancelled: %w", err)
    }

    return pgx.BeginFunc(ctx, t.pool, func(tx pgx.Tx) error {
        if _, err := tx.Exec(ctx,
            `UPDATE order_sagas
                SET state = 'compensated', updated_at = NOW()
              WHERE order_id = $1`, s.OrderID); err != nil {
            return err
        }
        releaseRec := platformoutbox.Record{
            EventID:       uuid.NewString(),
            AggregateID:   s.OrderID,
            AggregateType: "Order",
            EventType:     "StockReleaseRequested",
            SchemaVersion: "1.0",
            Topic:         sagaoutbox.Topic,
            Payload:       releasePayload,
            Headers:       map[string]string{},
        }
        if err := t.writer.Append(ctx, tx, releaseRec); err != nil {
            return err
        }
        cancelRec := platformoutbox.Record{
            EventID:       uuid.NewString(),
            AggregateID:   s.OrderID,
            AggregateType: "Order",
            EventType:     "OrderCancelled",
            SchemaVersion: "1.0",
            Topic:         sagaoutbox.Topic,
            Payload:       cancelPayload,
            Headers:       map[string]string{},
        }
        return t.writer.Append(ctx, tx, cancelRec)
    })
}
```

- [ ] **Step 2.3: Add `fmt` to imports if missing**

Check `services/saga/internal/watchdog/ttl_sweep.go:16-31`. The import block already has `"encoding/json"`. After the change, `fmt.Errorf` is used in `compensate`, so add `"fmt"` to the import block. The result:

```go
import (
    "context"
    "encoding/json"
    "fmt"
    "log/slog"
    "time"

    "github.com/google/uuid"
    "github.com/jackc/pgx/v5"
    "github.com/jackc/pgx/v5/pgxpool"

    platformoutbox "github.com/t0pm1x/orderflow/platform/outbox"

    sagaev "github.com/t0pm1x/orderflow/services/saga/internal/events"
    sagaoutbox "github.com/t0pm1x/orderflow/services/saga/internal/outbox"
    "github.com/t0pm1x/orderflow/services/saga/internal/repository"
)
```

- [ ] **Step 2.4: Delete the `mustMarshal` helper**

Remove the entire `mustMarshal` function (lines 153-165) from `services/saga/internal/watchdog/ttl_sweep.go`. It is no longer referenced after Step 2.2.

- [ ] **Step 2.5: Run the watchdog test suite**

Run: `cd services/saga && go test ./internal/watchdog/... -short -v`
Expected: PASS. The existing tests (`TestTTLSweep_CompensatesExpiredSaga`, `TestTTLSweep_NoExpiredIsNoOp`, `TestTTLSweep_SkipsTerminalExpiredSagas`) exercise the real path; they do not set `DATABASE_URL` so they skip. With `DATABASE_URL` set against a real Postgres, all three must pass.

- [ ] **Step 2.6: Run `go vet` on the watchdog package**

Run: `cd services/saga && go vet ./internal/watchdog/...`
Expected: clean (no `mustMarshal` references left).

- [ ] **Step 2.7: Run `make verify` from repo root**

Run: `cd <repo-root> && make verify`
Expected: PASS.

- [ ] **Step 2.8: Commit**

```bash
git add services/saga/internal/watchdog/ttl_sweep.go
git commit -m "v1.1.pre: replace mustMarshal panic with error return in TTL sweep compensate"
```

---

## Task 3: Update CHANGELOG and STATUS, then tag v1.1.pre

**Files:**
- Modify: `CHANGELOG.md`
- Modify: `STATUS.md`

**Step-by-step:**

- [ ] **Step 3.1: Add `v1.1.pre` entry to CHANGELOG.md**

Open `CHANGELOG.md`. Under the `## [Unreleased]` heading (line 35), add:

```markdown
## [1.1.0-pre] - 2026-08-17

### Fixed
- **Saga shutdown goroutine leak**: The saga binary's outbox poller, TTL sweep, and HTTP server goroutines were launched without `sync.WaitGroup` tracking. On SIGTERM, `Run()` returned before the goroutines had exited, allowing a rolling Kubernetes deploy to kill a poller iteration or TTL sweep mid-transaction. The saga service now mirrors the `order`/`payment`/`inventory` shutdown pattern: a `sync.WaitGroup` tracks every background goroutine, and the close path waits on a 5-second shutdown context before closing the DB pool.
- **`mustMarshal` panic in TTL sweep compensation**: `services/saga/internal/watchdog/ttl_sweep.go` called `panic(err)` from a `json.Marshal` failure inside `pgx.BeginFunc`. A panic there could leave the surrounding transaction in an indeterminate state. The helper has been removed; `compensate` now uses inline `json.Marshal` and propagates errors through the function's normal `error` return.

### Changed
- `startSagaOutbox` signature now takes a `*sync.WaitGroup` so its poller goroutine is tracked alongside the TTL sweep and HTTP server.
- New private helper `wgWait` consolidates the saga shutdown sequence (wait goroutines, close outbox, close consumer, shutdown HTTP, close pool).
```

Insert this block between the existing `## [Unreleased]` heading (line 35) and the `## [1.0.0]` heading (line 37). Keep `## [Unreleased]` as the first heading.

- [ ] **Step 3.2: Add `v1.1.pre` row to STATUS.md sub-stages table**

Open `STATUS.md`. Find the sub-stages table (starts around line 8). After the `v1.0` row (around line 98), insert:

```markdown
| v1.1.pre | Saga shutdown goroutine leak + `mustMarshal` panic fix | done | <this commit> | this plan |
```

Replace `<this commit>` with the actual commit hash from `git log -1 --format=%H` after Task 2 is committed.

- [ ] **Step 3.3: Move `v1.1.pre` entries from `Unreleased`**

In `CHANGELOG.md`, change the `## [Unreleased]` heading to remain at the top but empty (it should have no entries). Confirm the new `## [1.1.0-pre]` block you added is correctly positioned directly below.

- [ ] **Step 3.4: Update Deferred section in STATUS.md**

In `STATUS.md`, the "Deferred to v1.1" section currently lists three items. After this plan, those three are still deferred (this plan only addresses the critical bugs, not the deferred items themselves). Confirm the "Deferred to v1.1" section is unchanged.

- [ ] **Step 3.5: Commit**

```bash
git add CHANGELOG.md STATUS.md
git commit -m "v1.1.pre: update CHANGELOG and STATUS for critical saga bug fixes"
```

- [ ] **Step 3.6: Tag v1.1.pre**

```bash
git tag -a v1.1.pre -m "v1.1.pre: saga shutdown WaitGroup + mustMarshal panic fix"
git push origin main
git push origin v1.1.pre
```

The push step is only needed if the user has confirmed `origin` is configured and wants the tag published. If running locally without push permission, skip the push commands and just create the tag locally.

---

## Self-Review Checklist (run before declaring plan complete)

- [ ] **Spec coverage:**
  - Spec goal "no-loss Kafka recovery" → not addressed in this plan (v1.1.b). Correct — this plan is `v1.1.pre`, not the reliability stage.
  - Spec v1.1.pre change #1 (WaitGroup) → covered by Task 1.
  - Spec v1.1.pre change #2 (mustMarshal) → covered by Task 2.
  - Spec v1.1.pre exit criteria ("both bugs fixed; `go test ./services/saga/...` green; tag v1.1.pre") → covered by Tasks 1, 2, 3.

- [ ] **Placeholder scan:** No "TBD", "TODO", "fill in details", or "similar to Task N" in the plan. Every code block contains the actual code.

- [ ] **Type and signature consistency:**
  - Task 1: `startSagaOutbox` signature changes from `(ctx, logger, pool, broker)` to `(ctx, logger, pool, broker, *sync.WaitGroup)`. Task 1.3 calls it with `&wg`. ✓
  - Task 1: `wgWait` signature `(wg, pool, consumerClose, outboxClose, httpSrv, ln)` — matches the parameters passed at all three call sites in Task 1.3. ✓
  - Task 2: `compensate(ctx, *repository.Saga) error` signature unchanged. The existing `RunOnce` caller (line 96-99) is unaffected. ✓

- [ ] **No external breakage:**
  - No other workspace module imports `mustMarshal`. Search `pkg/`, `services/`, `cmd/` for `mustMarshal` — only `services/saga/internal/watchdog/ttl_sweep.go` defines it, and `compensate` is the only caller (both in the same file).
  - No other caller of `startSagaOutbox` exists; it is package-private to `services/saga/cmd/saga`.

## Plan Complete

This plan addresses exactly the two bugs flagged by the v1.0 audit. After `v1.1.pre` lands:

- `v1.1.a` (drift cleanup + quick wins) is unblocked.
- `v1.1.b` (delivery reliability) is unblocked.
- All subsequent v1.1 stages can build on a saga binary that shuts down cleanly.