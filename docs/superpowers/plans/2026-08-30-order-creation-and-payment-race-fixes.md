# Bug 1 + Bug 2 Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix two playground-blocking bugs in the orderflow BFF and order consumer: (1) BFF auto-generates `customer_id` when SPA submits an empty one (regression introduced by SvelteKit rewrite `96755b3`); (2) order consumer handles `PaymentCompleted` directly so the `pending → confirmed` transition doesn't depend on the saga's 3-topic consumer racing correctly.

**Architecture:** Surgical fixes inside existing files. Bug 1 = `+12` lines in `services/web/internal/server/api.go` `SubmitOrder` handler (uuid auto-gen + regression test). Bug 2 = `+18` lines in `services/order/internal/consumer/handlers.go` (new `PaymentCompleted` handler method + registry entry + existing nil-pool test extension). No new files except one test extension. No schema changes. No new dependencies.

**Tech Stack:** Go 1.25.13 (per `go.work`), chi v5.3.1, github.com/google/uuid (already in `go.sum`), pgx/v5, slog.

**Spec:** `docs/superpowers/specs/2026-08-30-order-creation-and-payment-race-fixes-design.md`

## Global Constraints

- Go version floor: **1.25.13** (per `go.work`).
- All backend changes inside `services/web/internal/server/` (Bug 1) and `services/order/internal/consumer/` (Bug 2). No edits to `services/order/internal/api/`, `services/saga/`, `services/payment/`, `pkg/`, or `cmd/`.
- **DO NOT touch pre-existing F-010 modifications** in the working tree:
  - `services/web/internal/backend/payment.go`
  - `services/web/internal/backend/types.go`
  - the F-010 parts of `services/web/internal/server/api.go` (the `paymentFireReq`/`OrderID` additions in `FireWebhook`).
  - The ONLY changes to `services/web/internal/server/api.go` are: (a) adding `"github.com/google/uuid"` to imports, (b) the auto-gen block inside `SubmitOrder`. Nothing else in that file.
- The pre-existing modifications to `services/web/frontend/dist/index.html` are unrelated; leave them alone.
- Tests use the existing conventions:
  - `services/web/internal/server/api_test.go`: external-test-package `package server_test`, uses existing `fakeOrder`/`fakePayment`/`fakeInventory` fakes.
  - `services/order/internal/consumer/handlers_test.go`: internal `package consumer`, no external fake DB.
- No new external Go dependencies. `github.com/google/uuid` is already in `go.sum`; verify with `grep uuid services/web/go.sum` before adding the import.
- No new npm dependencies (no frontend changes).
- Commit message style: `fix(web): <short description>` for Bug 1; `fix(order): <short description>` for Bug 2.
- All identifiers stay ASCII-only.
- `updateState` in `services/order/internal/consumer/handlers.go:112-150` already guards terminal states (`WHERE state NOT IN ('confirmed', 'cancelled', 'failed')`). The new `PaymentCompleted` handler relies on this guard for idempotency. **Do not modify `updateState`.**

---

## File Structure Map

### New files

None.

### Modified files

| Path | Modified by | Change |
|---|---|---|
| `services/web/internal/server/api.go` | T1 | Add `uuid` import + auto-gen in `SubmitOrder` |
| `services/web/internal/server/api_test.go` | T2 | Add `TestAPI_SubmitOrder_AutoGenCustomerID_OnEmpty` |
| `services/order/internal/consumer/handlers.go` | T3 | Add `PaymentCompleted` to registry + handler method |
| `services/order/internal/consumer/handlers_test.go` | T4 | Extend `TestRegistry_HasAllEventTypes` `want` slice |

---

# STAGE 1 — Bug 1: customer_id auto-generation in BFF

## Task 1: Add `uuid.NewString()` to `SubmitOrder`

**Files:**
- Modify: `services/web/internal/server/api.go` (imports block + `SubmitOrder` body)

**Why:** This is the actual fix for Bug 1. The SvelteKit rewrite dropped the `uuid.NewString()` call that the pre-existing htmx handler had at `services/web/internal/handlers/pages.go` (deleted by commit `96755b3`). The placeholder promise in `/orders/new` survives but the implementation is gone.

**Interfaces:**
- Consumes: `req.CustomerID string` (from the existing `orderSubmitReq` struct).
- Produces: `req.CustomerID` populated with a fresh UUID v4 when it was empty on entry.

- [ ] **Step 1.1: Verify `github.com/google/uuid` is already in `go.sum`**

Run:
```bash
grep "google/uuid" "C:\Users\t0p_m\projects\orderflow\services\web\go.sum" | Select-Object -First 5
```

Expected: at least one line matching. If the output is empty, STOP and report `BLOCKED` to the controller — the import would require a new dependency.

- [ ] **Step 1.2: Read the current `SubmitOrder` to find the exact insertion point**

Open `services/web/internal/server/api.go`. The `SubmitOrder` handler begins at line 292 (per spec). Find:
- The imports block (top of the file).
- The validation chain that ends with `if a.replaySeen(req.IdempotencyKey, time.Now()) { ... }` (around line 328).
- The `submit := backend.OrderSubmit{...}` construction (around line 339).
- The existing `if req.CustomerID != "" && !isValidUUID(req.CustomerID)` check (around line 323) — note this for code locality.

- [ ] **Step 1.3: Add `uuid` to the imports block**

In `services/web/internal/server/api.go`, in the import block (currently around lines 25-37), add the new import. Match the existing alphabetical/grouped ordering:

```go
import (
    "encoding/json"
    "errors"
    "log/slog"
    "net/http"
    "sync"
    "time"

    "github.com/go-chi/chi/v5"

    "github.com/google/uuid"

    "github.com/t0pm1x/orderflow/services/web/internal/backend"
)
```

If the existing import block is not grouped as above, place `"github.com/google/uuid"` in alphabetical position among the third-party imports.

- [ ] **Step 1.4: Insert the auto-gen block in `SubmitOrder`**

In `SubmitOrder`, immediately after the existing `if req.CustomerID != "" && !isValidUUID(req.CustomerID) { ... }` block (around line 323-327), and before the `if req.IdempotencyKey == ""` check, add:

```go
// Auto-generate customer_id when the SPA sends an empty value.
// /orders/new form has placeholder text promising "auto-generated
// UUID" — that promise was lost in the SvelteKit rewrite (commit
// 96755b3 deleted the uuid.NewString() call from the pre-existing
// htmx handler). The Order Service rejects empty customer_id with
// 400 VALIDATION because orders.customer_id is NOT NULL UUID.
// F-009 / F-010 set the precedent that BFF owns this UX promise.
if req.CustomerID == "" {
    req.CustomerID = uuid.NewString()
}
```

Verify the placement: the new block must be between the existing UUID validation and the `backend.OrderSubmit` construction. Reading `req.CustomerID` after this block yields a non-empty UUID; the existing `if req.CustomerID != "" && !isValidUUID(...)` block above does not need to change.

- [ ] **Step 1.5: Build the BFF to confirm compile**

```bash
cd "C:\Users\t0p_m\projects\orderflow\services\web"
go build ./...
```

Expected: `go build` exits 0 with no output.

- [ ] **Step 1.6: Commit (TDD note: no test yet — added by Task 2)**

Per the plan's TDD structure, we commit the implementation first so the failing test in Task 2 has something to fail against. The TDD RED phase is "the new test fails because the implementation isn't there yet" — but since we're implementing across two tasks, Task 2's test will be a regression test that PASSES on the implementation from Task 1. If you'd rather flip the order (write test first, watch it pass immediately because Task 1 hasn't landed yet → actually, the test would FAIL because the function wouldn't auto-generate), do so; the per-task order is flexible as long as both ship together.

```bash
cd "C:\Users\t0p_m\projects\orderflow"
git add services/web/internal/server/api.go
git commit -m "fix(web): auto-generate customer_id in SubmitOrder when empty"
```

---

## Task 2: Add `TestAPI_SubmitOrder_AutoGenCustomerID_OnEmpty`

**Files:**
- Modify: `services/web/internal/server/api_test.go` (append new test)

**Why:** Regression test that locks in the auto-generation behavior. Without this test, the implementation could regress silently in another rewrite.

- [ ] **Step 2.1: Read the existing test file structure**

Open `services/web/internal/server/api_test.go`. Verify:
- Package declaration is `package server_test` (line 7 per the earlier read).
- `fakeOrder` struct exists with a `lastSubmit backend.OrderSubmit` field (around line 37).
- Existing tests construct the chi router manually with `r.Post("/api/orders", api.SubmitOrder)`.
- `isValidUUID` helper exists in the production code (used implicitly by `SubmitOrder`).

- [ ] **Step 2.2: Add the new test**

Append the following to the end of `services/web/internal/server/api_test.go`:

```go
// TestAPI_SubmitOrder_AutoGenCustomerID_OnEmpty locks in the fix
// for the SvelteKit-rewrite regression: when the SPA submits an
// order with no customer_id (placeholder text in /orders/new
// promises "auto-generated UUID"), the BFF must generate a UUID
// rather than forwarding the empty string to the Order Service
// (which rejects with 400 VALIDATION because orders.customer_id
// is NOT NULL UUID).
func TestAPI_SubmitOrder_AutoGenCustomerID_OnEmpty(t *testing.T) {
    o := &fakeOrder{
        submitResp: &backend.Order{ID: "00000000-0000-4000-8000-000000000001"},
    }
    api := &API{Order: o, Logger: slog.Default()}

    r := chi.NewRouter()
    r.Post("/api/orders", api.SubmitOrder)

    body := `{"idempotency_key":"00000000-0000-4000-8000-000000000002","items":[{"sku":"WIDGET","quantity":1}]}`
    req := httptest.NewRequest(http.MethodPost, "/api/orders", strings.NewReader(body))
    req.Header.Set("Content-Type", "application/json")
    rec := httptest.NewRecorder()

    r.ServeHTTP(rec, req)

    if rec.Code != http.StatusCreated {
        t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
    }
    if o.lastSubmit.CustomerID == nil {
        t.Fatalf("CustomerID was nil — BFF did not forward the field at all")
    }
    got := *o.lastSubmit.CustomerID
    if got == "" {
        t.Fatalf("CustomerID was empty string — BFF forwarded the raw empty value instead of auto-generating")
    }
    if !isValidUUID(got) {
        t.Errorf("auto-generated CustomerID=%q is not a valid UUID", got)
    }
    if got == "00000000-0000-4000-8000-000000000002" {
        t.Errorf("CustomerID matches the idempotency_key by coincidence — fake or copy-paste bug?")
    }
}

// TestAPI_SubmitOrder_PassesThroughCustomerID_WhenProvided locks in
// the inverse path: when the SPA supplies a customer_id explicitly,
// the BFF must forward it verbatim rather than overwriting it.
func TestAPI_SubmitOrder_PassesThroughCustomerID_WhenProvided(t *testing.T) {
    o := &fakeOrder{
        submitResp: &backend.Order{ID: "00000000-0000-4000-8000-000000000003"},
    }
    api := &API{Order: o, Logger: slog.Default()}

    r := chi.NewRouter()
    r.Post("/api/orders", api.SubmitOrder)

    provided := "11111111-2222-3333-4444-555555555555"
    body := `{"idempotency_key":"00000000-0000-4000-8000-000000000004","customer_id":"` + provided + `","items":[{"sku":"WIDGET","quantity":1}]}`
    req := httptest.NewRequest(http.MethodPost, "/api/orders", strings.NewReader(body))
    req.Header.Set("Content-Type", "application/json")
    rec := httptest.NewRecorder()

    r.ServeHTTP(rec, req)

    if rec.Code != http.StatusCreated {
        t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
    }
    if o.lastSubmit.CustomerID == nil {
        t.Fatalf("CustomerID was nil")
    }
    if got := *o.lastSubmit.CustomerID; got != provided {
        t.Errorf("CustomerID=%q, want %q (BFF must not overwrite an explicit value)", got, provided)
    }
}
```

The `strings` import is already in the file per the earlier read (line 18).

- [ ] **Step 2.3: Run the tests**

```bash
cd "C:\Users\t0p_m\projects\orderflow\services\web"
go test ./internal/server/... -run "TestAPI_SubmitOrder_AutoGen|TestAPI_SubmitOrder_PassesThrough" -v
```

Expected: both tests PASS. Output should show:

```
=== RUN   TestAPI_SubmitOrder_AutoGenCustomerID_OnEmpty
--- PASS: TestAPI_SubmitOrder_AutoGenCustomerID_OnEmpty (0.00s)
=== RUN   TestAPI_SubmitOrder_PassesThroughCustomerID_WhenProvided
--- PASS: TestAPI_SubmitOrder_PassesThroughCustomerID_WhenProvided (0.00s)
PASS
```

If either test FAILS:
- `AutoGen` test failing on the `isValidUUID` check: the implementation may be using a different UUID format. Verify by reading the auto-gen block in api.go.
- `PassesThrough` test failing: the implementation may be unconditionally overwriting CustomerID. Fix: gate the auto-gen on `== ""` (which the spec mandates).

- [ ] **Step 2.4: Run the full BFF test suite**

```bash
cd "C:\Users\t0p_m\projects\orderflow\services\web"
go test ./...
```

Expected: all tests pass; the `server` package takes ~5s (per Task 11 baseline) and includes the new tests plus all existing tests including `TestHealthAll_*`.

- [ ] **Step 2.5: Commit**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
git add services/web/internal/server/api_test.go
git commit -m "test(web): cover SubmitOrder customer_id auto-gen + pass-through"
```

---

# STAGE 2 — Bug 2: order consumer handles PaymentCompleted directly

## Task 3: Add `PaymentCompleted` handler method + registry entry

**Files:**
- Modify: `services/order/internal/consumer/handlers.go` (registry + new method)

**Why:** Bug 2 fix. The saga consumer's silent `ErrNotFound` skip on `PaymentCompleted` (when the saga row hasn't been committed yet due to cross-topic race) leaves the order stuck in `pending`. The order consumer needs a direct path, mirroring the existing `PaymentFailed → cancelled` pattern. The existing `updateState` (lines 112-150) is already idempotent via its `WHERE state NOT IN ('confirmed', 'cancelled', 'failed')` clause — no wrapper needed.

**Interfaces:**
- Consumes: `events.Envelope` with `EventType == "PaymentCompleted"`; payload JSON `{"order_id": "<UUID>", "payment_id": "<UUID>"}` (matches `services/payment/internal/webhook/handler.go:82-86`).
- Produces: `UPDATE orders SET state='confirmed' WHERE id=$1 AND state NOT IN ('confirmed', 'cancelled', 'failed')`.

- [ ] **Step 3.1: Read the current `Registry` and `updateState` to confirm the pattern**

Already done during plan self-review. Both are at `services/order/internal/consumer/handlers.go:42-50` (registry) and `:112-150` (updateState). The terminal-state guard is on lines 138 and 143: `WHERE id = $2 AND state NOT IN ('confirmed', 'cancelled', 'failed')`. **Do not modify `updateState`** — it already supports the new handler.

- [ ] **Step 3.2: Add `PaymentCompleted` to the registry**

In `services/order/internal/consumer/handlers.go`, modify the `Registry()` method (lines 42-50) to add the new handler:

```go
func (h *Handler) Registry() pkgconsumer.HandlerRegistry {
    return pkgconsumer.HandlerRegistry{
        "StockReserved":           h.StockReserved,
        "StockReservationFailed":  h.StockReservationFailed,
        "OrderConfirmed":          h.OrderConfirmed,
        "OrderCancelled":          h.OrderCancelled,
        "PaymentCompleted":        h.PaymentCompleted, // <- NEW: see method doc for rationale
        "PaymentFailed":           h.PaymentFailed,
    }
}
```

The position of `PaymentCompleted` in the map is alphabetical and matches the order of the existing handlers' declarations (alphabetical: Order, Payment, Stock).

- [ ] **Step 3.3: Add the `PaymentCompleted` handler method**

In `services/order/internal/consumer/handlers.go`, add the new method after `OrderCancelled` (after line 98, before `PaymentFailed` at line 102). Maintain alphabetical ordering of method declarations:

```go
// PaymentCompleted handles PaymentCompleted events by transitioning
// the referenced order to the confirmed state. Independent path from
// the saga's OrderConfirmed emit — covers the cross-topic race where
// PaymentCompleted arrives before the saga's OrderCreatedHandler has
// committed the order_sagas row, which causes the saga's
// PaymentCompletedHandler to silently skip with ErrNotFound. Idempotent
// via updateState's terminal-state WHERE clause; the saga's eventual
// OrderConfirmed becomes a no-op UPDATE.
func (h *Handler) PaymentCompleted(ctx context.Context, env *events.Envelope) error {
    var p struct {
        OrderID string `json:"order_id"`
    }
    if err := json.Unmarshal(env.Payload, &p); err != nil {
        return err
    }
    return h.updateState(ctx, p.OrderID, domain.OrderState("confirmed"))
}
```

All required imports (`context`, `encoding/json`, `events`, `domain`) are already in the file.

- [ ] **Step 3.4: Build the order service to confirm compile**

```bash
cd "C:\Users\t0p_m\projects\orderflow\services\order"
go build ./...
```

Expected: exit 0. The handler registry is type-checked at compile time, so a missing method or wrong signature would fail here.

- [ ] **Step 3.5: Commit**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
git add services/order/internal/consumer/handlers.go
git commit -m "fix(order): consumer handles PaymentCompleted directly to close saga race"
```

---

## Task 4: Extend registry-shape test

**Files:**
- Modify: `services/order/internal/consumer/handlers_test.go`

**Why:** The existing `TestRegistry_HasAllEventTypes` pins the registry shape. Adding `PaymentCompleted` to the `want` slice locks in that the handler must remain registered (otherwise the bug returns silently — `pkg/consumer` would just `ack-and-skip` an unregistered event).

- [ ] **Step 4.1: Read the existing test**

Already done during plan self-review. The test is at `services/order/internal/consumer/handlers_test.go:18-32`.

- [ ] **Step 4.2: Add `PaymentCompleted` to the `want` slice**

In `services/order/internal/consumer/handlers_test.go`, modify `TestRegistry_HasAllEventTypes` to add the new event type. The list must remain alphabetical to match existing convention:

```go
func TestRegistry_HasAllEventTypes(t *testing.T) {
    r := NewHandler(nil, slog.Default()).Registry()
    want := []string{
        "OrderCancelled",
        "OrderConfirmed",
        "PaymentCompleted",
        "PaymentFailed",
        "StockReserved",
        "StockReservationFailed",
    }
    for _, ev := range want {
        if _, ok := r[ev]; !ok {
            t.Errorf("Order Service handler for %q is missing", ev)
        }
    }
}
```

Note: the existing test uses an unsorted `want` slice. The brief mandates keeping it alphabetical (same as the registry). Update the whole slice to alphabetical order at the same time.

- [ ] **Step 4.3: Verify the existing nil-pool test still passes**

The existing `TestRegistry_HandlersReturnErrorOnNilPool` iterates the full registry, so the new `PaymentCompleted` handler is automatically exercised (its `updateState` call returns the expected `pool not initialized` error). No code change needed there. Confirm:

```bash
cd "C:\Users\t0p_m\projects\orderflow\services\order"
go test ./internal/consumer/... -v
```

Expected:

```
=== RUN   TestRegistry_HasAllEventTypes
--- PASS: TestRegistry_HasAllEventTypes (0.00s)
=== RUN   TestRegistry_HandlersReturnErrorOnNilPool
--- PASS: TestRegistry_HandlersReturnErrorOnNilPool (0.00s)
=== RUN   TestStart_DisabledWhenNoEnv
--- PASS: TestStart_DisabledWhenNoEnv (0.00s)
=== RUN   TestStart_InvalidBrokerFails
--- PASS: TestStart_InvalidBrokerFails (0.00s)
PASS
ok  	github.com/t0pm1x/orderflow/services/order/internal/consumer	0.xxx
```

- [ ] **Step 4.4: Commit**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
git add services/order/internal/consumer/handlers_test.go
git commit -m "test(order): pin PaymentCompleted in consumer registry shape"
```

---

# STAGE 3 — Build + verify

## Task 5: Full build + test sweep

**Files:** (none — verification only)

**Why:** Catches any cross-service regression and confirms both fixes are green together.

- [ ] **Step 5.1: Build all binaries**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
make build
```

Expected: 5 binaries produced in `bin/`: `order.exe`, `payment.exe`, `inventory.exe`, `saga.exe`, `web.exe`. The build embeds the SPA bundle (no SvelteKit rebuild needed — frontend is unchanged).

If `make build` errors out due to the GnuWin32 multi-line shell-loop bug noted in the dashboard plan's Task 11 report, work around with direct `go build` invocations:

```bash
cd "C:\Users\t0p_m\projects\orderflow"
set GOTOOLCHAIN=auto
go build -ldflags="-X github.com/t0pm1x/orderflow/services/web/internal/web.Version=v1.1.5-bugfix" -o bin/order.exe    ./cmd/order
go build -ldflags="-X github.com/t0pm1x/orderflow/services/web/internal/web.Version=v1.1.5-bugfix" -o bin/payment.exe  ./cmd/payment
go build -ldflags="-X github.com/t0pm1x/orderflow/services/web/internal/web.Version=v1.1.5-bugfix" -o bin/inventory.exe ./cmd/inventory
go build -ldflags="-X github.com/t0pm1x/orderflow/services/web/internal/web.Version=v1.1.5-bugfix" -o bin/saga.exe     ./cmd/saga
go build -ldflags="-X github.com/t0pm1x/orderflow/services/web/internal/web.Version=v1.1.5-bugfix" -o bin/web.exe      ./cmd/web
```

- [ ] **Step 5.2: Run all tests**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
set GOTOOLCHAIN=auto
go test ./...
```

Or, if `make test` works (it doesn't on this host per dashboard Task 11 report), use `make test`. Expected: all 15 modules pass with no regressions.

Specifically verify:
- `services/web/internal/server`: new `TestAPI_SubmitOrder_AutoGenCustomerID_OnEmpty` and `TestAPI_SubmitOrder_PassesThroughCustomerID_WhenProvided` pass; all existing tests (including `TestHealthAll_*`) pass.
- `services/order/internal/consumer`: updated `TestRegistry_HasAllEventTypes` passes (now includes `PaymentCompleted`); all existing tests pass.
- `services/saga`, `services/payment`, `pkg/*`, `cmd/*`, `tests/*`: unchanged, all green.

- [ ] **Step 5.3: Run `go vet` on changed packages**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
go vet ./services/web/... ./services/order/...
```

Expected: clean.

- [ ] **Step 5.4: Run linter on changed packages (if installed)**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
where.exe golangci-lint 2>$null
```

If `golangci-lint` is installed:

```bash
golangci-lint run ./services/web/internal/server/... ./services/order/internal/consumer/...
```

Expected: the changes introduce **zero new lint findings**. Pre-existing lint debt (11 issues across `api.go`, `server.go`, `sse.go`, `spa.go` — all noted in dashboard Task 11 report) remains; that debt is out of scope here.

If `golangci-lint` is NOT installed, skip and note in the report.

- [ ] **Step 5.5: Verify scope — only the 4 expected files modified**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
git status
```

Expected diff (after Tasks 1-4):

```
modified:   services/web/internal/server/api.go          (T1: +12 lines)
modified:   services/web/internal/server/api_test.go     (T2: +60 lines)
modified:   services/order/internal/consumer/handlers.go (T3: +18 lines)
modified:   services/order/internal/consumer/handlers_test.go (T4: +1 line)
```

NO OTHER FILES should be modified by this work. Specifically:
- `services/web/internal/backend/payment.go` — **must remain in its pre-existing F-010 state**. Do not touch.
- `services/web/internal/backend/types.go` — **must remain in its pre-existing F-010 state**. Do not touch.
- `services/web/frontend/dist/index.html` — **must remain unchanged**. No frontend work.
- The pre-existing F-010 modifications to `services/web/internal/server/api.go` (the `OrderID` field additions in `paymentFireReq`) — **must remain in place**. Only the imports block and the new auto-gen block in `SubmitOrder` should be added; nothing else.

- [ ] **Step 5.6: Verify diff stat against origin/main (within scope of this spec)**

```bash
cd "C:\Users\t0p_m\projects\orderflow"
git fetch origin 2>$null
git diff --stat origin/main -- services/web/internal/server/ services/order/internal/consumer/
```

Expected: only the 4 files listed in § File Structure Map. Total ~90 insertions, 0 deletions (the changes are additive; nothing existing is removed or replaced).

- [ ] **Step 5.7: Append the verification report to the workspace (if it exists)**

If `.superpowers/sdd/2026-08-30-order-creation-and-payment-race-fixes/` exists (the controller created it during task setup), append a `task-5-report.md` with the captured outputs. If the directory was deleted between stages, skip this step.

---

## Definition of Done

- `make build` produces all 5 binaries.
- `go test ./...` is green for all 15 modules.
- `go vet ./services/web/... ./services/order/...` is clean.
- `golangci-lint` introduces zero new findings on the changed packages.
- 4 commits land on `main`, each with the prescribed `fix(web)`/`test(web)`/`fix(order)`/`test(order)` prefix.
- Only the 4 files in § File Structure Map are modified. Pre-existing F-010 working-tree changes are untouched.
- `git diff --stat origin/main -- services/web/internal/server/ services/order/internal/consumer/` shows the expected 4 files only.

---

## Out of scope reminders for downstream specs

- **Saga inbox table (Option B from design discussion):** create `pending_payment_events` table; saga's `PaymentCompletedHandler` and `PaymentFailedHandler` write into it on `ErrNotFound`; `OrderCreatedHandler` drains it for the new orderID. This would fix the race at its source instead of routing around it. ~80 LOC + new tests on both ends.
- **Saga TTL sweep regression:** when the saga eventually catches up via TTL, the `OrderCancelled reason="timeout"` may emit AFTER the order consumer's `PaymentCompleted → confirmed` already won. The saga's emit would race against the now-terminal state. Currently, the saga's `EmitOrderCancelled` does not check state before publishing.
- **Per-partition Kafka keys for saga topics:** topic partitioning by `order_id` would let `OrderCreated` and subsequent `PaymentCompleted` for the same order land on the same partition, restoring in-order processing. Architectural change, much larger than a hotfix.
- **Pre-existing F-010 modifications:** the user's in-progress payment-webhook hardening (defensive defaults in BFF `payment.go` / `types.go` / `api.go`'s `FireWebhook`). Deliberately excluded from this spec.