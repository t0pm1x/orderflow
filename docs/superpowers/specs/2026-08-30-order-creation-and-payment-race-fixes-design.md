# orderflow — Bug 1 + Bug 2 Fixes (Spec #2 of dashboard UX passes)

**Created:** 2026-08-30
**Status:** approved (brainstorming complete; "Делаем")
**Project:** orderflow (`C:\Users\t0p_m\projects\orderflow`)
**Sequence:** Hotfix spec — broken out from the UX-pass sequence. Bug 1 (order creation) and Bug 2 (pending order stuck) are both blockers for the dashboard's recent-orders table to populate, and for the playground's happy-path demo to work.

## 1. Context

User reported two related symptoms after the dashboard UX pass landed:

1. **New order not created** — clicking "+ New order" in `/orders/new` produces a 400 error and the order never lands in the DB.
2. **Pending order stuck** — after manually getting an order into `pending` state (or after F-009/F-010 fixed the webhook path), the Force-succeed button on `/payments/sim` does not transition the order to `confirmed`.

Phase 1 root-cause investigation (read-only explore via the codebase + `services/order/`, `services/saga/`, `services/payment/`, `services/web/` consumer wiring) found two **independent** bugs:

### Bug 1 — `customer_id` auto-generation lost in SvelteKit rewrite

- **Where:** `services/web/internal/server/api.go` `SubmitOrder` handler.
- **Symptom:** SPA `/orders/new` form sends `customer_id: undefined` when the field is blank. BFF accepts empty `customer_id` ("leave blank for auto-generation") but never generates one — sends `&""` to the Order service. Order service rejects with `400 VALIDATION "customer_id and items required"` (`services/order/internal/api/handler.go:117-124`). DB schema requires `customer_id UUID NOT NULL` (`services/order/migrations/0001_init.sql:13`).
- **Root cause:** The pre-SvelteKit htmx handler at `services/web/internal/handlers/pages.go` (deleted by commit `96755b3`) had explicit `uuid.NewString()` auto-generation. The placeholder text "leave blank for auto-generated UUID" survived in the SPA, but the implementation was lost in the rewrite.
- **Pre-existing precedent for fix location:** F-008 fixed "webhook auto-creates payments row from payload" in the **payment service** (domain invariant); F-009 and F-010 added defensive defaults in the **BFF** (UX promise). The SPA's "leave blank" promise is a UX concern → BFF owns it → fix in BFF.

### Bug 2 — Saga race + silent skip for `PaymentCompleted`

- **Where:** Missing handler in `services/order/internal/consumer/handlers.go` registry, combined with saga consumer's silent `ErrNotFound` skip.
- **Symptom:** When user fires Force-succeed webhook within seconds of submitting the order, the order remains `pending` indefinitely (until saga's 5-minute TTL sweep eventually cancels it).
- **Root cause chain:**
  1. Saga subscribes to `order-events`, `inventory-events`, `payment-events` in a single consumer group — no cross-topic ordering guarantee.
  2. `OrderCreatedHandler` (`services/saga/internal/consumer/handlers.go:123-212`) inserts the `order_sagas` row in `StateInitiated`.
  3. If `PaymentCompleted` is delivered BEFORE the saga row is committed (Kafka cross-topic race), `PaymentCompletedHandler` calls `TransitionStateTx(orderID, ...)` → `repository.ErrNotFound` → logs `"PaymentCompleted for unknown saga"` → returns `nil`. **Event is offset-committed and silently dropped, never replayed.**
  4. `OrderCreatedHandler` does not retroactively check for already-delivered `PaymentCompleted`.
  5. **Asymmetry:** `PaymentFailed` works even when the saga row is missing because the **order consumer** has a direct `PaymentFailed → cancelled` handler (`handlers.go:102-110`). `PaymentCompleted` lacks this direct path and depends entirely on saga-emitted `OrderConfirmed`.
- **Reproduction:** 100% reproducible with the playground's existing F-009/F-010 SPA + BFF.

This spec fixes **both bugs** in one tightly-scoped patch. The saga's silent-skip behavior is left as-is (a follow-up "saga inbox" fix is out of scope here — see § 14).

## 2. Goals

1. **Submitting an order with empty `customer_id`** via `/orders/new` succeeds; the BFF auto-generates a UUID and the Order service stores the order.
2. **Force-succeed webhook** on a `pending` order transitions it to `confirmed` within the polling window (~2s on `/orders/[id]`).
3. **No regressions** in the happy path: orders still go through full saga (`pending → reserved → confirmed`) when no race occurs.
4. **No new external Go deps** (use `github.com/google/uuid` already in `go.sum`).
5. **No domain-service edits** (Bug 1 fix in BFF; Bug 2 fix in order consumer's handler registry — surgical, ~15 lines).

## 3. Non-goals

- **Saga silent-skip fix** (Option B from the design discussion: `pending_payment_events` table + drain on `OrderCreatedHandler`). This is a more thorough fix requiring a schema migration. Out of scope here. See § 14.
- **Modifying `services/order/internal/api/handler.go`** (the strict validator). Bug 1 fix is in the BFF per the F-009/F-010 precedent.
- **Touching the pre-existing F-010 working-tree modifications** to `services/web/internal/backend/payment.go`, `services/web/internal/backend/types.go`, or any other handler in `services/web/internal/server/api.go` *except* `SubmitOrder` and `SubmitOrder`'s import block. F-010 is the user's in-progress payment-webhook hardening; this spec does not touch it.
- **Saga ordering fixes** (transactional outbox, per-partition Kafka keys). Out of scope — too large a change for a hotfix.
- **New e2e test for "submit + immediately force-succeed"** race. The bug-2 race is timing-dependent; an e2e test would be flaky. Covered by unit tests + manual smoke (see § 11).
- **Touching any of the dashboard UX-pass files** (`dashboard.ts`, `dashboard/+page.svelte`, etc.). This is a separate concern.

## 4. Approach

**Bug 1 — surgical UUID auto-gen in BFF `SubmitOrder`.**

In `services/web/internal/server/api.go`:
1. Import `github.com/google/uuid`.
2. After the validation chain (after `if req.IdempotencyKey == ""` check) and before constructing `backend.OrderSubmit`, add:
   ```go
   if req.CustomerID == "" {
       req.CustomerID = uuid.NewString()
   }
   ```
3. Add a focused unit test (see § 11).

**Bug 2 — direct `PaymentCompleted → confirmed` path in order consumer.**

In `services/order/internal/consumer/handlers.go`:
1. Add `PaymentCompleted` to the handler registry alongside the existing `PaymentFailed`:
   ```go
   "PaymentCompleted": h.PaymentCompleted,
   "PaymentFailed":    h.PaymentFailed,
   ```
2. Add the `PaymentCompleted` handler method, mirroring `PaymentFailed`:
   ```go
   // PaymentCompleted handles PaymentCompleted events by transitioning
   // the referenced order to the confirmed state. Independent path
   // from the saga's OrderConfirmed — covers the case where the saga
   // row is not yet committed when the PaymentCompleted event arrives
   // (cross-topic race on the saga's 3-topic subscription). Idempotent
   // via updateState's terminal-state guard.
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
3. Extend `TestRegistry_HasAllEventTypes` to include `PaymentCompleted`.
4. `updateState`'s existing `WHERE state NOT IN ('confirmed', 'cancelled', 'failed')` clause already guarantees idempotency — the second call from the saga's eventual `OrderConfirmed` becomes a no-op UPDATE. No new wrapper needed.

**Why Option A (order-consumer direct path) over Option B (saga inbox table):**

- Idempotency: `updateState` already guards terminal states. Direct path is safe to run in parallel with saga.
- ~15 lines total, surgical, no migration.
- Mirrors the asymmetric precedent that already exists for `PaymentFailed` (which has had the direct path since the v1.0 release — works fine).
- The saga's silent-skip behavior remains, but **its visible effect is reduced**: even if the saga misses `PaymentCompleted`, the order consumer now catches it.

## 5. Architecture

```
Browser                          BFF (services/web)              Order Service (services/order)
   |                                  |                                    |
   | POST /api/orders                 |                                    |
   | (customer_id omitted)            |                                    |
   |------------------------------->  |                                    |
   |                                  | [submit] validation passes         |
   |                                  | [submit] customer_id = ""  ←  THIS SPEC
   |                                  |        ↓ uuid.NewString()         |
   |                                  | [submit] constructs OrderSubmit    |
   |                                  | [submit] a.Order.Submit(ctx, ...)  |
   |                                  |------------------------------->    |
   |                                  |                                    | [submit] parseCustomerID(req.CustomerID)
   |                                  |                                    | [submit] domain.NewOrder(...)
   |                                  |                                    | [submit] repo.Create(...)
   |                                  |                                    | [submit] OrderCreated event → Kafka
   |                                  |                                    |
   | 201 Created  Order(JSON)         |                                    |
   |<-------------------------------   |                                    |

(separate flow)

SPA /payments/sim  →  BFF /api/payments/webhook  →  Payment Service  →  Kafka topic "payment-events"
                                                                              |
                                                                              v
                                                              ┌───────────────────────────────┐
                                                              │  Saga consumer (3 topics)     │
                                                              │  PaymentCompletedHandler      │
                                                              │  → may race; row may not yet  │
                                                              │    exist → silent skip        │
                                                              └───────────────────────────────┘
                                                                              |
                                                                              v (parallel)
                                                              ┌───────────────────────────────┐
                                                              │  Order consumer (3 topics)     │  ←  THIS SPEC adds:
                                                              │  PaymentCompletedHandler       │     direct path
                                                              │  → updateState(confirmed)      │
                                                              └───────────────────────────────┘
                                                                              |
                                                                              v
                                                                       orders.state = 'confirmed'
```

## 6. Backend changes

### 6.1 `services/web/internal/server/api.go`

- Add `"github.com/google/uuid"` to the imports block.
- In `SubmitOrder`, after the existing `if req.IdempotencyKey == ""` validation block (around line 318) and before the replay cache check, insert:

  ```go
  // Auto-generate customer_id when SPA sends an empty value. The
  // /orders/new form has placeholder text promising "auto-generated
  // UUID" — that promise was lost in the SvelteKit rewrite
  // (commit 96755b3 deleted the uuid.NewString() call from the
  // pre-existing htmx handler). The Order Service rejects empty
  // customer_id with 400 VALIDATION (services/order/internal/api/
  // handler.go:117-124) because orders.customer_id is NOT NULL UUID.
  // F-009 / F-010 set the precedent that BFF owns the UX promise.
  if req.CustomerID == "" {
      req.CustomerID = uuid.NewString()
  }
  ```

  Place it adjacent to the existing `if req.CustomerID != "" && !isValidUUID(...)` block (around line 323) for code locality — both branches touch the customer_id field.

### 6.2 `services/web/internal/server/api_test.go`

- Extend the existing `fakeOrder` type with a `submitErr error` field already used by tests, OR add a new fake variant that returns a `*backend.HTTPError{Status: 400, Body: "customer_id and items required"}` on the first call and accepts the next call. Cleanest: add a `submitResponseCount` counter to the existing fake so we can assert the call succeeded (call was made and the auto-generated UUID was forwarded).
- Add `TestAPI_SubmitOrder_AutoGenCustomerID_OnEmpty`:
  - Calls `SubmitOrder` with `req.CustomerID = ""`.
  - Asserts: no error returned; `fakeOrder.lastSubmit.CustomerID` is a non-empty valid UUID.
- (Optional, deferred if no time) Add `TestAPI_SubmitOrder_PassesThroughCustomerID_WhenProvided`:
  - Calls `SubmitOrder` with a known `req.CustomerID = "<some UUID>"`.
  - Asserts: no error; `fakeOrder.lastSubmit.CustomerID == "<same UUID>"`.

### 6.3 `services/order/internal/consumer/handlers.go`

- Add `PaymentCompleted` to the `Registry()` method (line 42-50):
  ```go
  func (h *Handler) Registry() pkgconsumer.HandlerRegistry {
      return pkgconsumer.HandlerRegistry{
          "StockReserved":           h.StockReserved,
          "StockReservationFailed":  h.StockReservationFailed,
          "OrderConfirmed":          h.OrderConfirmed,
          "OrderCancelled":          h.OrderCancelled,
          "PaymentCompleted":        h.PaymentCompleted, // <- NEW: see doc comment for why
          "PaymentFailed":           h.PaymentFailed,
      }
  }
  ```
- Add the `PaymentCompleted` handler method after `OrderCancelled` (around line 99), following the existing pattern:

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

  No new wrapper, no new imports (the existing `encoding/json`, `context`, `events`, `domain` imports already cover this).

### 6.4 `services/order/internal/consumer/handlers_test.go`

- Extend `TestRegistry_HasAllEventTypes` to add `"PaymentCompleted"` to the `want` slice. The existing nil-pool test `TestRegistry_HandlersReturnErrorOnNilPool` will automatically exercise the new handler (it iterates `r` after registration) — no change needed there.

That's the entire test surface for Bug 2. Real-DB integration tests for `updateState(confirmed)` idempotency would require testcontainers Postgres; deferred per § 3.

## 7. Data flow

Unchanged from existing. Bug 1 is purely a BFF-side pre-processing step before the existing `a.Order.Submit` flow. Bug 2 is a new handler that fits into the existing Kafka consumer registry with no changes to consumer goroutines, partitioning, or topic subscriptions.

## 8. Error handling

**Bug 1** — if `uuid.NewString()` itself fails (impossible in practice, but Go won't admit it — returns empty string), the request proceeds to `a.Order.Submit` with empty `customer_id` and the Order service returns 400 as before. No new failure mode.

**Bug 2** — the new `PaymentCompleted` handler returns an error from `json.Unmarshal` if the payload is malformed (matching the existing handler pattern). It returns `updateState`'s error on DB failures. The consumer DLQs on returned errors, same as existing handlers.

## 9. Race interaction analysis

After both fixes:

| Scenario | Path 1 (saga) | Path 2 (order consumer) | Final state |
|---|---|---|---|
| `OrderCreated` arrives first, then `PaymentCompleted` | Saga transitions `initiated → stock_reserved`, emits `OrderConfirmed`. Order consumer's `OrderConfirmed → confirmed` succeeds. | Order consumer's new `PaymentCompleted → confirmed` runs first (since `PaymentCompleted` arrives after `OrderCreated`'s emit but before `OrderConfirmed`'s emit). State becomes `confirmed`. | `confirmed`. Saga's later `OrderConfirmed` is no-op (terminal guard). ✓ |
| `PaymentCompleted` arrives first, then `OrderCreated` | Saga finds no row, silent skip. Saga's `OrderCreatedHandler` inserts row at `initiated`. Saga never sees `PaymentCompleted` again (offset committed). | Order consumer's `PaymentCompleted → confirmed` succeeds. State becomes `confirmed`. | `confirmed`. ✓ **Bug 2 fixed.** |
| Both arrive simultaneously | Whichever consumer thread wins; saga may or may not transition. | Order consumer always transitions. | `confirmed`. ✓ |
| `PaymentCompleted` fires twice (duplicate webhook) | n/a | First call: `pending → confirmed`. Second call: UPDATE matches no rows (terminal guard). | `confirmed`. ✓ Idempotent. |
| Happy-path e2e (slow race) | Saga handles everything; saga emits `OrderConfirmed`; order consumer's `OrderConfirmed → confirmed`. | Order consumer's `PaymentCompleted` runs first (since it fires immediately after webhook). State becomes `confirmed`. Saga's later `OrderConfirmed` is no-op. | `confirmed`. ✓ No regression. |

Net effect: `pending → confirmed` always happens within the webhook round-trip latency. No new failure modes.

## 10. Testing

### Backend (Go)

- `cd services/web && go test ./...` — new BFF unit test passes; all existing tests green.
- `cd services/order && go test ./...` — extended registry test passes; nil-pool test exercises new handler; all existing tests green.
- `cd services/saga && go test ./...` — unchanged; saga's silent-skip behavior is out of scope.
- `cd services/payment && go test ./...` — unchanged.
- `cd pkg && go test ./...` — unchanged.
- Full `make test` end-to-end must stay green.

### Frontend

No frontend changes. No new tests required.

### Manual smoke (the 8-point checklist from dashboard spec, extended)

1. **Bug 1 regression:** Open `http://localhost:8085/orders/new`, leave `customer_id` blank, fill 1 SKU + quantity, click Submit. Expect: redirect to `/orders/<id>`, order visible in `/orders` list. Pre-fix this failed with `BAD_REQUEST "The backend rejected the request..."`.

2. **Bug 2 regression (race):** Run `bash scripts/run.sh`, open `/payments/sim`. Create a fresh order. Within ~2 seconds (before saga's `OrderCreatedHandler` commits), click "Force succeed". Pre-fix: order stays `pending` indefinitely. Post-fix: order transitions to `confirmed` within 2s polling window.

3. **Happy-path no-regression:** Create order → wait for full saga (~5s) → all states transition correctly: `pending → reserved → confirmed`. Click "Force fail" instead → order → `cancelled`. Both paths preserve the existing user experience.

4. **Dashboard "Orders today" tile** ticks to 1 after a successful order creation.

## 11. Risks

1. **Bug 2 — duplicate transitions in saga AND order consumer.** The saga may emit `OrderConfirmed` after the order consumer's `PaymentCompleted` already transitioned the order. The terminal-state guard prevents the second UPDATE from changing the row. SQL `UPDATE ... WHERE state NOT IN (...)` returns 0 affected rows for a no-op — no error, no log noise. Verified by reading `updateState` (handlers.go:138).
2. **Bug 1 — UUID generation under high load.** `uuid.NewString()` is process-local and thread-safe (uses crypto/rand). No allocation pressure concern at playground scale.
3. **Bug 1 — message ordering regression.** If the SPA starts sending `customer_id` deliberately as an empty string to test BFF behavior, BFF will silently auto-generate. Acceptable per the placeholder promise.
4. **Bug 2 — silent saga skip remains.** A future change that reorders the saga or removes the `OrderConfirmed` emit would not be caught by this fix. The saga still has the silent-skip behavior; we just route around it. Documented in § 14.

## 12. File-level delta

| File | Change | Lines |
|---|---|---|
| `services/web/internal/server/api.go` | Add `uuid` import + auto-gen block | +12 |
| `services/web/internal/server/api_test.go` | New `TestAPI_SubmitOrder_AutoGenCustomerID_OnEmpty` | +30 |
| `services/order/internal/consumer/handlers.go` | Add `PaymentCompleted` to registry + new method | +18 |
| `services/order/internal/consumer/handlers_test.go` | Extend `want` slice in `TestRegistry_HasAllEventTypes` | +1 |

Total: ~61 lines across 4 files. Net +58 (after removing comments in the original).

## 13. Definition of done

- `make build`, `make test`, `make lint` (golangci-lint) all green.
- New BFF unit test passes; new registry test assertion passes.
- Manual smoke: empty `customer_id` submission succeeds (Bug 1 regression).
- Manual smoke: race-y Force-succeed transitions `pending → confirmed` within 2s (Bug 2 regression).
- Manual smoke: happy-path e2e saga still works end-to-end (no regression).
- Only files in § 12 modified; no domain-service, OpenAPI, infra, or dashboard-UX files touched.
- Commit message style: `fix(web): ...` for Bug 1; `fix(order): ...` for Bug 2.

## 14. Out of scope reminders for downstream specs

- **Saga inbox table (Option B from design discussion):** create `pending_payment_events` table; `PaymentCompletedHandler` and `PaymentFailedHandler` write into it on `ErrNotFound`; `OrderCreatedHandler` drains it for the new orderID. This would actually fix the race at its source instead of routing around it. Schema migration + ~80 LOC + new tests on both ends. Worth doing as a follow-up.
- **Saga TTL sweep regression:** when the saga eventually catches up via TTL, the `OrderCancelled reason="timeout"` may emit AFTER the order consumer's `PaymentCompleted → confirmed` already won. The saga's emit would race against the now-terminal state. Currently, the saga's `EmitOrderCancelled` does not check state before publishing. Out of scope here; could cause "confirmed" orders to be clobbered back to "cancelled" in some edge cases.
- **Per-partition Kafka keys for saga topics:** topic partitioning by `order_id` would let `OrderCreated` and subsequent `PaymentCompleted` for the same order land on the same partition, restoring in-order processing. Architectural change, much larger than a hotfix.
- **Touching pre-existing F-010 modifications** in `services/web/internal/backend/payment.go`, `services/web/internal/backend/types.go`, and the F-010 parts of `services/web/internal/server/api.go`. The user's in-progress F-010 work is orthogonal; we deliberately limit our `api.go` touch to the `SubmitOrder` block + the import.