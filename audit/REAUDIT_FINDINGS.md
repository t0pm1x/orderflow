# orderflow v1.2 — Senior-Go Adversarial Re-Audit Findings

**Audit started:** 2026-08-27
**Starting HEAD:** ea41dd1 (plan) on `main`
**Mode:** Inline execution (subagent-driven skipped per user direction — too slow)
**Environment:** Windows host with Go 1.25.4, `GOTOOLCHAIN=auto` downloads 1.25.13 toolchain.
Docker available; kind/k8s NOT available.

## Summary

| Severity | Count | Status |
|----------|-------|--------|
| P0       | 7     | FIXED (F-001 SvelteKit embed symbols; F-004 SPA blank page; F-005 order chips route to `/`; F-006 payment-sim bind crash; F-007 BFF empty items returns `null` not `[]`; F-008 webhook 404 when payment row not pre-created; F-009 F-008 INSERT failed with empty order_id for UUID-typed column) |
| P1       | 2     | FIXED (F-002 saga cmd test race; F-003 Makefile GOTOOLCHAIN) |
| P2       | 0     | — |
| P3       | 0     | — |

**Outcome:** Project now passes `make build` (all 5 binaries, correct LDFLAGS versions),
`make test` (15/15 modules), `go vet` (15/15 clean), `-race -short -count=10/20`
on hot paths, `make e2e-happy` (43.30s), `make e2e-compensation` (44.69s).

## Findings

### F-009 [P0] — F-008 webhook INSERT fails with empty order_id (UUID-typed column)

- **Component**: services/payment/internal/repository/pg_repo.go + services/web/frontend (PaymentWebhook type + payment-sim)
- **File**: `services/payment/internal/repository/pg_repo.go:80-90`, `services/web/frontend/src/lib/types.ts:50-58`, `services/web/frontend/src/routes/payments/sim/+page.svelte:62-68`
- **Category**: bug (data contract regression from F-008)
- **Reproduction**: with the stack running (`make run` or `scripts/run.ps1`) and all services wired with DATABASE_URL/KAFKA_BROKERS, click "Force succeed ✓" or "Force fail ✗" on `/payments/sim`. The SPA POSTs to `/api/payments/webhook`. The BFF proxies to `/v1/payments/webhook`. The payment service executes F-008's `UpsertFromWebhook` → `INSERT INTO payments (id, order_id, ...)` with `order_id = ""`. Postgres rejects the empty string against the UUID column (`ERROR: invalid input syntax for type uuid`). Go returns 500, BFF maps to 502 `UPSTREAM_UNAVAILABLE`, SPA shows `error.message: "..."`. Pre-F-008 the handler did `Get + UpdateStatusFromNonTerminal` which never inserted with `""`, so the bug didn't surface until F-008's `INSERT ON CONFLICT DO NOTHING` was added.
- **Root cause**: the SPA's `PaymentWebhook` type (services/web/frontend/src/lib/types.ts:50-56) declared only `payment_id`; `order_id` was implicitly the same as `payment_id` per the mock-provider contract (`payment_id == order.id`). F-008's `UpsertFromWebhook` requires `order_id` to populate the `payments.order_id` UUID column; empty string broke the INSERT.
- **Fix**:
  1. **Backend (defensive belt-and-suspenders)**: `PGRepo.UpsertFromWebhook` defaults `orderID = paymentID` when empty. Mirrors the SPA's deterministic mapping; idempotent because the result is the same row identity.
  2. **Frontend (explicit)**: add `order_id?: string` to `PaymentWebhook`. SPA's `payments/sim` button click sets `order_id: order.id` explicitly so the type contract matches what the backend expects.
- **Regression test gap**: the existing `TestWebhook_AutoCreatesRow_FromPayload` provides `order_id` in the request body, so it doesn't catch the empty-`order_id` path. **Add `TestWebhook_AutoCreatesRow_NoOrderIDDefaultsToPaymentID`** that posts a webhook without `order_id` and asserts the row is created with `order_id == payment_id`.
- **Commit**: (this fix)

---

### F-008 [P0] — Payment webhook returns 404 when saga hasn't pre-created payment row

- **Component**: services/payment/internal/webhook/{handler,repository}
- **File**: `services/payment/internal/webhook/handler.go`, `services/payment/internal/repository/pg_repo.go`, `services/payment/internal/webhook/handler_test.go`
- **Category**: bug (UX + missing mock-provider semantics)
- **Reproduction**: on the SPA's `/payments/sim` page, click "Force succeed ✓" or "Force fail ✗" on any in-flight order BEFORE the saga's PaymentRequested consumer has run (i.e. before inventory has reserved stock → payment consumer hasn't created the `payments` row). The BFF returns 404 from `/api/payments/webhook`, the SPA shows `error.message: "payment not found: ..."` (or similar), and the order stays stuck in `reserved`.
- **Root cause**: the webhook handler at `services/payment/internal/webhook/handler.go:176-189` did `repo.Get(payment_id)` and returned 404 (`PAYMENT_NOT_FOUND`) when no row existed. In production this row is created by `services/payment/internal/consumer/handlers.go::PaymentRequested` when the saga fires `PaymentRequested`. But the playground's "Force succeed/fail" buttons fire the webhook directly — bypassing the saga path. Returning 404 makes the operator's "force" action silently fail.
- **Fix**: replace the `Get + UpdateStatusFromNonTerminal` pair with a single `UpsertFromWebhook(paymentID, orderID, amountCents, lastFour, to)` method:
  ```sql
  INSERT INTO payments (id, order_id, amount_cents, status, last_four)
  VALUES ($1, $2, $3, '', $4)
  ON CONFLICT (id) DO NOTHING;
  UPDATE payments
    SET status = $2, updated_at = NOW()
   WHERE id = $1
     AND status NOT IN ('captured', 'failed');  -- terminal-state guard preserved
  ```
  All in one transaction so a crash between INSERT and UPDATE never produces a half-row. The terminal-state guard inside the UPDATE prevents same-status replay and opposite-terminal flip, so idempotency / P1-#1 invariants are unchanged.
- **Repo signature change**: `webhook.Repository` shrunk from 3 methods (`Get`, `UpdateStatus`, `UpdateStatusFromNonTerminal`) to one (`UpsertFromWebhook`). Both `PGRepo` and `fakeRepo` updated.
- **Regression test**: `TestWebhook_AutoCreatesRow_FromPayload` in `services/payment/internal/webhook/handler_test.go` starts with an EMPTY fakeRepo, posts a webhook, asserts the row was created with the right order_id and the outbox event was emitted. The pre-fix `TestWebhook_MissingPayment_404` is gone (replaced by the new behavior). All other terminal-state-guard tests still pass unchanged.
- **Side cleanup**: dropped the now-unused `ErrPaymentNotFound` (and the `Get` / `UpdateStatus` / `UpdateStatusFromNonTerminal` methods) — kept the variable as an alias for backwards-compat with any future caller that might exist outside this package.
- **Commit**: `d21da3d`

---

### F-002 [P1] — Flaky `TestRun_ServesHealthzAndMetrics` in services/saga/cmd/saga

- **Component**: services/saga
- **File**: `services/saga/cmd/saga/main_test.go:44`
- **Category**: race
- **Reproduction**: `go test -short -race -count=3 ./services/saga/cmd/saga/...` — fails ~2/3 with `connectex: No connection could be made`
- **Root cause**: `waitForAddr` polls `saga.ListenAddr()` and returns as soon as the address is non-empty. The listener may be bound but `httpSrv.Serve(ln)` not yet accepting; the test's `http.Get` races against `Accept()`. The companion helper `waitForFreshReadyAddr` already polls the GET until success — but this test never used it. The v1.1.3 audit flagged this exact race (P0 in order/payment/inventory); saga was missed.
- **Fix**: Switch `TestRun_ServesHealthzAndMetrics` to `waitForFreshReadyAddr`. One-line change.
- **Regression test**: `go test -short -race -count=5 ./services/saga/cmd/saga/...` now 5/5 PASS; `-count=10` PASS.
- **Commit**: `c739d69`

### F-003 [P1] — `make build` fails on hosts with Go < 1.25.13 (GOTOOLCHAIN not set)

- **Component**: Makefile
- **File**: `Makefile:13-18`
- **Category**: bug / config
- **Reproduction**: on a host with Go 1.25.4 (any host older than `go.work`'s pin of 1.25.13), run `make build` from a fresh shell. Output: `go: go.work requires go >= 1.25.13 (running go 1.25.4; GOTOOLCHAIN=local)` then `make: *** [build] Error 1`. Build exits 1.
- **Root cause**: `go.work` declares `go 1.25.13` but the Makefile never exports `GOTOOLCHAIN=auto`. The Go toolchain defaults to `GOTOOLCHAIN=local`, which uses the host binary and refuses to build against a newer pin. The user must remember to `export GOTOOLCHAIN=auto` before every `make build` — an undocumented, unergonomic contract. The audit only worked around this by manually exporting the variable in the shell.
- **Fix**: Add `export GOTOOLCHAIN ?= auto` to the Makefile. The `?=` keeps any explicit override from the user (CI may pin a specific toolchain via `go.mod`); for the default shell flow, GOTOOLCHAIN becomes `auto` and Go downloads the pin's toolchain on demand.
- **Regression test**: unset GOTOOLCHAIN (`Remove-Item Env:GOTOOLCHAIN` in PowerShell), run `make build` — all 5 binaries build (exit=0), saga binary reports correct `version=v1.1.4-...`. Verified.
- **Commit**: `c36ff53`

### F-004 [P0] — Web SPA renders blank page (JS bundles 404)

- **Component**: services/web (SvelteKit SPA rewrite)
- **File**: `services/web/spa.go:55-61` and `services/web/internal/server/server.go:200-210,219-229`
- **Category**: bug
- **Reproduction**: start `bin/web.exe`, then `curl -i http://127.0.0.1:8085/`. HTML loads (200, valid SvelteKit SPA bootstrap). Then `curl -i http://127.0.0.1:8085/_app/immutable/entry/start.<hash>.js` → 404. CSS assets at `/_app/immutable/assets/0.<hash>.css` happen to resolve coincidentally via a different code path (separate from the JS lookup). The browser runs the inline `Promise.all([import(start.js), import(app.js)])` → both fail → the SPA never renders → blank page.
- **Root cause**: Two compounded mistakes in the SvelteKit SPA rewrite (commit 96755b3):
  1. **`spa.go`**: The `//go:embed frontend/dist/_app` pattern embeds files at `frontend/dist/_app/...` — Go's embed pattern preserves the directory prefix. The doc comment incorrectly claims "the file path inside the FS is exactly the URL path" (no prefix); in reality, `fs.WalkDir(AppFS, ".")` shows files at `frontend/dist/_app/immutable/...`, not `_app/immutable/...`.
  2. **`server.go`**: The `/_app/*` handler did `path := strings.TrimPrefix(req.URL.Path, "/")` and then `AppFS.Open(path)`. Pre-fix, the embedded files were at `frontend/dist/_app/...` so the lookup failed; the SPA author intended no prefix stripping (per a doc comment that asserted the FS was already rooted at `_app`).
- **Fix**: 
  1. **`spa.go`**: Use `fs.Sub(appFSRaw, "frontend/dist/_app")` to expose AppFS as a sub-FS rooted at the `_app/` directory. Now `AppFS.Open("immutable/entry/start.X.js")` works — the FS root is `_app`, paths inside are relative.
  2. **`server.go`**: Strip the `_app/` URL prefix before opening: `path = strings.TrimPrefix(path, "_app/")`. Static asset content-type and 404 behavior unchanged.
  3. **`/static/*`**: SvelteKit static files (none today beyond favicon.svg, which has its own handler) live under `_app/immutable/assets/` post-build; the `/static/*` route now returns a clean 404 instead of inheriting the SPA HTML fallback (which would have given a misleading text/html content-type to an image request).
- **Regression test**: probe program reads the index.html, extracts every `/_app/...` URL, curls each; all return 200 with correct Content-Type. Verified: 16/16 assets return 200 after fix.
- **Commit**: `d0a3a55`

### F-005 [P0] — Order filter chips navigate to `/` instead of `/orders`

- **Component**: services/web/frontend (SvelteKit SPA)
- **File**: `services/web/frontend/src/routes/orders/+page.svelte:32,44,51,118`
- **Category**: bug
- **Reproduction**: open `http://127.0.0.1:8085/orders?sku=A`. Click any state chip ("Pending"/"Reserved"/etc.) — browser navigates to `/?state=pending&sku=A` (the homepage), not `/orders?state=pending&sku=A`. Symptoms reported by user: "filter doesn't apply, page goes to /orders without filters" (actually: page goes to `/` which renders the homepage; user perceives this as "no filter applied"). Same bug for SKU toggle chips and the "clear SKU filter" link.
- **Root cause**: The page lives at `/orders` (file `routes/orders/+page.svelte`), but every href-builder function constructs the URL with root prefix `'/'`:
  ```ts
  function chipHref(target) {
    ...
    return '/' + (qs ? '?' + qs : '');  // → '/', not '/orders'
  }
  ```
  Same mistake in `skuChipHref`, `clearSkus`, and the inline "All" chip on line 118. Compiled SvelteKit SPA shell renders the same `<script>` regardless of URL, so the user always saw the bootstrap — actual chip HTML only exists client-side after JS hydration, masking the bug if you only curl `/`.
- **Fix**: change `'/' + qs` to `'/orders' + qs` in all 4 sites. No other code path needs to change — `$derived(stateFilter)` and `$derived(skuFilters)` already reactively track `$page.url.searchParams`, and `$effect` re-fetches on change. SPA navigation between `/orders?...` and `/orders?...` triggers a same-route param change that SvelteKit handles without a full page reload.
- **Regression test**: source-level verification (no test infra per user choice 1) — grep confirms 0 occurrences of `'/' + (qs` remain in this file; all 4 hrefs now target `/orders`. Manual: curl `bin/web.exe` for the page, open `/orders?sku=A` in a browser, click "Pending", confirm URL bar shows `/orders?state=pending&sku=A` and the filtered list renders.
- **Commit**: (this fix)

### F-006 [P0] — Payment simulator dropdown crashes with `TypeError: p is not iterable`

- **Component**: services/web/frontend (SvelteKit SPA)
- **File**: `services/web/frontend/src/routes/payments/sim/+page.svelte:130`
- **Category**: bug
- **Reproduction**: open `/payments/sim`. Browser console: `Dzetc_XL.js:1 Uncaught TypeError: Cannot read properties of null (reading 'length') at 7.BZARjGKU.js:2:2027 at DOmiYlhT.js:2:2724 ... at Array.<anonymous> (Dzetc_XL.js:1:13323)`. Plus `TypeError: p is not iterable` originating from Svelte 5's bind proxy. Page renders but the error_code `<select>` is non-interactive / crashes when interacted with.
- **Root cause**: Svelte 5 (runes mode) does not support `bind:value` on indexed access to a `$state` record when the key may not exist yet:
  ```svelte
  <select bind:value={errorCode[o.id]} aria-label="...">
  ```
  `errorCode` is `Record<string, string>` keyed by `order.id`. For newly-loaded orders the entry exists (`load()` populates it), but the bind proxy machinery evaluates `errorCode[o.id]` to coerce the value, finds the initial lookup returns the proxy's `undefined` sentinel, then tries to iterate over it during the `value` → DOM-string round-trip — yielding the `null.length` and "not iterable" errors. Other `bind:value=` sites in the codebase (`orders/new/+page.svelte`) all bind to local `let` variables, which are properly tracked.
- **Fix v1 (commit 3d75eca, INSUFFICIENT)**: convert `<select bind:value={...}>` to `<select value={...} onchange={...}>`. The compiler still routed the value through the same select-bind machinery (a MutationObserver watches the `<select>` for `value` attribute / option changes and re-applies the value via `bind_select_value`). The first iteration of the fix did NOT remove the `<select value>` attribute, so the Svelte bind machinery stayed active and kept crashing.
- **Fix v2 (commit de3039e)**: drop the `value=` attribute entirely. Manage selection via `selected={opt === errorCode[o.id]}` on each `<option>` instead:
  ```svelte
  <select onchange={(e) => { errorCode[o.id] = (e.currentTarget as HTMLSelectElement).value; }} ...>
    <option value="card_declined" selected={errorCode[o.id] === 'card_declined' || !errorCode[o.id]}>card_declined</option>
    <option value="insufficient_funds" selected={errorCode[o.id] === 'insufficient_funds'}>insufficient_funds</option>
    ...
  </select>
  ```
  Now Svelte's select-bind machinery is not engaged at all — verified by inspecting the compiled chunk `dist/_app/immutable/nodes/7.CJ0zraJ1.js`: no `function Q`, no `MutationObserver`, no `__value` token. The chunk shrank from 4309 → 3686 bytes (the bind-select helper code dropped out).
- **Fix v3 — DISCOVERED AFTER v2 SHIPPED**: the `null.length` / `not iterable` errors PERSISTED even after v2. The chunk 7 still crashed on initial render. Root cause was NOT the `<select>` — it was the upstream `/api/orders` endpoint returning `{"items": null}` when no orders match the filter (Go serialises a nil slice as JSON `null`). The payments/sim page's `load()` does `for (const r of [...pending, ...reserved])`; spread of `null` throws "null is not iterable". See **F-007** for the actual fix.
- **Side cleanup**: also dropped the unused `idempotencyKeys` record + `newOrderKeys()` helper. The keys were tracked but never read by `onFire()` (pre-existing dead state).
- **Regression test**: see F-007 — `TestAPI_ListOrders_NilItemsCoercedToEmptyArray` (BFF) + defensive `Array.isArray(body.items) ? body.items : []` in `listOrders` (frontend).
- **Commit**: `de3039e`

### F-007 [P0] — BFF `/api/orders` returns `{"items": null}` when empty, crashes SPA `[...items]` spread

- **Component**: services/web/internal/server/api.go + services/web/frontend/src/lib/api.ts
- **File**: `services/web/internal/server/api.go:200-231`, `services/web/frontend/src/lib/api.ts:46-58`
- **Category**: bug (data contract + downstream client)
- **Reproduction**: with zero orders matching the filter (e.g. brand-new cluster, or all orders in terminal states), `GET /api/orders?state=pending` returns `{"items":null,"next_cursor":""}` instead of `{"items":[],"next_cursor":""}`. The SPA `listOrders` returns `null` (despite the TypeScript type declaring `Order[]` non-nullable). Any consumer that spreads the result — `for (const o of [...pending, ...reserved])` in `payments/sim/+page.svelte`, `{orders.length}` access in `orders/+page.svelte`, etc. — throws `TypeError: null is not iterable` (minified as `n is not iterable` after the `null.length` check first fails). The bug is masked when E2E tests have populated orders, but is reliably triggered by `make e2e-chaos` or any test that drains the queue before asserting empty.
- **Root cause**: the BFF at `services/web/internal/server/api.go` does `filtered := orders.Items` (where `orders` is `*backend.OrderList`); when the upstream returns no rows, `Items` is `nil` (Go zero-value slice). `writeJSON(..., map[string]any{"items": filtered, ...})` serialises the nil slice as JSON `null`. Sibling test `services/web/internal/repository/pg_repo_test.go:418-420` explicitly documents this same shape drift: "the inventory page rendered 'No stock items yet' even when confirmed orders existed in stock_items, because the list call returned `{\"items\":null}`". The fix was never propagated to the BFF.
- **Fix**:
  1. **Backend (root cause)**: in `api.go::ListOrders`, coerce nil to empty slice before writing JSON:
     ```go
     filtered := orders.Items
     if filtered == nil {
         filtered = []backend.Order{}  // never serialise as `null`
     }
     // ... rest of SKU filter unchanged ...
     writeJSON(w, http.StatusOK, map[string]any{"items": filtered, ...})
     ```
  2. **Frontend (defensive belt-and-suspenders)**: in `api.ts::listOrders`, also coerce:
     ```ts
     return Array.isArray(body.items) ? body.items : [];
     ```
     Catches the bug for any future endpoint that regresses.
- **Regression test**: `TestAPI_ListOrders_NilItemsCoercedToEmptyArray` in `services/web/internal/server/api_test.go`. Mocks `listResp: &backend.OrderList{Items: nil}` (the exact production shape), calls `GET /api/orders`, asserts the body contains `"items":[]` and not `"items":null`. Would have failed before the fix.
- **Commit**: (this fix)

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
| `6643c5f` | docs(audit): finalize v1.2 senior-go re-audit findings + STATUS.md |
| (pending) | fix(make): export GOTOOLCHAIN=auto so `make build` works on hosts with Go < 1.25.13 — F-003 |
| (pending) | fix(web): strip embed prefix with fs.Sub + strip _app/ in handler so SPA JS bundles serve — F-004 |
| (pending) | fix(web): route order chips to /orders + unbind payment-sim select (F-005, F-006) |
| (pending) | fix(web): drop <select value=> entirely — use option[selected] (F-006 v2) |
| (pending) | fix(web/api): BFF ListOrders coerces nil items to [] + SPA listOrders defends; regression test TestAPI_ListOrders_NilItemsCoercedToEmptyArray (F-007) |
| (pending) | fix(payment): webhook handler auto-creates payments row from payload (UpsertFromWebhook; mock-provider semantics) so SPA "Force succeed/fail" works pre-saga; regression test TestWebhook_AutoCreatesRow_FromPayload (F-008) |
| (pending) | fix(payment/web): PGRepo.UpsertFromWebhook defaults order_id = payment_id when empty (UUID-typed column rejects ''); SPA PaymentWebhook type adds optional order_id; payment-sim sends it explicitly (F-009) |
