# orderflow-web Playground — Full Audit & Polish

**Created:** 2026-08-19
**Status:** approved (brainstorming complete)
**Owner:** orderflow platform
**Engagement length:** ~7 working days, dispatched across 8 parallel tracks

## Context

`orderflow` is a distributed order-processing platform: 4 Go microservices (Order / Payment / Inventory / Saga orchestrator) over Kafka (Redpanda), Postgres per service, Redis reservations, OpenTelemetry tracing. Reached v1.0 on 2026-08-17, now at v1.1.5.

The `services/web` playground (Go binary `bin/web`, server-rendered HTML + htmx 2.0.3 + SSE) was added in sub-stages `web.1`–`web.11` (commits `d38cf27`..`a52edef`). Its stated goal (per `docs/superpowers/specs/2026-08-18-orderflow-web-design.md:5`): *"a real playground — open `http://localhost:8085`, create an order, watch it transition `pending → reserved → confirmed`, fire a forced-failure webhook to see compensation in action, and see the underlying Kafka events arrive in the sidebar."*

The 32-phase audit requested by the user surfaces that **the current playground does not satisfy that brief**:
- No saga visualization (per-order timeline, state-machine diagram). Just a flat list of colored event lines in the sidebar.
- Port drift: README says `:8083`; every launcher uses `:8085`.
- Multiple BLOCKER-class issues in the runtime (Kafka log loop, missing smoke script, OTEL exporter misconfigured in demo scripts).
- Backend contract drift that makes the playground display empty timestamps and dead fields.
- Doc-rot across top-level `README.md`, `STATUS.md`, ADR-0001, ADR-0003, events spec.

## Goals

1. **Audit** the `services/web` playground end-to-end across all 32 phases of the brief, with high confidence on the code-reviewable phases (0-2, 5-7, 9-10, 12-14, 17, 21-24, 26-28, 30) and explicit "code-only" caveats for the browser-user-test phases (3, 4, 18-20, 31).
2. **Fix all BLOCKER-tier runtime issues** so the audit itself runs on a clean, instrumented stack.
3. **Fix P0-tier defects** that prevent the playground from satisfying its own brief (vendor htmx, add saga viz, form double-submit, UUID validation, backend field alignment).
4. **Fix P1-tier UX gaps** that materially affect first-time-user comprehension and demo quality (error mapping, responsive design, ARIA, status icons, etc.).
5. **Fix P2-tier polish** (hero card, validation, performance, race fixes, code cleanup).
6. **Fix P3-tier cosmetics + doc-rot** (rebrand, design tokens, ADR-0001/0003, README/STATUS freshness).
7. **Produce a written audit report** at two locations (portfolio + demo discovery), with a final verdict (DEMO READY / DEMO READY WITH MINOR ISSUES / NOT DEMO READY).

## Non-goals (explicitly out of scope)

- Production hardening (auth, RBAC, TLS, rate limit, CSRF tokens, DDoS protection).
- Replacing the existing `docs/demo/demo.sh` CLI demo or the asciinema recording.
- SPA conversion / new frontend toolchain (React/Svelte/Vue, Node.js, npm).
- New E2E tests for the playground (its value is interactive per the design doc; we add unit tests for new handlers only).
- Multi-tenancy / pagination beyond the current 50-order limit.
- Helm chart for the `web` service (compose-only is fine for a playground).
- New Kafka topic or schema-version changes (the wire shape change is "fix backend, not protocol").
- GitOps delivery for `web` (compose only).
- Production k8s manifests for `web`.
- Migrating the playground to use gRPC (ADR-0003 will be updated to reflect REST-only; ADR-0001 will be updated to reflect Postgres-only reservations — see P3.5/P3.6).

## Architecture — current state (audit baseline)

Stack: Go 1.25.13, chi v5.3.1, html/template, htmx 2.0.3 from CDN, embedded CSS, SSE for live events via `pkg/consumer`, in-process `events.Bus` for fan-out, polling via htmx every 1-3 s.

```
services/web/
├── cmd/web/main.go              (1-line wrapper)
├── internal/
│   ├── web/main.go              (signal-aware run loop; HTTP_ADDR default :8083)
│   ├── server/
│   │   ├── server.go            (chi router; /healthz inert, /readyz probes upstreams)
│   │   └── probe.go             (parallel /healthz ping)
│   ├── backend/                 (typed HTTP clients → :8081/:8082/:8083)
│   ├── events/bus.go            (in-proc pub/sub for SSE)
│   ├── kafkatail/tail.go        (consumer group "orderflow-web" on 3 topics)
│   ├── handlers/
│   │   ├── handlers.go          (Route registry + PageOrdersList + renderFragment)
│   │   └── pages.go             (5 page handlers + 3 action handlers + SSE)
│   ├── templates/               (layout.html + 6 body fragments)
│   └── static/                  (styles.css only)
└── Dockerfile                   (binary baked; EXPOSE 8083 inside container)
```

Audit findings (full table in §"Defects"):

| Severity | Count | Source |
|----------|-------|--------|
| BLOCKER | 4 | runtime evidence in `docs/demo/logs/web*.log` |
| P0 | 10 | spec ↔ impl + contract drift + security |
| P1 | 12 | UX / functionality |
| P2 | 10 | polish |
| P3 | 8 | cosmetic / docs |
| **Total** | **44 fixes** | |

## Defects — full list

### BLOCKER (must fix before audit runs clean)

| ID | Where | Issue | Fix |
|----|-------|-------|-----|
| BL.1 | `services/web/internal/kafkatail/tail.go` + `pkg/consumer` | Kafka tail enters a tight `WARN consumer: poll fetch error topic="" partition=-1 err="client closed"` loop after startup; web.log grew to 511 MB in 62 s. | Trace the consumer close signal; ensure the poll loop exits cleanly when `ctx.Done()` fires or `Stop()` is called; add a single backoff so the WARN is not emitted every iteration. |
| BL.2 | `scripts/` (no such file exists) | `docs/demo/logs/web-smoke*.log` are all 0 bytes — there is no automated smoke script. The README "Smoke recipe" is purely manual. | Add `scripts/smoke-web.ps1` (and `scripts/smoke-web.sh`) that POST `POST /v1/orders`, GET `/orders/{id}`, exercise `/payments/sim/fire`, GET `/inventory`, assert happy path + compensation path + 4xx + 5xx, write results to `tests/logs/smoke-web.log`. |
| BL.3 | `docs/demo/demo.sh:55-95`, `scripts/run-demo.ps1`, `scripts/run-demo-manual.ps1` | Do not set `OTEL_EXPORTER=stdout`. Binaries default to `otlp` and try `otel-collector:4317` (in-network DNS) which is unresolvable from host. Every 15s: `traces export: no such host`. | Set `OTEL_EXPORTER=stdout` for all 5 binaries in those scripts (matches what `run.ps1:148` / `run.sh:162` already do). |
| BL.4 | `services/web/internal/kafkatail/tail.go:55` | Subscribes to event type `OrderUpdated` which **no service ever publishes**. Dead subscription, wastes a Kafka consumer handler slot. | Remove `OrderUpdated` from the handler registry. |

### P0 (must fix for the playground to satisfy its brief)

| ID | Where | Issue | Fix |
|----|-------|-------|-----|
| P0.1 | `templates/layout.html:8` + `static/` | htmx 2.0.3 loaded from CDN without SRI hash (design-doc risk #4 explicitly required SRI). Supply-chain risk + offline-unfriendly. | Download `htmx.min.js@2.0.3` + `htmx-sse.min.js@2.0.3` into `services/web/internal/static/vendor/`; serve via existing `/static/*` route via `embed.FS`; remove the CDN `<script>` tag. |
| P0.2 | `internal/events/bus.go` + new handler + new template | **No per-order event timeline**. The user can see "global" events in the sidebar but cannot see what happened to *their* order. | Add a bounded per-aggregate ring buffer (cap 200 events × 8 KB ≈ 1.6 MB) in `events.Bus`. Add `events.History(aggregateID)` API. New `PageOrderEvents` handler returns full layout or `?frag=1` fragment. New `templates/order_events.html` renders a vertical timeline. |
| P0.3 | `templates/order_new.html:6`, `templates/order_detail.html:32`, `templates/payments.html:15,22`, `handlers/pages.go:37-95,149-167,281-304` | No double-submit protection on any form. Rapid clicks / Enter-spam / browser-retry create duplicate orders. | (a) Add `hx-disabled-elt="this"` to every submit button. (b) On order submit, read `Idempotency-Key` header; if absent, generate a per-form-render token (HMAC of form-render-time + session). Return 409 on replay. |
| P0.4 | `handlers/pages.go:42-50, 118, 286`, `backend/order.go:42-44`, `backend/inventory.go:13-14` | No UUID validation on `customer_id`, path `{id}`, `order_id`. No URL escaping on `id`/`sku` interpolation into paths (breaks for `?`/`#`/`%`/`/`). | Add `uuid.Parse` checks (400 with friendly message) on all UUID inputs. Add `url.PathEscape` to all path interpolations in `backend/{order,inventory}.go`. |
| P0.5 | `services/order/internal/repository/pg_repo.go:91-92` | `Order.Get` SELECTs only `id, customer_id, items, state, total_cents`. `CreatedAt`, `UpdatedAt`, `CompletedAt`, `LastFour` are **always** zero/nil in GET responses. Playground's `Order.CreatedAt.Format` renders `0001-01-01 00:00`. | Extend the SELECT to include `created_at, updated_at, completed_at, last_four` (the column exists per `domain/order.go:32-34`). |
| P0.6 | `services/order/internal/api/handler.go:188-191` vs `services/web/internal/backend/types.go:54-57` vs `api/openapi.yaml:290-300` | Backend returns `{"items":[...], "has_more": bool}`. Web expects `{"items":[...], "next_cursor": *string}`. `NextCursor` is always nil; no pagination cursor exists. | Fix backend to return `next_cursor` (the `id` of the last item in `items`). Update web's `OrderList` type to match. Update OpenAPI to match. |
| P0.7 | `services/web/internal/backend/types.go:50` vs `services/order/internal/domain/order.go:20-39` | Web's `Order.FailureReason *string` does not exist upstream. Web reads always get `nil`. | Remove `FailureReason` from web's `Order` type until the order domain actually exposes it. |
| P0.8 | `services/web/internal/kafkatail/tail.go:55` | Subscribes to `OrderUpdated` (dead — see BL.4). | Same as BL.4. |
| P0.9 | `services/web/README.md:11-34` | README says `→ listens on :8083 by default`, all curl examples target `:8083`. Actual launcher puts it on `:8085`. First-time user opens the wrong port. | Rewrite `services/web/README.md` to: (a) document the `:8085` host port as the canonical entry point; (b) explain that bare-binary mode uses `:8083`; (c) document every route + the SSE stream; (d) link to the demo script. |
| P0.10 | All `cmd/*/main.go`, all `scripts/*`, `docker-compose.yml` | Env var naming inconsistency: `KAFKA_BROKER` (4 services) vs `KAFKA_BROKERS` (web). Setting one doesn't enable the other. | Unify on `KAFKA_BROKERS` (CSV). In each of the 4 non-web services, read `KAFKA_BROKERS` and fall back to `KAFKA_BROKER` (back-compat). Update all scripts. |

### P1 (significant UX / functionality gaps)

| ID | Where | Issue | Fix |
|----|-------|-------|-----|
| P1.1 | `handlers/pages.go:76,87-90,156-162,299`, `handlers/handlers.go:95,107`, `handlers/pages.go:140,229,264,317` | Upstream error body / raw transport error message reflected to user; template execute error text leaked. | New `handlers/errors.go` with `mapUpstreamError(err) (userMessage string, status int)`. Map known 4xx codes → friendly copy. Never echo raw body. Log full path server-side. |
| P1.2 | `kafkatail/tail.go:39-44`, `templates/layout.html:28-31` | No UI for "Kafka down" state. When `KAFKA_BROKERS` is empty, the SSE endpoint silently never connects; sidebar just sits empty. | When tail returns `(nil, nil)`, mark `Bus` as "events disabled". Render a persistent banner in the sidebar: "Live events: disconnected (Kafka tail not started)". |
| P1.3 | `static/styles.css:16` | Sidebar grid `1fr 360px` with no media query. At <720px the sidebar overflows / pushes content off-screen. Tables overflow. | Add `@media (max-width: 720px) { .main { grid-template-columns: 1fr; } .sidebar { border-left: 0; border-top: 1px solid var(--border); } }`. Add `overflow-x: auto` to table wrappers. |
| P1.4 | `static/styles.css:28-31`, `handlers/pages.go` + templates | State communicated only via color. Fails for color-blind users. | Inline-SVG icon set for `pending` (clock), `reserved` (lock), `confirmed` (check), `cancelled` (ban), `failed` (x). Rendered in badge alongside color. |
| P1.5 | `templates/layout.html:28-31, 34-50` | No `role="log"` / `aria-live="polite"` on `#events`. No focus-visible styles. | Add `role="log" aria-live="polite" aria-label="Order event stream"` to the `<ul>`. Add `:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }` for interactive elements. |
| P1.6 | (new) `templates/order_events.html` + handlers | Per-order event timeline UI (consumer of P0.2 data). | Render the timeline with each event as a node in state-colored icon + timestamp + event_type + a `<details>`-wrapped JSON payload. Polls at 1s while the order is non-terminal (uses existing `?frag=1` pattern). |
| P1.7 | `templates/payments.html:18,26` | Buttons labeled "force ✓" / "force ✗" — confusing, no tooltips. | Rename to "Force succeed (✓)" / "Force fail (✗ card_declined)" + add `aria-label` + tooltip explaining the wire shape. |
| P1.8 | `templates/order_detail.html` | No visible Refresh button; no humanized timestamps. | Add a "Refresh" button (uses existing `hx-get`). Render timestamps via `timeago` template func (e.g. `2s ago`, `5m ago`) plus the absolute time in `title`. |
| P1.9 | `services/web/internal/backend/order.go:74-89` | `Cancel` does not use `do()` — returns raw `fmt.Errorf`. Downstream `errors.As(&HTTPError{})` in `handlers/pages.go:153-163` is **dead code**. | Route `Cancel` through `do()`; return `*HTTPError` on non-204/404; surface 404 vs 502 correctly. |
| P1.10 | `services/web/internal/backend/{order,inventory}.go` | `id`/`sku` interpolated via `fmt.Sprintf` with no URL escaping — `?`, `#`, `%`, `/` break the URL. | Use `url.PathEscape(id)` and `url.PathEscape(sku)` in path interpolation. (P0.4 covers validation; P1.10 covers the escaping itself.) |
| P1.11 | `handlers/pages.go:194-232` | Inventory page does serial N+1 `GetStock` per distinct SKU (up to 50 sequential HTTP calls). | Use `errgroup` with `MaxConcurrency = 8`. Preserve SKU order in output. |
| P1.12 | `handlers/pages.go:42-50, 48-50` | Server-side validation only checks `sku != "" && quantity > 0`. No length cap, no quantity upper bound, no `unit_price_cents` upper bound, no type-coercion error surfacing. | Reject `len(sku) > 64`, `quantity > 10000`, `unit_price_cents < 0` or > 100_000_000; check `strconv.ParseInt` errors and return 400. |

### P2 (polish)

| ID | Where | Issue | Fix |
|----|-------|-------|-----|
| P2.1 | `templates/orders_list.html` (empty state) + new `templates/order_hero.html` | No "what is OrderFlow?" affordance for first-time user. | Replace the current "No orders yet" empty state with a 3-step card: "1. Click + New order • 2. Watch the timeline • 3. Try force ✗ to see compensation". Include a "Create demo order" button that pre-fills the form with `last_four=4242` (happy) or `last_four=0001` (compensation). |
| P2.2 | `templates/layout.html:34-50` (inline JS) + `templates/*` (`hx-trigger="every Ns"`) | Polling has no visibility-based pause. Polls every 1-2s even when tab is backgrounded. | Tiny inline JS: listen to `visibilitychange`; when `document.hidden`, dispatch `htmx:trigger` cancel; when visible, trigger refetch. Apply to orders list, order detail, inventory, payments sim. |
| P2.3 | `handlers/pages.go` | Duplicated `ExecuteTemplate(w, "layout", vm)` + `w.Header().Set("Content-Type", ...)` blocks in every handler. ~30 lines of boilerplate. | Extract `renderPage(w, vm)` + `renderPageFrag(w, name, vm)` helpers in `handlers/handlers.go`. |
| P2.4 | `internal/events/bus.go:56-77` | `Publish` holds the mutex during the entire subscriber fan-out (slow subscriber blocks all publishers). Drop-oldest has a subtle race. | Build the subscriber snapshot under the mutex, then send without holding it. Move the "drop oldest" sequence to a per-channel helper that doesn't double-select. |
| P2.5 | `handlers/pages.go:149-167` | `ActionOrderCancel` `errors.As(err, &he)` is dead code today (depends on P1.9). | After P1.9 lands, this naturally works; just delete the misleading dead branch. |
| P2.6 | `handlers/pages.go:249-267` | `PagePaymentsSim` silently discards partial errors; `BackendDown` only set when both list queries fail. | Capture each list error separately; if either is non-nil, surface it. Distinguish "order service down" from "empty list". |
| P2.7 | `handlers/pages.go:117-142` | `Order.Get` failure returns 404 regardless of cause (transport vs 404). | Branch on `*HTTPError` (after P1.9). Surface 404 only when `he.Status == 404`; otherwise 502. |
| P2.8 | `server/server.go:89-102` | `static.FS.ReadFile("styles.css")` runs per request. | Read once at server startup, capture in `Server` struct as `[]byte`. |
| P2.9 | `internal/web/main.go:107-112` | `boundAddr.Store(srv.Addr())` runs *after* `Start` returns (i.e., after shutdown). Address is post-shutdown. | Move inside `Start` once `s.srv` is wired. |
| P2.10 | `handlers/pages.go:344-355` | SSE handler: `json.Marshal` failure silently `continue`s; no `id:` line means no replay. | Log marshal failure (server-side). Emit `id: {EventID}\n` line so SSE-aware clients can resume via `Last-Event-ID`. |

### P3 (cosmetic / docs)

| ID | Where | Issue | Fix |
|----|-------|-------|-----|
| P3.1 | `templates/layout.html:11-17` | Brand says "orderflow-web" — technical, not product. | Rebrand to "OrderFlow" with tagline "Distributed order processing — playground". |
| P3.2 | `static/styles.css:1-5` | Status colors only — not color-blind safe. | Design tokens file: define `--status-pending: ... --status-confirmed: ...` with shapes/patterns (stripe, dot, solid) as the secondary signal. |
| P3.3 | `templates/orders_list.html:20`, `templates/order_detail.html:9` | Order IDs are mono-font text — no way to copy. | Click-to-copy via inline JS (`navigator.clipboard.writeText`) + a subtle "✓ copied" toast. |
| P3.4 | `static/` (new) | No saga state-machine diagram. | Two static SVGs in `static/diagrams/`: `saga_happy.svg` (OrderCreated → StockReserveRequested → StockReserved → PaymentRequested → PaymentCompleted → OrderConfirmed), `saga_compensation.svg` (same path → PaymentFailed → StockReleaseRequested + OrderCancelled). Render below the timeline. |
| P3.5 | `docs/adr/0003-rest-vs-grpc.md` | Promises gRPC for service-to-service; no gRPC exists. | Rewrite ADR-0003 to reflect REST-only architecture; record the deferred-to-v1.2+ decision; explain why (faster path to v1.0; gRPC infra not yet needed for playground scale). |
| P3.6 | `docs/adr/0001-saga-vs-choreography.md:13-14` | Claims Redis reservations. Reality is Postgres `stock_items.reserved` column + `lock.PGLocker`. | Update ADR-0001 to describe Postgres-backed optimistic-locking reservations + Redis reserved for webhook idempotency only. |
| P3.7 | `README.md:7, 142-144, 162-168, 180, 197-202` | Doc-rot: "Status: v1.0.0", "Migrations: goose (planned, not yet wired)", "Local development (planned, not yet wired)", "saga orchestrator (stub)", ADR list missing 0004. | Update top-level `README.md` to v1.1.5 status; correct or remove the stale sections; add ADR-0004 to the ADR list. |
| P3.8 | `STATUS.md` | Last updated 2026-08-17 (post v0.2.0). Missing v1.0.* through v1.1.5 + web.1..web.11 sub-stages. | Append all sub-stages from v1.0 through v1.1.5; mark `done` per git log; update the "Deferred to v1.2+" section. |

## Execution plan — parallel tracks

8 tracks; each track = 1 parallel agent.

| Track | Theme | Fix IDs |
|-------|-------|---------|
| **α** | `services/web/internal/handlers` + `events` Go logic | BL.1, P1.1, P1.2, P1.11, P1.12, P2.3, P2.5, P2.6, P2.7, P2.10 |
| **β** | `services/web/internal/kafkatail` (Kafka tail + ring buffer) | BL.4, P0.2, P0.8 |
| **γ** | `services/web/internal/static/styles.css` + `templates/*.html` (CSS + template markup only) | P1.3, P1.4, P1.7, P1.8, P2.1, P3.1, P3.2, P3.3, P3.4 |
| **δ** | `services/web/internal/templates/layout.html` (JS + vendored htmx) | P0.1, P1.5, P1.6, P2.2 |
| **5** | `services/order/` + `services/web/internal/backend/` | P0.5, P0.6, P0.7, P1.9, P1.10 |
| **6** | `services/web/README.md` | P0.9 |
| **7** | scripts, Makefile, docker-compose | BL.2, BL.3, P0.10, P2.8, P2.9 |
| **8** | top-level docs (README/STATUS/ADR) | P3.5, P3.6, P3.7, P3.8 |

Stage sequence (each stage runs after the previous verifies clean):

```
Stage 1 — BLOCKERs (4 fixes, 1 track, ~½ day)
Stage 2 — P0 (10 fixes, tracks 1-7, 1-2 days)
Stage 3 — P1 (12 fixes, tracks α/β/γ/δ/5, 1-2 days)
Stage 4 — P2 (10 fixes, all tracks, 1 day)
Stage 5 — P3 (8 fixes, tracks γ/6/8, ½ day)
Stage 6 — Final E2E re-run + written report
```

Merge strategy: each track lands as 1 atomic PR; orchestrator (operator agent) sequences merges and runs `go test ./...` + `make build` after each stage.

## Testing strategy

- **Unit tests for new handlers** (P0.2 timeline, P0.3 idempotency, P1.1 error mapping, P1.11 concurrent fanout, P1.12 validation): added in the same change as the handler.
- **Extend `internal/events/bus_test.go`** with ring-buffer tests (P0.2), concurrent-publish stress (P2.4).
- **Extend `internal/backend/*_test.go`** with: URL-escape cases (P1.10); `Cancel` going through `do()` (P1.9); new error-mapping helper (P1.1).
- **No new E2E tests** for the playground itself (per design-doc non-goal).
- **Smoke script** (`scripts/smoke-web.ps1` / `.sh` from BL.2) is the post-merge regression net — it asserts happy path + compensation path + 4xx + 5xx via curl.

## Verification matrix (final E2E re-run)

| Scenario | How verified | Expected |
|----------|--------------|----------|
| Happy path | POST `/v1/orders` with default form → poll `/orders/{id}` | state reaches `confirmed` within 10 s |
| Failure path | POST `/v1/orders` with `last_four=0001` → force ✗ on payments sim | state reaches `cancelled` |
| Refresh | During saga, reload page → poll picks up state | state transitions visible |
| Network failure | Stop order service → POST `/v1/orders` | 502 with banner + retry button |
| Duplicate submit | Rapid `Invoke-WebRequest` × 5 | exactly 1 order (idempotency-key dedupe) |
| Responsive | curl headers + visual inspection at 3 viewports (code review of media query) | layout reflows correctly |
| Accessibility | Code review of ARIA / focus-visible / role attributes | meets WCAG AA |
| Console | inspect web.log after all scenarios | no unexpected errors |
| Network | count polling requests over 60 s | matches htmx `every` cadence; no leaks |
| Performance | measure page render time + concurrent inventory fetch | inventory < 500 ms with 20 SKUs |

Final verdict from the three options: `DEMO READY` / `DEMO READY WITH MINOR ISSUES` / `NOT DEMO READY`.

## Honest limitations (will be stated in the report)

- **No real-browser user testing** in this environment (no Playwright). Phases 3 (first impression), 4 (visual), 18 (responsive), 19 (keyboard), 20 (browser compat), 31 (final visual) are **code-reviewed**, not user-tested. The report flags each.
- **Audit host is Windows**: SSE reconnect behavior under Linux network stack may differ.
- **No browser-side smoke**: the `scripts/smoke-web.ps1` script asserts HTTP-level correctness only. A user opening the page in a browser may still surface visual bugs not visible from the HTTP wire.
- **44 fixes touch many files**: merge conflicts in concurrent tracks are likely; orchestrator (operator) sequences merges and re-runs `make build` + `go test ./...` after each stage.

## Spec self-review (per brainstorming skill checklist)

| Check | Status |
|-------|--------|
| **Placeholders** ("TBD"/"TODO"/incomplete) | None. Every fix ID has a Where/Issue/Fix. |
| **Internal consistency** | Severity ladder is monotonic; tracks don't overlap (Track α does not touch files Track β modifies); dependency P1.6→P0.2 is recorded. |
| **Scope check** | 44 fixes + report; fits one implementation plan. May decompose into per-stage plans. |
| **Ambiguity check** | Each fix names a specific file or new file; one fix = one mergeable change. |

## Open questions deferred to writing-plans skill

- Exact `Idempotency-Key` HMAC scheme for P0.3 — need a stable per-form-render secret.
- SVG diagram style for P3.4 — match the dark theme or invert for embedding.
- Whether `services/web/README.md` (P0.9) becomes the canonical web docs or `docs/demo/PLAYGROUND.md` does.
- Whether to bump the README "Status" line to `v1.1.5` (P3.7) or a new `v1.2.0` reflecting the audit work.
