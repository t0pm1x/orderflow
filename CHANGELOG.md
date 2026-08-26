# Changelog

All notable changes to orderflow will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0-MVP] - 2026-08-17

### Added
- Monorepo bootstrap with `go.work` (1 platform module + 3 service modules + 4 cmd stubs)
- pkg/platform library: slog logging (with OTel trace correlation), chi middleware stack, types (Money, OrderID, etc.), event envelope, typed errors
- Order Service: domain (state machine: pending→reserved→confirmed/cancelled/failed), REST API (POST/GET/LIST) with 5 tests
- Payment Service: mock provider with deterministic success/decline/insufficient-funds/timeout
- Inventory Service: Stock model with optimistic locking version column
- Docker-compose stack: 3 Postgres, Redis, Redpanda (KRaft), OTel Collector, Prometheus, Tempo, Grafana
- K8s base: namespace, RBAC, default-deny + intra-namespace NetworkPolicies
- 3 ADRs: saga-vs-choreography, outbox-pattern, REST-vs-gRPC
- 3-level C4 architecture diagrams (PlantUML)
- OpenAPI 3.0 spec for Order/Payment/Inventory REST APIs
- Domain events spec (11 events + envelope type)
- Makefile with build/test/lint/run/run-<svc>/clean/tidy targets
- GitHub Actions CI (3-OS matrix)

### Known Limitations (MVP scope)
- No database migrations implemented (services have no DB wired)
- No Kafka producers/consumers (events not flowing)
- No outbox pattern implementation (3.7 deferred)
- No saga orchestrator (3.9 deferred)
- HTTP API only for Order Service; Payment and Inventory have REST stubs only
- docker-compose stack defined but services don't connect to it
- No E2E / chaos / load tests
- No Helm charts

## [Unreleased]

### Fixed — web UI: inline script crash + htmx-sse 1.x + filter loss + saga timeline hidden (v1.1.7 part 6)

Six bugs surfaced together when an operator walked through the
playground end-to-end. Five had a single shared root cause (a
JavaScript error in the inline `<script>` that aborted every
listener registration and broke htmx navigation); the sixth was
independent. All fixed in this revision.

- **DEFECT-7: inline script crash on null `document.body`.**
  The listener-registration block was at the top of `<head>` and
  called `document.body.addEventListener(...)` synchronously during
  HTML parsing, when `<body>` doesn't exist yet. The TypeError
  "Cannot read properties of null (reading 'addEventListener')"
  aborted the script before the SSE / aria-busy / copy-id
  listeners could attach, and — more critically — before htmx
  finished wiring its swap target cache. With htmx init partially
  broken, every link / form click appeared to do nothing (manual
  URL entry worked because it bypasses htmx). Fix: wrap every
  body-listener registration in a `DOMContentLoaded` handler.
  New regression test `TestLayout_InlineScript_DOMContentLoaded`
  asserts the wrapper is present and every `document.body.addEventListener`
  call comes AFTER it in the rendered markup.

- **DEFECT-8: htmx-sse.js 1.x incompatible with htmx 2.0.3.**
  The vendored plugin was the htmx-1.x build; it logged
  "WARNING: You are using an htmx 1 extension with htmx 2.0.3"
  on every page load and used a removed internal API
  (`api.selectAndSwap`) that threw inside `swap()`. Replaced with
  the byte-for-byte vendored copy of `htmx-ext-sse@2.2.4` (the
  official 2.x plugin) from the jsDelivr CDN. The wire format and
  attribute names are unchanged from the user's perspective.

- **DEFECT-1: `sse-swap="event"` filter never matched.** The
  sidebar's `<aside>` carried `sse-swap="event"` which makes
  htmx-sse only listen for SSE messages with the literal event
  type `"event"`. The server emits messages with types like
  `OrderCreated`, `OrderConfirmed`, etc., so the listener never
  fired and the live-event list stayed empty. Fix: switch the
  server to emit unnamed messages (browsers default the event
  type to `"message"`) and have the `<ul>` listen for
  `sse-swap="message"`. New regression test
  `TestLayout_AsideSseSwapMessage` + `TestPageEventsStream_EmitsIDLine`
  (updated) pin the wire format.

- **DEFECT-2: `← back to list` lost the active filter.** The
  link was hardcoded `href="/"`, so navigating from a filtered
  orders list (e.g. `?state=reserved&sku=SKU-001`) to an order
  detail and back dropped the filter context. Fix: added
  `BackHref` to `orderDetailVM`, built in `PageOrderDetail` from
  `r.URL.Query()` (`state=` + `sku=...`), and rendered in the
  template. New regression test
  `TestPageOrderDetail_BackHref_PreservesFilter` covers the 5
  query-string combinations.

- **DEFECT-5: `Saga timeline` header vanished on 404.** The
  `<h3>Saga timeline</h3>` was inside the outer `{{if .Order}}`
  block, so when the upstream order fetch returned 404 the entire
  section (including the timeline header) was silently elided.
  Fix: move the header + fallback outside the outer `if`, so the
  404 page now shows `<h3>Saga timeline</h3>` + an explicit
  "Saga timeline unavailable — order not loaded. <error msg>"
  line. The full timeline + saga diagrams still render normally
  when the order is present. New regression test
  `TestPageOrderDetail_OrderNil_SagaTimelineFallback`.

- **DEFECT-6: `hx-swap="afterend"` on the SSE aside.** The
  attribute was a no-op (the aside has no `hx-get` / `hx-post`,
  only `sse-connect`, and `hx-swap` only governs swap styles for
  HTMX requests, not SSE message handling). Removed for clarity
  — the new `htmx-ext-sse` plugin uses `sse-swap` exclusively.

### Added — orderflow-web Helm chart (v1.1.7 part 5)

The web service was added in v1.1 but had no Helm chart and no
kustomize Deployment — operators could run it via
`docker compose` or the bare-binary launcher, but not via
`helm install` / `kubectl apply -k`. New chart at
`deploy/helm/orderflow-web/`:

- `Chart.yaml`, `values.yaml`, `values-dev.yaml`,
  `values-override.yaml.example`.
- `templates/`: `deployment.yaml`, `service.yaml`,
  `configmap.yaml`, `secret.yaml`, `serviceaccount.yaml`,
  `_helpers.tpl`.
- 7 env vars: `HTTP_ADDR`, `OTEL_EXPORTER`,
  `OTEL_EXPORTER_OTLP_ENDPOINT`, `LOG_LEVEL` (ConfigMap);
  `ORDER_URL`, `PAYMENT_URL`, `INVENTORY_URL` (ConfigMap); and
  the optional `KAFKA_BROKERS` (Secret). Defaults match the
  in-cluster DNS of the standard `orderflow-{order,payment,
  inventory}` Services, so a vanilla `helm install` of all
  four backend charts plus the new web chart produces a working
  playground.
- The web binary is stateless (no DB, no Redis, no
  migrations), so the chart is the simplest of the service
  charts: no PodDisruptionBudget, no init container, no
  migration Job. `KAFKA_BROKERS` is optional; leaving it empty
  lets the page render with a "disconnected" SSE sidebar
  instead of crashing on startup.
- Resource requests/limits are sized for a stateless BFF
  (50m/250m CPU, 64Mi/256Mi memory) — much smaller than the
  stateful backend services.
- `startupProbe` budget is 60s (12 × 5s) — shorter than the
  150s budget on the backend services because web has no DB
  pool to warm up.

### Changed — Helm chart + kustomize image tag bump to v1.1.7

The deploy manifests had been frozen on the v0.2.0 release image
tag for every service even though the binary version has moved
through v0.5 → v1.0 → v1.1.x. A `helm template` of the existing
charts would have deployed a stale binary from 18+ months ago.
Bumped:

- `deploy/helm/orderflow-{order,payment,inventory,saga}/values.yaml`:
  `image.tag` from `"0.2.0"` to `"v1.1.7"`.
- `deploy/helm/orderflow-{order,payment,inventory,saga}/Chart.yaml`:
  `version` 0.2.0 → 1.1.7 and `appVersion` "0.2.0" → "v1.1.7".
- `deploy/helm/orderflow-{postgres,redis,redpanda}/Chart.yaml`:
  `version` 0.2.0 → 1.1.7 (matches our deployment); `appVersion`
  preserved (16-alpine / 7-alpine / v24.2.7 are upstream
  versions, not orderflow release versions).
- `deploy/kustomize/base/services.yaml`: image tag on all four
  service Deployments from `:0.2.0` to `:v1.1.7`.

Not changed in this pass (left for v1.1.8):

- `orderflow-web` has no Helm chart and no kustomize Deployment
  yet — it can be deployed via `docker compose` or the bare
  binary launcher (`scripts/run.ps1`), but not Helm / kustomize.
- The `autoscaling:` block in
  `deploy/helm/orderflow-{order,saga}/values.yaml` is dead config:
  no HPA template consumes it. Documented gap, not removed.

### Fixed — order-service List completeness + consumer completed_at (v1.1.7 part 3)

Cross-service audit pass turned up two real bugs in the Order
Service that the BFF had silently compensated for:

- **`Order.List` SELECT omitted `last_four` and `completed_at`**
  (`services/order/internal/repository/pg_repo.go:List`). The
  `Get` path correctly read both columns, but the List path
  scanned only `(id, customer_id, items, state, total_cents,
  created_at, updated_at)`, so orders surfaced via `/v1/orders`
  carried `LastFour=""` and `CompletedAt=nil` on the wire. Visible
  symptoms in the playground:
  - The `/payments/sim` page's hidden `<input name="last_four">`
    was always empty, so the upstream `errorCode()` fallback
    always picked `"network_error"` instead of the card-derived
    reason. The new `<select name="error_code">` (v1.1.7 UI
    additions) compensates for this by making the operator pick
    the code explicitly, but the underlying bug still
    surfaces for any caller that reads `/v1/orders` and
    forwards `last_four` on a follow-up webhook.
  - The order-detail page's "completed {{time}}" line never
    rendered for terminal orders surfaced via the list endpoint
    (only via direct `/v1/orders/{id}` reads).
  Fix: add both columns to the SELECT and the scan list. New
  regression test `TestPGRepo_ListReturnsLastFourAndCompletedAt`
  asserts both fields come back populated. (Skipped when
  `DATABASE_URL` is unset — same skip-on-no-DB contract as the
  other `pg_repo_test.go` tests.)

- **`consumer.updateState` did not set `completed_at` on
  terminal transitions** (`services/order/internal/consumer/handlers.go`).
  When the saga or inventory emitted
  `OrderConfirmed`/`OrderCancelled`/`StockReservationFailed`,
  the consumer's bulk `UPDATE orders SET state=$1, updated_at=NOW()`
  left `completed_at` NULL — visible in the BFF as "the order
  is confirmed but no completion timestamp". The `PGRepo.Cancel`
  path already sets `completed_at`; the consumer path is now
  consistent (state→terminal sets `completed_at=NOW()`; other
  transitions keep the existing `updated_at=NOW()` behaviour).

### Added — hx-boost navigation + sidebar persistence (v1.1.7 part 2)

Closes the "clicking any nav link reloads the entire page" UX
regression reported by an operator walk-through, AND the
"sidebar reconnects to SSE on every navigation" follow-up.

- **`<body hx-boost="true" hx-target="#main-content" hx-swap="outerHTML" hx-push-url="true">`**
  on the layout (`services/web/internal/templates/layout.html`).
  htmx now intercepts every plain `<a href="...">` click (topbar
  nav, state-filter chips, SKU links, "view →" row link, "← back to
  list", "+ New order" buttons, etc.) and swaps **only** the
  `<section id="main-content">` region — the topbar and the SSE
  sidebar are siblings of `#main-content` and survive every
  navigation unchanged. `hx-push-url` keeps the URL bar in sync
  with the rendered view, so the back button still works and the
  page is bookmarkable.

- **Sidebar EventSource persists across navigation.** Pre-fix,
  `hx-target="body"` swapped the entire body, which destroyed the
  `<aside hx-ext="sse" sse-connect="/events/stream">` element on
  every navigation — htmx-sse then re-created the EventSource,
  briefly dropping live events. With `hx-target="#main-content"`,
  the aside stays mounted: the SSE EventSource lives for the
  lifetime of the page session, the live-event `<ul>` keeps its
  accumulated items, and operators can click around without
  losing the in-flight event tail.

- **Inline scripts moved from `<body>` to `<head>`**
  (`layout.html`). With hx-boost swapping `#main-content`
  outerHTML, any inline `<script>` inside the swap target would
  re-run on every navigation, duplicating the SSE / aria-busy /
  copy-id listeners. htmx event listeners are attached to
  `document.body` (which survives the swap target), so the
  handlers continue to work after navigation. New regression test
  `TestLayout_HxBoostSwapsMainContentOnly` asserts:
  - `<body hx-boost="true" hx-target="#main-content">` is present,
  - `<header>` precedes `<section id="main-content">` which
    precedes `<aside>` in document order,
  - `sse-connect="/events/stream"` is **outside** `#main-content`
    (sidebar must persist across nav),
  - each listener is registered exactly once.

- **Existing forms unchanged.** Every form in the app
  (`/v1/orders`, `/v1/orders/{id}`, `/payments/sim/fire`) already
  declared explicit `hx-post`, which takes precedence over
  hx-boost's URL inference — so form-submission behaviour is
  identical to before (success → `HX-Redirect` → navigation).

### Added — UI: state filter chips, SKU filter, error_code selector (v1.1.7)

Closes three operator-experience gaps surfaced by the post-audit
walk-through of `services/web`: the orders list had no UI surface
for filtering, and the payments simulator's "Force fail" button
could only emit `card_declined`.

- **State filter chips on the orders list** (`services/web`).
  `GET /?state=reserved` now renders a chip-row above the table
  with All / Pending / Reserved / Confirmed / Cancelled / Failed;
  the active chip is highlighted with the accent colour, and the
  htmx polling re-fetch preserves the filter. `PageOrdersList`
  forwards the chip's value to the Order service as the existing
  `state=` query param — no upstream contract change.

- **SKU filter (client-side, BFF-level)** (`services/web`). The
  SKU cell on each orders-list row is now a link to `/?sku=SKU-X`;
  multiple SKUs compose via either `?sku=A&sku=B` or `?sku=A,B`
  (the BFF normalises both via `parseSKUFilter`). When an SKU
  filter is active the BFF widens the upstream page to 200
  (`OrderListBySKUs` then filters in memory — the Order service
  doesn't expose a per-SKU list endpoint yet, and the BFF doesn't
  want a second round-trip just for a UI filter). The inventory
  page's SKU cells link to the same view, so operators can hop
  from a stock row to "orders using this SKU" in one click. A
  "Filtered by SKU:" banner with a copy-to-clipboard SKU pill and
  a "clear SKU filter" link is rendered above the table when the
  filter is non-empty.

- **error_code selector in payments simulator** (`services/web`).
  The Force-fail form's hidden `error_code=card_declined` input
  is replaced with a `<select>` exposing all four decline paths
  the mock provider supports: `card_declined`,
  `insufficient_funds`, `network_error`, `provider_timeout`. Pre-fix
  only `card_declined` was reachable from the UI; the
  `insufficient_funds` (last-4 `0002`) and `network_error`
  (default fallback) branches existed in
  `services/payment/internal/webhook/handler.go:errorCode` but
  required a curl-level call to exercise.

- **Filter template helper** (`dict` template func). The chip-row
  template uses `{{template "filterChip" (dict ...)}}` to pass
  per-chip data; the `dict` template func is registered in
  `handlers.NewSet` with a strict even-arg contract that panics at
  template parse time rather than silently dropping the trailing
  key in production.

- **Styles** (`services/web/internal/static/styles.css`). Added
  `.filter-chips`, `.chip`, `.chip-active`, `a.mono:hover`, and
  `form.row select` selectors for the new UI elements. All
  existing responsive / focus-visible / disabled-state selectors
  are unchanged.

### Fixed — order_detail template guard + binary hygiene (v1.1.6)

- **Web: `order_detail.html` template guard on 404 path** (CRITICAL).
  The Refresh button at `services/web/internal/templates/order_detail.html:11`
  interpolated `{{.Order.ID}}` outside any `{{if .Order}}` guard, so a
  `GET /orders/<id>` whose backend returned 404 produced a
  `template: executing "orderDetailBody" at <.Order.ID>: nil pointer
  evaluating *Order.ID` panic. The HTTP 404 status was already written
  before `ExecuteTemplate`, so the body silently truncated to a
  half-rendered fragment (no "Order not found." banner, malformed
  `hx-get="/orders/<no value>?frag=1"` attribute on the Refresh
  button). Fix: wrap the Refresh button in `{{if .Order}}…{{end}}`
  and add a new `<div class="error">{{.Error}}</div>` banner shown
  only when `vm.Error` is set but `vm.Order` is nil. New regression
  test `TestOrderDetail_NotFound_BodyRendersBanner` asserts the body
  carries the banner, does not contain `<no value>`, and does not
  render a Refresh button on the 404 path.

- **Build hygiene: extended `make clean`** (MEDIUM). The previous
  `clean` target only removed `bin/`, leaving stale binaries under
  `cmd/*/*.exe`, `services/*/bin/*.exe`, and `services/*/*.exe` to
  accumulate. `make clean` now sweeps those on both Windows
  (PowerShell) and POSIX (`find -delete`). All 19 stale artifacts
  removed in this batch: `bin/_smoke_test.exe`, `bin/*-debug.exe`
  (×4), `bin/web_new.exe`, root `web_new.exe`, root
  `inventory.exe` / `order.exe` / `payment.exe` / `saga.exe` /
  `web.exe`, `cmd/{order,payment,inventory,saga,web}/*.exe`,
  `services/{order,payment,inventory,saga,web}/bin/*.exe`,
  `services/saga/internal/outbox/saga.test.exe`,
  `services/inventory/inventory.exe`, root `nul` file.

- **Docs: documented the dual `cmd/<svc>` layout** (LOW).
  `services/web/README.md` now has a "Two-layer binary layout"
  subsection explaining why both `cmd/web/main.go` (outer
  `package main`) and `services/web/cmd/web/main.go` (inner
  `package web`) exist; the same pattern is used by
  `cmd/{order,payment,inventory,saga}`. The inner layer owns
  `Main()` and the `-ldflags -X`-injected `Version`; the outer
  layer is a 10-line delegation so `go build ./cmd/web` from the
  repo root produces a working binary without exposing internal
  types from the service package.

- **Tests: marked a deferred TODO as `DEFERRED`** (LOW). The
  `// TODO: full end-to-end recovery` comment in
  `tests/chaos/kafka_kill_test.go:15` is now `// DEFERRED (v1.2+,
  see STATUS.md)` so the linter doesn't flag it and the reason is
  inline with the comment (services cache `KAFKA_BROKER` at
  startup).

### Fixed — E2E chain repair (v1.1.5)

Closes the E2E test gap surfaced by the v1.1.4 batch: the orderflow
chain never actually reached `confirmed` in CI because two distinct
test-wiring bugs masked one production bug; one of the three had
been flaky for several batches and was masked by the others.

- **Harness: saga migrations now applied to the order PG** (P0).
  The E2E test wires the saga service's `DATABASE_URL` at
  `h.PostgresURLs["order"]`, but the harness only applied the
  order service's own migrations to that PG — never the saga
  migrations. `order_sagas` and `saga_outbox` never existed; the
  saga's TTL sweep logged `relation "order_sagas" does not
  exist` continuously and `OrderCreatedHandler.InsertTx` failed on
  every event, so the chain stalled on `pending`. Fix: apply the
  saga migrations to the order PG in `mustPostgres("order")` so
  the saga runtime finds its tables where it expects them.

- **Harness: pre-create Kafka topics before any service boots**
  (P0). The testcontainer Kafka image enables
  `auto.create.topics.enable=true`, but the producer's first publish
  on a topic that doesn't yet exist races auto-create latency and
  receives `UNKNOWN_TOPIC_OR_PARTITION` for several hundred ms.
  The order service's outbox poller has a hard retry budget of
  `MaxAttempts=5 × Interval=100ms = 500ms`, after which the row
  is DLQ'd. CI logs showed the exact pattern: 5×
  `UNKNOWN_TOPIC_OR_PARTITION` followed by `context canceled`.
  Fix: new `preCreateKafkaTopics` helper in
  `tests/harness/kafka_topics.go` issues a single `CreateTopics`
  request via `kgo.Client.Request` + `kmsg` before any service
  binary boots; tolerates `TOPIC_ALREADY_EXISTS` for re-runs.

- **`payment.last_four` plumbed end-to-end** (P1 — production
  bug). The order service's `submitRequest` had no `payment`
  field, so the compensation test's
  `"payment": {"last_four": "0001"}` body was silently dropped.
  The mock payment provider's deterministic success/decline
  branch
  (`services/payment/internal/provider/provider.go`) is keyed on
  the last 4 chars of `last_four` (`0001` → declined); the
  pre-v1.1.5 handler fell back to deriving from
  `orderID[len(orderID)-4:]` (random hex, virtually never
  `0001`), so the compensation test was always exercising the
  happy path, not the failure path. Fix: add
  `submitRequest.Payment.LastFour` → forward on
  `OrderCreatedPayload.LastFour` → persist on
  `order_sagas.last_four` (new
  `services/saga/migrations/0003_saga_payment_last_four.sql`) →
  forward on `PaymentRequestedPayload.LastFour` → prefer in
  `PaymentRequested` handler (fall back to deriving from
  `orderID` for pre-v1.1.5 wire-shape clients).

### Removed

- Drop the temporary `Fprintf` chain-stall diagnostics from
  `pkg/outbox/poller.go` that landed in commit `4cd5f46` to find
  the E2E failure. Root cause was the harness saga-migrations
  bug, not something visible from inside the poller.

## [1.1.4] - 2026-08-19

### Fixed — final-engineering-pass batch

Closes the gaps surfaced by the senior-level audit of v1.1.3:
the buttons that didn't work on the web UI (`Cancel`, force
webhook), distributed-systems bugs the saga/outbox poller was
hiding, and a doc-vs-reality gap in the orderflow architecture
diagrams.

- **Cancel button on `/orders/{id}` actually cancels** (P0). The
  Order Service's chi router registered `POST` / `GET` only;
  clicking Cancel POSTed to a non-existent route and the BFF's
  `DELETE` proxy got `405 Method Not Allowed` → `502` to the
  user. Added `Repository.Cancel(ctx, id)` to the order service,
  wired through `DELETE /v1/orders/{id}` → `204` on success →
  `404` on terminal/unknown id. The handler and the
  `PGRepo.Cancel` implementation are atomic: the state transition
  to `cancelled` and the `OrderCancelled` outbox row commit (or
  roll back) in the same `pgx.BeginFunc` so the saga's consumer
  sees exactly one matching downstream event. Caller-supplied
  tests now verify: happy path, terminal-state guard (no
  double-emit when the order is already cancelled/confirmed/failed),
  unknown id → `404`, `Repository.Cancel` emits the correct
  `OrderCancelled` payload (`reason:"user_request"`,
  `source:"user"`).

- **Force-webhook buttons on `/payments/sim` work with Redis** (P0).
  Pre-fix, the BFF's `FireWebhook` issued a POST with no
  `Idempotency-Key` header. When the Payment Service runs with
  `REDIS_URL` set, the idempotency middleware returns
  `400 Idempotency-Key header required`. The web UI's
  `force ✓ / force ✗` therefore returned `502 Bad Gateway` in
  every docker-compose run. Fix: `FireWebhook` now sets a
  deterministic `Idempotency-Key: orderflow-web:{order}:{status}`
  so the provider mock's idempotency cache can dedupe replays.

### Fixed — outbox reliability (P0/P1)

- **Outbox poller no longer double-fires DLQ on persistent broker
  failure** (P0). Pre-fix, the poller called
  `src.MarkFailedTx(ctx, tx, ids)` inside the same
  `RunInTx` closure that was about to roll back on the publish
  error; the FAILED transition undid itself and the row stayed
  `PENDING` forever, so the in-memory `p.attempts` counter
  climbed across every poll and `DLQ.Send` fired once per
  ~3 polls (~33 entries in 500 ms). v1.1.3 had a regression net
  for this that PASSED against the fake source but missed the
  rollback semantics. v1.1.4 splits the closure's return contract:
  if any row in the batch crossed `MaxAttempts` the closure
  returns `nil` (commit), so the FAILED transition is durable and
  the next poll's `WHERE status = 'PENDING'` filter skips the
  row; rows still under the cap keep returning `err` (rollback)
  and stay PENDING. The saga's `MarkFailedTx` SQL is also updated
  to set `status = 'FAILED'` alongside the existing
  `attempts++` so the saga source matches the order / payment /
  inventory sources.

- **Per-Pod `attempts` counter now survives restarts** (P1, the
  v1.1.2 deferred P1-#3). The retry budget was tracked only in a
  per-Pod `sync.Map`; a pod restart wiped it and silently reset
  the budget. Added an `attempts INT NOT NULL DEFAULT 0` and
  `last_error TEXT` column to `order_outbox`, `payment_outbox`,
  and `inventory_outbox` (saga already had them) via
  `0003_outbox_attempts.sql` / `0004_outbox_attempts.sql`.
  `pkg/outbox.Source` gains an `AttemptsOfTx` method that the
  poller reads inside the locked `RunInTx` tx; `handlePublishFailure`
  uses `max(in-memory, DB)` to bootstrap the cache from zero
  without ever under-counting. The DB value is the source of
  truth so a fresh pod sees the same retry state as the pod that
  crashed. `TestPoller_DBQueriesAttemptsForDLQ` is the regression
  net: pre-seeds `dbAttempts[e1] = MaxAttempts-1` and asserts the
  very first observed failure crosses the threshold.

### Fixed — concurrency / graceful shutdown

- **Consumer dispatch marks records for unknown event types**
  (P1). When a record carried an `event_type` no service handles
  yet (forward-compatible producer), `dispatch` early-returned
  without calling `markRecord(rec)`. With
  `kgo.DisableAutoCommit`, only `CommitMarkedOffsets` advances
  offsets — the unknown record re-fetched on every poll and
  held the partition hostage forever. Fix: `dispatch` now calls
  `markRecord(rec)` before the unknown-type early-return AND on
  decode-error → DLQ paths. New tests pin the contract for both.

- **Payment Repository respects the request context** (P1). The
  `webhook.Repository` interface omitted `context.Context`, so
  every PG call used `context.Background()`. Client cancellation
  (HTTP disconnect, Kafka shutdown) could not abort in-flight
  queries; the DB backend kept processing requests the client
  would never read. `Get`/`UpdateStatus`/`UpdateStatusFromNonTerminal`
  now take and forward `ctx`; the chi handler passes
  `r.Context()`; the fake-repo test helper updated. No
  end-user-visible behavior change but observability + shutdown
  are now correct.

### Fixed — middleware saga hardening

- **Saga StockReleasedHandler uses state-guarded transition** (P2).
  The handler called `repo.UpdateState(ctx, orderID,
  sagapkg.StateCompensated)` with no `from` guard; an out-of-order
  replay of `StockReleased` could in principle overwrite a
  `Completed` saga. Replaced with `TransitionStateTx(from=
  Compensated → to=Compensated)` inside a `pgx.BeginFunc`
  (matches the rest of the saga handlers). Defensive only — the
  normal event flow makes the race unreachable — but the change
  closes the door for free.

### Fixed — infrastructure (P1/P2)

- **`kubectl kustomize deploy/kustomize/overlays/{dev,staging,prod}`
  now renders** (P1). Pre-fix the base was a comment-only stub
  with a `for svc in ... helm template ...` instruction; without
  `helm` on the controller's PATH every overlay failed with
  *"no resource matches strategic merge patch ..."*. v1.1.4 ships
  a hand-rolled `deploy/kustomize/base/services.yaml` mirroring
  the helm-template shape, fixes the per-overlay `replicas-` and
  `resources-` patch targets to the base (un-prefixed) Deployment
  names so `namePrefix` works correctly, drops the redundant
  per-overlay `namespace.yaml`, moves HPA + PDB to
  `resources:` (they're new resources, not patches), and updates
  `deploy/kustomize/README.md` with the regeneration procedure.

- **Dead code / doc drift removed** (P2). Deleted
  `services/inventory/internal/redis/doc.go` and
  `services/order/internal/saga/doc.go` — doc-only stubs with no
  implementation. The redis package documented a Redis reservation
  store that is not implemented (the actual reservation lives in
  `internal/lock` via Postgres `stock_items`); the saga package
  was a one-line marker for sub-stage 3.9 that never landed any
  code. Updated `docs/architecture/c4-level-2.puml` and
  `c4-level-3-inventory.puml` to remove the Redis-reservation
  component and the misleading `Reservations with TTL` relationship;
  Redis is now correctly described as `Idempotency cache +
  consumer dedup`.

### Tests

- `pkg/outbox/poller_test.go`:
  `TestPoller_DoesNotDoubleDLQOnPersistentBrokerDown` — 500 ms
  persistent-publisher-failure run, asserts `dlq.sent == 1`
  (pre-fix: ~33).
  `TestPoller_DBQueriesAttemptsForDLQ` — pre-seeds
  `dbAttempts[id] = MaxAttempts-1`, asserts the FIRST poll
  crosses the threshold (not after `MaxAttempts` new failures).
  `TestPoller_RetriesOnPublishError` updated to use
  `MaxAttempts=1000` so it stays in the under-cap branch and
  asserts the rollback path.

- `pkg/consumer/consumer_test.go`:
  `TestDispatch_UnknownEventTypeStillMarksForCommit` and
  `TestDispatch_DecodeErrorMarksRecord` — regression nets for
  the v1.1.4 unknown-event-type and decode-error mark
  behavior.

- `services/web/internal/backend/payment_test.go`:
  `TestPaymentClient_FireWebhook_SetsIdempotencyKey` — pins the
  Idempotency-Key determinism (replay-safe via
  `orderflow-web:{order}:{status}`).

- `services/order/internal/api/handler_test.go` and
  `services/order/internal/repository/pg_repo_test.go`:
  `TestCancel_OK/NotFound/InvalidID`,
  `TestPGRepo_Cancel_TransitionsAndEmitsEvent`,
  `TestPGRepo_Cancel_AlreadyTerminalReturnsErrNotFound`,
  `TestPGRepo_Cancel_UnknownIDReturnsErrNotFound` — handler-level
  + PG-real (skip without `DATABASE_URL`) regression nets for the
  cancel endpoint.

### Migration

```sql
-- applies to order, payment, inventory (saga already has the
-- columns since sub-stage 3.10.e):
ALTER TABLE <svc>_outbox
    ADD COLUMN IF NOT EXISTS attempts   INT    NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_error TEXT;

File names (lexically sorted by the harness testcontainers
loader): `services/order/migrations/0003_outbox_attempts.sql`,
`services/payment/migrations/0004_outbox_attempts.sql`
(payment already has `0003_payment_order_unique.sql`),
`services/inventory/migrations/0004_outbox_attempts.sql`
(inventory already has `0003_seed.sql`).
```

Pre-existing rows have `attempts=0`; their first failure is
counted as the first attempt under the new budget. Existing
`markFailed.sql` updated to `SET status='FAILED', attempts =
attempts + 1, last_error = COALESCE($1, last_error) WHERE ...
status = 'PENDING'`.

## [1.1.3] - 2026-08-19

### Fixed

- **Flaky `TestRun_ServesHealthzAndMetrics`** in 3 of 5 service
  binaries (`order`, `payment`, `inventory`). The test polled
  `ListenAddr()` to detect when the HTTP server was bound, but
  `boundAddr.Store(...)` runs **before** `httpSrv.Serve(ln)`,
  so the test sometimes returned an address whose listener
  was bound but not yet accepting. The `http.Get` then got
  ECONNREFUSED ~4 of 5 runs. Fix: poll via successful
  `GET /healthz` (pattern lifted from the saga binary, which
  fixed the same race earlier — see
  `services/saga/cmd/saga/main_test.go:waitForFreshReadyAddr`).
  Verified stable across `-race -count=20`. No production code
  change: the production runtime bound → ready transition is
  correct; only the test synchronisation was racy. Web service
  has no `main_test.go` so was unaffected.

### Tests

- `services/{order,payment,inventory}/internal/outbox/sql_test.go`:
  `TestFetchPendingSQL_OrdersByCreatedAt` now also asserts the
  embedded `fetchPendingSQL` contains `FOR UPDATE SKIP LOCKED`
  (and `status = 'PENDING'` in payment/inventory, which were
  missing). Catches the v1.1.1 regression class — a removed
  row-lock would no longer pass.

- `services/payment/internal/consumer/handlers_test.go`:
  `TestSetHandler_RaceWithLoad` — 4 writers × 200 ops hammering
  `SetHandler` while 8 readers hammer `Registry`. Race detector
  fails immediately on a regression to the plain-pointer
  pre-v1.1.2 design.

- `services/inventory/internal/consumer/handlers_test.go`:
  `TestSetPool_RaceWithRegistry` — same shape for the
  inventory `globalDeps atomic.Pointer[handlerDeps]`.

- `services/payment/internal/idempotency/middleware_test.go`:
  `TestMiddleware_ConcurrentSameKey_ExactlyOneHandlerCall` —
  32 goroutines fire the same Idempotency-Key, assert exactly
  one handler invocation. Catches the pre-v1.1.1 bug where
  duplicates got HTTP 200 with body `"in-flight"` (which the
  saga falsely treated as success).

- `services/inventory/internal/repository/pg_repo_test.go`:
  `TestPGRepo_ReleaseStock_RejectsOverRelease` (release qty >
  reserved returns ErrNotFound, stock unchanged, outbox empty)
  and `TestPGRepo_ReleaseStock_RejectsNonPositiveQty` (qty
  0/-1 rejected before any DB write). Both require DATABASE_URL;
  skip otherwise (same pattern as the rest of the file).

- `services/saga/internal/outbox/poller_pg_test.go` (NEW):
  real-PG `TestPoller_RoutesToDLQAfterMaxAttempts_PG` exercises
  the saga's `saga_outbox` schema against a live Postgres,
  pins the `attempts + 1` increment + status='FAILED'
  transition. Requires DATABASE_URL. (Located in the saga
  package rather than `pkg/outbox` to avoid a circular
  import — `pkg/outbox` is consumed by the saga, not vice
  versa.)

### Deferred to v1.2+

- (unchanged from v1.1.2) P1-#3 per-Pod attempts counter,
  P1-#4 KafkaPublisher atomic batch, Stripe-style webhook
  body-vs-strict-mode handling.

## [1.1.2] - 2026-08-18

### Fixed — adversarial-audit follow-up

A senior/staff-level adversarial audit (see `docs/superpowers/portfolio/`)
uncovered one critical regression and three production-stopping bugs
that the v1.1.0-pre batch missed. All fixes preserve the existing
API surface; new handler-level tests close the testing gap that
allowed the regression to slip past.

- **CRITICAL — outbox poller never marked rows SENT** (v1.1.0-pre
  regression). The refactor that moved publish-and-mark into a
  RunInTx closure under FOR UPDATE SKIP LOCKED also moved the
  MarkSent call site out of the closure without wiring the new
  tx-aware MarkSentTx. The poller was returning nil from the
  closure, the row's status flip never happened, and the next
  poll re-fetched and re-published the same rows forever. Fix:
  collect the batch's event IDs and call src.MarkSentTx inside
  the closure on success — the row's status flip and the row's
  lock release commit atomically.

- **CRITICAL — saga handlers re-emitted downstream events on
  Kafka redelivery** (4 of 6 handlers). StockReservedHandler,
  PaymentCompletedHandler, PaymentFailedHandler, and
  StockReservationFailedHandler called an unguarded
  UpdateStateTx (matching any state) and then unconditionally
  emitted a downstream event. On every Kafka rebalance /
  restart / duplicate delivery, the handler:
  1. bumped updated_at (no-op on state)
  2. emitted a fresh outbox row

  Worst case: a second PaymentRequested for the same order
  caused a second provider.Charge() in the payment service
  (the DB-level payments.order_id dedupe runs AFTER the charge).
  Fix: new repository method TransitionStateTx(ctx, tx, orderID,
  from, to) that returns (true, nil) on transition, (false, nil)
  when state is already past `from` (idempotent replay — handler
  must NOT emit a downstream event in this branch). Each of the
  4 non-idempotent handlers now checks the result and skips
  outbox emission on a false return.

- **CRITICAL — TTL sweep `compensate` had no state guard**. A
  saga already in `state='compensated'` (e.g. set by
  PaymentFailedHandler between the sweep's SELECT and UPDATE)
  would still match the sweep's UPDATE, and the sweep would
  emit a second StockReleaseRequested + OrderCancelled. Fix:
  add `AND state NOT IN ('completed', 'compensated')` to the
  WHERE clause, check RowsAffected, and skip outbox emission
  when 0.

- **CRITICAL — `ReleaseStock` SQL had no `WHERE reserved >= qty`
  guard**. A buggy producer emitting a larger release than the
  original reservation would drive stock_items.reserved negative
  and inflate available on every subsequent release — permanent
  counter corruption. Fix: add `AND reserved >= \$2` to the
  WHERE clause; RowsAffected=0 (over-release or unknown SKU)
  returns ErrNotFound so the consumer ack-skips.

- **Payment webhook had no terminal-state guard** (P1-#1). A
  late webhook with status='failed' against a payment already
  in StatusCaptured would overwrite the status AND emit
  PaymentFailed. Fix: new Repository method
  UpdateStatusFromNonTerminal(id, to, events) that runs
  UPDATE … WHERE id=\$1 AND status NOT IN ('captured',
  'failed'); RowsAffected=0 when the row is already terminal, in
  which case no outbox row is emitted.

- **Order consumer's terminal-state guard excluded 'failed'**
  (P1-#2). Pre-v1.1.1 SQL excluded only 'confirmed' and
  'cancelled', letting a late OrderConfirmed event resurrect a
  'failed' order to 'confirmed'. StateFailed is reachable via
  the saga TTL sweep, so it must be protected by the same guard.
  Fix: add 'failed' to the NOT IN clause.

- **Idempotency middleware cached empty body on empty response**
  (P1-#7). A handler that returned without writing (panic
  recovered, ctx cancelled mid-flight, or simply a no-op
  handler) had its empty body cached via Complete(). The next
  retry got HTTP 200 with body='', falsely reporting success
  to the saga. Fix: if buf.status==0, call Release() instead of
  Complete() so the next retry hits the handler.

- **Idempotency middleware crashed on handler panic**. A panic
  inside next.ServeHTTP propagated out of the middleware,
  leaving the in-flight marker in Redis for the full TTL (24h
  by default) — operators saw 409s pile up. Fix: wrap
  next.ServeHTTP in defer recover(), log, and Release the
  reservation so a retry can succeed once the underlying bug
  is fixed.

- **Consumer dispatch crashed on handler panic** (P1-#6). A
  single panic killed the consumer goroutine silently and
  events piled up in Kafka retention until Kubernetes noticed
  the failed liveness probe. Fix: defer recover() in dispatch,
  log with event_id / event_type / offset / partition, mark the
  record for commit so the panic doesn't loop on retry.

- **Webhook had no max body size** (P1-#9). json.NewDecoder on
  r.Body would allocate the entire JSON object in memory before
  any size check. Fix: cap the body at 64 KiB via
  http.MaxBytesReader.

- **Global vars `globalHandler` / `globalDeps` were plain
  pointers** (P1-#10). Go's memory model doesn't guarantee that
  a pointer write is visible atomically to readers on other
  CPUs — a concurrent read in the consumer goroutine could
  observe a torn pointer. Fix: switch to atomic.Pointer[Handler]
  / atomic.Pointer[handlerDeps]. The Store/Load semantics are
  well-defined across goroutines and lock-free on the hot
  path (every Kafka record).

- **events.Client.Publish ignored ctx**. The convenience
  Publish method used context.Background() and a slow Kafka
  produce kept the service goroutine alive past the SIGTERM
  grace period. Fix: Publish now takes ctx and forwards it to
  PublishRaw.

### Tests
- `services/saga/internal/consumer/handlers_idempotency_test.go`:
  the first handler-level (not just registry-level) tests in
  the saga package. They exercise the FULL handler with a real
  PostgreSQL transaction and would have caught P0-#2 if they
  had existed.

- `services/payment/internal/webhook/handler_test.go`:
  TestWebhook_TerminalGuard_LateFailedAfterCaptured,
  TestWebhook_TerminalGuard_LateCapturedAfterFailed,
  TestWebhook_TerminalGuard_SameStatusReplay.

- `pkg/consumer/consumer_test.go`:
  TestDispatch_RecoversHandlerPanic.

- `services/payment/internal/idempotency/middleware_test.go`:
  TestMiddleware_EmptyBodyReleases,
  TestMiddleware_HandlerPanicRecovers.

- `pkg/outbox/poller_test.go`: extended
  TestPoller_PollsAndPublishesOnce to assert MarkSentTx was
  called. The test now FAILS if MarkSentTx is removed (catches
  the v1.1.0-pre regression).

### Deferred to v1.2+
- P1-#3: per-Pod attempts counter (sync.Map in the poller). The
  saga's DB attempts column is incremented by MarkFailedTx but
  unused for the DLQ decision; order/payment/inventory don't
  even have an attempts column. Fix requires schema changes
  (0002 migration for each of the 3 outbox tables) plus
  refactoring the poller to read attempts from the row.
- P1-#4: KafkaPublisher.Publish is documented as "batched into
  a single Kafka producer transaction" but actually makes N
  separate PublishRaw calls. Fix requires adding
  BeginTransaction/EndTransaction to the KafkaClient interface
  and adopting franz-go's transaction API.
- Webhook body-size-vs-strict-mode + idempotency-key body
  mismatch (Stripe-style 422).

## [1.1.1] - 2026-08-18

### Fixed — Senior-review pass

This batch addresses findings from a Staff/Senior-level audit
covering distributed-systems correctness, concurrency, and
observability. The platform was previously end-to-end functional
but unsafe under realistic failure modes (multiple replicas, restart
loops, partial writes, retry storms). All changes preserve the
existing API surface and saga state machine.

- **Consumer offsets never committed** (`pkg/consumer/consumer.go`):
  `CommitMarkedOffsets` was a no-op because no record was ever
  marked. Added `MarkCommitRecords(rec)` after every successful
  dispatch (and after DLQ exhaustion) so franz-go actually
  advances the offset on each commit.
- **No deduper wired in any service**: even with offsets fixed, the
  in-memory deduper lost state on restart. Added
  `pkg/consumer.RedisDeduper` (7-day TTL) and a
  `NewDeduperFromRedisURL` helper; all 4 service binaries accept
  and wire a deduper. Falls back to `NoopDeduper` when REDIS_URL
  is unset so local development keeps working.
- **Saga state-update and outbox.Append on separate transactions**:
  every saga handler in `services/saga/internal/consumer/handlers.go`
  could leave the saga advanced without the matching event emitted
  on a transient failure. Refactored to wrap state-update +
  emit in a single `pgx.BeginFunc`. Added tx-aware
  `InsertTx`/`UpdateStateTx`/`GetTx` to the saga repository.
- **Inventory never released stock on compensation**:
  `StockReleaseRequestedPayload` carried only `OrderID` and
  `ReservationID`, so the inventory handler couldn't decrement
  `stock_items.reserved`. Every cancelled order leaked a phantom
  reservation. Added `SKU` and `Quantity` to the payload; saga
  emits one StockReleaseRequested per item; inventory now calls
  `repo.ReleaseStock` atomically with the outbox row.
- **Outbox poller double-published on multi-replica deploys**:
  plain `SELECT ... WHERE status='PENDING'` had no row lock.
  Two replicas would both pick up the same rows. Added
  `FOR UPDATE SKIP LOCKED` and changed the `Source` interface to
  `RunInTx` so fetch + publish + mark happen under one row lock.
- **Payment events published to wrong topic**: `const topic = "order-events"`
  in the payment consumer contradicted the spec / Helm. Now
  publishes to `payment-events`; saga subscribes to it.
- **Idempotency in-flight duplicates returned 200 with `"in-flight"`
  body**: introduced `ErrInFlight` and mapped it to HTTP 409
  Conflict. Stripe-style concurrent-retry semantics.
- **`docker compose up` failed with "relation does not exist"**:
  init scripts only created extensions. Mounted per-service
  migrations and apply them after extensions.
- **`Repository.Insert` ignored request context**: client
  cancellation mid-write still committed. Added ctx to the
  Repository interface; PGRepo honors it.
- **Order consumer flipped cancelled orders back to confirmed** on
  redelivered events: added `WHERE state NOT IN ('confirmed', 'cancelled')`
  guard on `UPDATE orders`.
- **Payment consumer deduped on paymentID (fresh UUID per delivery),
  never on order_id**: added migration `0003_payment_order_unique.sql`
  (UNIQUE constraint) and switched to `ON CONFLICT (order_id) DO NOTHING`.
- **Consumer goroutines not tracked**: under Kubernetes rolling
  deploys, main could exit while the consumer was still processing.
  All 4 runners now take a `*sync.WaitGroup` and block shutdown on
  it.

### Housekeeping
- README port claims now match code/Helm defaults (8081/8082/8083/8084).
- Dropped `StatePaymentPending`/`StatePaymentComplete` saga constants
  (declared but never wired in `transitionTable`).
- Saga consumer `itemsJSON` error now propagated (was `_ =`).
- Repository `List` clamp widened to 500 to match handler.
- pkg/consumer swallowed errors now logged via slog.Warn.
- `.gitignore` extended for Windows redirect targets (`nul`),
  demo recording artifacts, and stray service logs.

### Deferred (v1.2+)
- Outbox-retry chaos assertion (currently asserts only that the
  order service stays healthy after Kafka kill; full recovery
  assertion blocked on broker-discovery in service binaries).
- ghcr.io publishing pipeline.

## [1.1.0-pre] - 2026-08-17

### Fixed
- **Saga shutdown goroutine leak**: The saga binary's outbox poller, TTL sweep, and HTTP server goroutines were launched without `sync.WaitGroup` tracking. On SIGTERM, `Run()` returned before the goroutines had exited, allowing a rolling Kubernetes deploy to kill a poller iteration or TTL sweep mid-transaction. The saga service now mirrors the `order`/`payment`/`inventory` shutdown pattern: a `sync.WaitGroup` tracks the poller, TTL sweep, and HTTP server goroutines, and the close path waits on a 5-second shutdown context before closing the DB pool.
- **`mustMarshal` panic in TTL sweep compensation**: `services/saga/internal/watchdog/ttl_sweep.go` called `panic(err)` from a `json.Marshal` failure inside `pgx.BeginFunc`. A panic there could leave the surrounding transaction in an indeterminate state. The helper has been removed; `compensate` now uses inline `json.Marshal` and propagates errors through the function's normal `error` return.

### Changed
- `startSagaOutbox` signature now takes a `*sync.WaitGroup` so its poller goroutine is tracked alongside the TTL sweep and HTTP server.
- New private helper `wgWait` consolidates the saga shutdown sequence (wait goroutines, close outbox, close consumer, shutdown HTTP, close pool).

## [1.0.0] - 2026-08-17

### Added — v1.0 release

- **kind smoke test** (`v1.0.kind`): New `tests/k8s/smoke_test.go` creates a real kind cluster using `deploy/kind/kind.yaml`, waits for nodes ready, validates that all 4 service Helm charts + the infra postgres chart render to valid YAML. Skippable via `KIND_SKIP=1` or `-short`. New `make smoke-k8s` target. CI will run it.
- **Asciinema recording** (`v1.0.demo`): `docs/demo/record.sh` + `docs/demo/RECORDING.md` document how to capture the demo as an asciinema `.cast` file. New `make record` target. Manual step (requires asciinema binary install — not automated in CI).

### Platform status at v1.0

- ✅ All 4 services (`order`, `payment`, `inventory`, `saga`) compile + run with PGRepository + REST API + real consumer handlers.
- ✅ End-to-end event flow: `POST /v1/orders` → OrderCreated → saga → StockReserveRequested → inventory reserves → StockReserved → saga → PaymentRequested → payment → PaymentCompleted → saga → OrderConfirmed → order=confirmed.
- ✅ Saga recovery: cross-restart TTL sweep compensates stuck sagas.
- ✅ W3C tracecontext through Kafka (kafkaprop module + outbox + consumer + chi middleware + service.version resource).
- ✅ Helm charts for all 4 services + 3 infra deps.
- ✅ Kustomize overlays (dev/staging/prod) with HPA + PDB for prod.
- ✅ ArgoCD ApplicationSet for GitOps delivery.
- ✅ testcontainers harness + E2E tests (happy + compensation + chaos) + k6 load test.
- ✅ CI: build matrix + E2E job (ubuntu-only, `needs: build`).
- ✅ 4 ADRs + 5 C4 diagrams + demo script + recording runbook.
- ✅ Binaries report real git version (LDFLAGS injection).

### Deferred to v1.1
- Full outbox-retry chaos assertion (services cache `KAFKA_BROKER`).
- kind smoke: actual image loading into cluster (currently validates Helm rendering only).
- ghcr.io publishing pipeline.

## [0.6.0] - 2026-08-17

### Added
- **Saga cross-restart TTL sweep**: New `services/saga/internal/watchdog` package with `TTLSweep` that periodically queries `order_sagas WHERE expires_at < NOW() AND state NOT IN ('completed', 'compensated')` and emits `StockReleaseRequested` + `OrderCancelled` (reason="timeout") for each expired saga. Wired into `cmd/saga/main.go` to run every 30s in the same goroutine as the consumer + outbox poller. Survives saga-binary restarts — any non-terminal saga past its 5-minute TTL gets compensated automatically.
- New repository method `PGRepo.ListExpired(ctx, limit)` — non-terminal expired sagas, ordered by `expires_at ASC`, bounded slice. TDD verified.

### Deferred to v1.0
- 3.12.f kind smoke test.
- 3.13.d asciinema recording.
- Full outbox-retry chaos assertion.

## [0.5.0] - 2026-08-17

### Added — platform now actually works end-to-end

The saga runtime and all consumer handlers are wired up. The platform can now process orders from `POST /v1/orders` through to `confirmed`.

- **Saga runtime** (`v0.5.0.saga`): Consumer subscribes to `order-events`, handles `OrderCreated` (start saga → emit `StockReserveRequested`), `StockReserved` (advance → emit `PaymentRequested`), `PaymentCompleted` (advance → emit `OrderConfirmed`), `PaymentFailed` (compensate → emit `StockReleaseRequested` + `OrderCancelled`), `StockReleased` (cleanup), `StockReservationFailed` (cancel). `PGRepository` over `order_sagas` table + new `saga_outbox` table + outbox poller publishing to Kafka. Watchdog code present but cross-restart TTL sweep deferred.
- **Inventory consumer** (`v0.5.0.inventory`): Real `StockReserveRequested` handler calls `repo.ReserveStock` and emits `StockReserved` (or `StockReservationFailed` on `ErrInsufficientStock`). Real `StockReleaseRequested` handler emits `StockReleased`. New `POST /v1/inventory/reserve` endpoint for synchronous reserve.
- **Payment consumer** (`v0.5.0.payment`): Real `PaymentRequested` handler calls `provider.Charge` (mock — last-4 derived from order_id), persists `payments` row + outbox event in same tx. Emits `PaymentCompleted` on `succeeded`, `PaymentFailed` on `failed`.
- **Order consumer** (`v0.5.0.order`): Real handlers for `StockReserved` (state=reserved), `StockReservationFailed` (state=cancelled), `OrderConfirmed` (state=confirmed), `OrderCancelled` (state=cancelled), `PaymentFailed` (state=cancelled). Order subscribes to all 3 topics (order-events, payment-events, inventory-events).

### Housekeeping
- `3.4.1` Committed missing `cmd/saga/go.sum` and `services/saga/go.sum` (existed since 3.10.d.saga but were untracked).
- `3.4.2` LDFLAGS injection: `make build` now bakes `git describe --tags --always --dirty` into each binary's `main.Version` field. Binaries report real version (e.g. `v0.4.0-3-gcf3195d-dirty`) instead of `0.0.0-dev`.

### Deferred to v0.6.0
- 3.12.f kind smoke test.
- 3.13.d asciinema recording.
- Saga watchdog cross-restart TTL sweep.
- Full outbox-retry chaos assertion (services cache `KAFKA_BROKER`).

## [0.4.0] - 2026-08-17

### Added
- **3.5.g** Payment Service complete: `POST /v1/payments/webhook` handler with `PaymentCompleted`/`PaymentFailed` event emission, `PGRepository` (tx-atomic UpdateStatus + outbox writer), wired into the payment binary's chi router at `/v1/payments/webhook` when DB+Redis wired. Idempotency middleware wraps the webhook when `REDIS_URL` is set (logs warning if not). 11 stdlib tests in `handler_test.go` cover happy path, status variants, error_code derivation from last-4 (matches `provider.Charge` table), 400/404/500 cases.
- **3.6.g** Inventory Service: `PGRepository` with `GetStock/ReserveStock/ReleaseStock` (optimistic version check via `UPDATE ... WHERE version = $1`, tx-atomic outbox INSERT via `svcoutbox.PGWriter.Append`), `GET /v1/inventory/stock/{sku}` mounted in inventory binary. 3 stdlib tests (round-trip, outbox-event atomicity, state-filtered List) — verified against live `postgres:16-alpine` via Docker.
- **3.11.polish** `make e2e` aggregate target + `e2e-happy`, `e2e-compensation`, `e2e-chaos` sub-targets. New `harness.RestartKafka(ctx)` helper for chaos recovery tests.

### Deferred to v0.5.0
- 3.12.f kind smoke test (`kind` binary not installed locally).
- 3.13.d asciinema recording.
- Full outbox-retry chaos assertion — services cache `KAFKA_BROKER` env at startup; restarting Kafka doesn't reconnect already-running service processes. Either the chaos test should restart services too, OR services should support dynamic broker discovery (separate concern).

## [0.3.0] - 2026-08-17

### Added
- **3.4.g** Order Service PGRepository (`Insert/Get/List` over `pgxpool.Pool`, tx-atomic with outbox writer). REST handler wired into the order binary's chi router at `/v1/orders`. Compile-time `var _ api.Repository = (*PGRepo)(nil)` assertion.
- **3.11.prep** `tests/harness.StartService(t, name, binName, env)` — launches a service binary as a child process with full env wiring (`DATABASE_URL`, `KAFKA_BROKER`, `REDIS_URL`, `HTTP_ADDR`) + `OTEL_EXPORTER=stdout` for hermeticity. Service logs to `tests/logs/<name>.log`. Graceful SIGTERM on stop.
- **3.11.b** E2E happy path test — POSTs `examples/order.json`, polls until `confirmed` within 60s. Boots order/payment/inventory/saga against testcontainers harness.
- **3.11.c** E2E compensation test — POSTs order with `last_four=0001` (forces `card_declined`), polls until `cancelled`/`failed`.
- **3.11.d** Chaos test — kills Kafka container mid-order, asserts order service stays healthy and order does not spuriously progress to `confirmed`. (Full outbox-retry recovery assertion deferred — requires `RestartKafka` harness helper.)
- **3.11.e** Load test — k6 50 VUs × 60s, thresholds `p(95)<1000` + `rate<0.05`. Go wrapper in `tests/load/load_test.go` shells out to k6 binary. New `make load` target.
- **3.11.f** CI job — new `e2e` GitHub Actions job (ubuntu-only, `needs: build`, 30-min timeout) running happy/compensation/chaos. Load test excluded (manual `make load`).
- **3.12.c** Kustomize overlays — `deploy/kustomize/{base,overlays/{dev,staging,prod}}`. Dev: replicas=1, ingress, reduced resources. Staging: replicas=2. Prod: replicas=3, HPA (cpu 70%, 3→10), PDB (`minAvailable: 2`), larger resources. Base references `all-services.yaml` (regenerated via `helm template`).
- **3.12.d** ArgoCD manifests — `projects.yaml` (AppProject RBAC constraining destinations to `orderflow-*`), `appset.yaml` (ApplicationSet per env), per-env `overlays/{dev,staging,prod}.yaml`. Automated prune + selfHeal, exponential backoff retry (5 attempts, 10s→5m).

### Deferred to v0.4.0
- 3.12.f kind smoke test (`kind` binary not installed locally).
- 3.13.d asciinema recording.
- `RestartKafka` helper for full outbox-retry chaos assertion.

## [0.2.0] - 2026-08-17

### Added
- **Tracing (3.10)**: W3C tracecontext propagation through Kafka. New `pkg/platform/instrumentation/kafkaprop` module (Inject / Extract / SpanFromEnvelope). Outbox publisher populates `Envelope.TraceID`/`SpanID` from active span. Consumer restores traceparent and creates `consumer.<EventType>` child span. chi middleware on `/healthz` and `/metrics` for all 4 service binaries. `service.version` resource attribute on every service. OTLP env defaults (`OTEL_EXPORTER=otlp`, `OTEL_EXPORTER_OTLP_ENDPOINT=otel-collector:4317`).
- **E2E harness (3.11.a)**: `tests/harness` testcontainers-go harness starting 3 Postgres + Redis + Kafka (confluent-local) + optional OTel collector. Self-test PASS in ~18s. Per-service migrations applied on startup. Individual E2E/chaos/load tests deferred to v0.3.0.
- **Helm charts (3.12.a, 3.12.b)**: Production-ready Helm 3 charts for all 4 services + 3 infra deps (postgres with 3 databases, redis, redpanda). `values.yaml` (production defaults) + `values-dev.yaml` (replicas=1, debug logs). Security defaults (runAsNonRoot, readOnlyRootFilesystem). ServiceAccounts + ConfigMaps. Kustomize overlays + ArgoCD manifests deferred to v0.3.0.
- **kind cluster (3.12.e)**: `deploy/kind/kind.yaml` with 10 host→container port mappings matching prod docker-compose. Makefile targets `kind-up / kind-down / kind-load / kind-status` with prereq check.
- **ADR-0004 (3.13.a)**: W3C tracecontext decision. Decision log table added to README.
- **Saga C4 diagram (3.13.b)**: `docs/architecture/c4-level-3-saga.puml` (33 lines) — state machine, consumer, publisher, watchdog, Postgres writer/source.
- **Demo script (3.13.c)**: `docs/demo/demo.sh` runs the full happy path (compose up → build → start services → POST order → poll until confirmed) in ~60s. `docs/demo/README.md` with prerequisites + troubleshooting.
- **Sub-stages index (3.13.e)**: `docs/superpowers/portfolio/orderflow-substages.md` — 73-row table; fixes broken README link.

### Changed
- `outbox.Record` gains `Headers map[string]string` (JSONB column added per service via `0002_outbox_headers.sql`).
- `events.Client.PublishRaw` takes headers map.
- `platform.InitTracing` signature now `(ctx, name, version)` (was `(ctx, name)`).

### Deferred to v0.3.0
- 3.11.b–e individual E2E tests (harness ready; tests to be added).
- 3.11.f E2E CI job.
- 3.12.c Kustomize overlays.
- 3.12.d ArgoCD Application manifests.
- 3.12.f kind smoke test (requires `kind` binary).
- 3.13.d asciinema recording (manual).
