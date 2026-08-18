# orderflow-web — Personal playground / demo UI design

**Created:** 2026-08-18
**Status:** approved (brainstorming complete; "Делаем")
**Goal:** Add a tactile web UI to orderflow so a developer can poke at the order, payment, and inventory flows from the browser, and watch saga events arrive in real time.

## Context

orderflow reached v1.0.0 on 2026-08-17 with 3 microservices (Order / Payment / Inventory) + saga orchestrator, REST APIs, Kafka (Redpanda), Postgres per service, Redis reservations, full E2E test suite, Helm charts, GitOps delivery. The platform has been verified end-to-end via `bash docs/demo/demo.sh` and `make e2e`, but there is no UI — all interaction is via `curl`/`k6`/asciinema. As a result, "trying it out" requires either:

1. Reading the OpenAPI spec and crafting curl commands, or
2. Running the asciinema recording and watching instead of touching.

That is fine for a CI artifact and a portfolio demo, but it makes the platform feel inaccessible for hands-on exploration. The goal of orderflow-web is to give a developer a real playground: open `http://localhost:8083`, create an order, watch it transition `pending → reserved → confirmed`, fire a forced-failure webhook to see compensation in action, and see the underlying Kafka events arrive in the sidebar.

This is the lowest-effort way to make the system feel "real" without turning it into a product. There is no production use case here — the existing `make demo` path stays as the canonical automated story; this adds a parallel hands-on path.

## Goals

1. **Hands-on playground** — at `:8083`, the developer can create / list / view orders and watch state transitions in the browser, with no CLI knowledge required.
2. **Real-time saga visibility** — a live event sidebar streams `order-events` from Kafka so the developer can see *why* an order moved between states, not just *that* it moved.
3. **Forced-failure exploration** — a payment webhook simulator lets the developer trigger `card_declined` and watch saga compensation undo the inventory reservation in real time.
4. **Inventory transparency** — a simple stock viewer shows available and reserved quantities per SKU so the developer can correlate an order with its inventory impact.
5. **Zero disruption to existing flows** — Order / Payment / Inventory / saga binaries and their tests are not modified. No new CORS surface. The web service is BFF; it owns all browser↔backend translation.

## Non-goals (explicitly out of scope)

- A production admin UI (no auth, no RBAC, no audit log, no pagination of millions of orders).
- A portfolio / showcase UI with bespoke styling, animated diagrams, or PNG-rendered C4 charts embedded in pages.
- Replacing the existing CLI demo (`docs/demo/demo.sh`) or the asciinema recording.
- Modifying Order / Payment / Inventory / saga code or their OpenAPI specs.
- New end-to-end tests for the web UI itself (its value is interactive, not asserted).
- A SPA framework (React / Svelte / Vue) or a Node.js toolchain.
- Multi-tenancy / customer scoping / user accounts.
- gRPC-web bridging (per ADR-0003, the gRPC surface does not yet exist; web only calls existing REST).
- TLS termination, reverse proxy configuration, or production deployment manifests for the web service (local-only; reuse existing `make run-*` + `make` / `docker compose up` patterns).

## Architecture

**New Go module in the existing monorepo:** `services/web/` (added to `go.work`). Binary `bin/web`, listens on port **8083**.

**Why a new module and not a new package inside an existing service:**
- Independent deploy / restart cycle during playground iteration.
- Independent test surface (handlers tested against fake `OrderClient`/`PaymentClient`/`InventoryClient`).
- A new entry point in the build matrix is honest about the new bounded context.

**Why a Go-served HTML page and not a separate frontend project:**
- Reuses `pkg/platform` for logging, OTel, types, errors.
- One `go build`, one binary, one deployable artifact.
- htmx gives ~90% of the interactivity a SPA would, with no Node.js toolchain.
- Aligns with the project's "lean Go monorepo" character.

**Stack:**
- HTTP: `chi` (matches existing services).
- Templating: `html/template` (stdlib, no deps).
- Interactivity: `htmx 2.x` from CDN + a small vanilla JS helper for SSE-event-to-badge updates.
- Styling: one hand-rolled CSS file (`internal/static/styles.css`, embedded). Minimal; the playground prioritizes function over form.
- Kafka consumer: existing `pkg/consumer` with a new `consumer-group-id = "orderflow-web"` and `topic = order-events`.
- Config via env (matches `cmd/order/main.go` etc.): `ORDER_URL`, `PAYMENT_URL`, `INVENTORY_URL`, `KAFKA_BROKERS`, `PORT` (default `:8083`).

### Component layout

```
services/web/
├── cmd/web/main.go                  # entry point; mirrors cmd/order shutdown (WaitGroup)
├── go.mod / go.sum
├── README.md                        # how to run locally + via compose
├── internal/
│   ├── server/
│   │   └── server.go                # chi router + middleware (recover, reqlog, otel) + routes
│   ├── backend/
│   │   ├── client.go                # OrderClient / PaymentClient / InventoryClient interfaces
│   │   ├── order.go                 # real HTTP client → :8080/v1/orders[/...]
│   │   ├── payment.go               # real HTTP client → :8081/v1/payments/webhook
│   │   └── inventory.go             # real HTTP client → :8082/v1/inventory[/...]
│   ├── kafkatail/
│   │   └── tail.go                  # pkg/consumer wrapper; broadcasts events to in-process subs
│   ├── events/
│   │   └── bus.go                   # in-process pub/sub (channel-per-subscriber) for SSE
│   ├── handlers/
│   │   ├── pages.go                 # GET /, GET /orders/new, GET /orders/{id}, GET /inventory, GET /payments/sim
│   │   ├── orders.go                # POST /v1/orders (proxy + redirect), DELETE proxy
│   │   ├── payments.go              # POST /payments/sim/... (builds webhook payload, forwards to :8081)
│   │   └── events.go                # GET /events/stream (SSE relay from bus)
│   ├── templates/
│   │   ├── layout.html              # shared shell, htmx script tag, sidebar
│   │   ├── orders_list.html         # /
│   │   ├── order_new.html           # /orders/new
│   │   ├── order_detail.html        # /orders/{id} (pollable fragment + full page)
│   │   ├── inventory.html           # /inventory
│   │   └── payments.html            # /payments/sim
│   └── static/
│       └── styles.css               # embedded via embed.FS
```

### Build / wiring

- `go.work`: append `./services/web` to the `use` array.
- `Makefile`:
  - `make build-web` — build only the web binary.
  - `make run-web` — `go run ./services/web/cmd/web`.
  - `make build` — extend to include `bin/web` (currently builds `order`, `payment`, `inventory`, `saga`; change to a glob or add an explicit entry).
  - `make clean` — already removes `bin/*`; no change needed.
- `deploy/docker-compose.yml`: new `web` service. Depends on `order`, `payment`, `inventory` being healthy. Exposes `:8083`. Reuses the same base image pipeline (none yet — services are built locally for now per v1.1 design); until images are introduced (deferred to v1.1.x), the compose entry builds `web` from source in dev mode, or the developer runs `make run-web` separately on the host.

  Concrete compose shape (the actual implementation stage will pick one of these explicitly):
  - **Option A (chosen):** add `web` to compose with a Dockerfile built from `services/web/Dockerfile` (new file, follows the pattern staged in v1.1 design's docker-build section but only for this one service for now). Wait for `order`, `payment`, `inventory` `healthy` before starting.
  - The default `make demo` flow gains a `web` step that prints `Open http://localhost:8083 once everything is up`.
- CI (`.github/workflows/ci.yml`): existing build matrix covers all Go modules via `make build` glob (verify exact pattern during planning). No new job needed. E2E job unaffected (it tests backend only).

## Pages / Routes / Data Flow

### Routes

| Method | Path                 | Purpose                                                     |
|--------|----------------------|-------------------------------------------------------------|
| GET    | `/`                  | Orders list (htmx polls every 2s)                           |
| GET    | `/orders/new`        | Create-order form (htmx-friendly)                           |
| GET    | `/orders/{id}`       | Order detail (htmx polls every 1s while not terminal)       |
| POST   | `/v1/orders`         | Form submit → proxied POST `:8080/v1/orders`; redirects to detail page |
| POST   | `/v1/orders/{id}`    | Cancel proxy to `:8080` (DELETE under the hood)              |
| GET    | `/inventory`         | Stock viewer (poll every 3s)                                |
| GET    | `/payments/sim`      | Force-success / force-fail webhook simulator                |
| POST   | `/payments/sim/fire` | Builds webhook payload, POSTs `:8081/v1/payments/webhook`, redirects to sim page |
| GET    | `/events/stream`     | SSE relay — broadcasts `order-events` from Kafka to clients |
| GET    | `/healthz`           | Liveness (always 200 if process is up)                      |
| GET    | `/readyz`            | Readiness — pings Order/Payment/Inventory `/healthz` in parallel |

### Data flow — "create an order"

1. Browser GETs `/orders/new`; server renders form (htmx-enhanced `<form>`).
2. User submits → POST `/v1/orders` (browser → `:8083`).
3. `handlers/orders.go` `Submit` validates form fields, builds `OrderSubmit` JSON, calls `OrderClient.Submit(ctx, payload)`.
4. `backend.OrderClient` POSTs to `${ORDER_URL}/v1/orders` with the same payload + content-type.
5. On `201 Created` with `Location` header (or 201 body): the handler issues `htmx`-aware redirect (`HX-Redirect: /orders/{id}`) so htmx navigates without full page reload.
6. On `4xx`: handler re-renders `order_new.html` with an inline error message; on `5xx` from backend: 502 page with retry link.
7. The new order's `id` is now in the world. Order service emits `OrderCreated` to `order-events` per its existing outbox flow.
8. saga consumes `OrderCreated` from `order-events` → emits `StockReserveRequested` to `order-events` (saga's outbox source always stamps `Topic = "order-events"`, per `services/saga/internal/outbox/source.go`).
9. Inventory reserves (Redis + DB), emits `StockReserved` to `inventory-events`.
10. saga consumes `StockReserved` from `inventory-events` → emits `PaymentRequested` to `order-events`.
11. Payment mock provider (per `cmd/payment`) auto-completes with `succeeded` when last-4 ≠ `0002`, otherwise `card_declined`. Emits `PaymentCompleted` or `PaymentFailed` to `payment-events`.
12. saga consumes the result (`PaymentCompleted` / `PaymentFailed` arrive back on `order-events` — the saga publishes its post-payment decision there) → emits `OrderConfirmed` to `order-events`, or starts compensation (which also emits to `order-events`).
13. Order service consumes `OrderConfirmed` from `order-events` (Order's consumer subscribes to `order-events`, `payment-events`, `inventory-events` per `services/order/internal/consumer/runner.go`) → updates state to `confirmed` → emits a new `order-events` entry if applicable.
14. Meanwhile, `services/web/internal/kafkatail` is running its own consumer group `orderflow-web` subscribed to `order-events`. Each event is pushed to the `events.Bus`.
15. Every browser with `/events/stream` open receives the event as an SSE `data: {json}\n\n` line.
16. The browser's `index.html` registered an `EventSource`; on each event, vanilla JS updates the right-side "live events" sidebar and, for the currently-viewed order, applies a brief state-change highlight.
17. Because the order detail page also polls `/orders/{id}` directly via htmx, the page's state badge will independently refresh even if SSE is disconnected.

### Data flow — "force a payment failure"

1. User goes to `/payments/sim`, sees a list of recent in-flight orders (open state: `pending`, `reserved`).
2. User clicks "Force card_declined" next to a `reserved` order.
3. Browser POSTs `/payments/sim/fire` with `{order_id, error_code}` (form-encoded).
4. `handlers/payments.go` builds a `PaymentWebhook` payload with `status=failed`, `error_code=card_declined`, and a `payment_id` derived from `order_id` (deterministic, so replay is idempotent — the mock provider is idempotent on payment_id).
5. Handler POSTs to `${PAYMENT_URL}/v1/payments/webhook`.
6. Payment service processes per existing flow → emits `PaymentFailed` → saga compensation → emits undo events → final order state becomes `cancelled` or `failed`.
7. The events arrive through `/events/stream`; the orders list updates within ~2s (poll) and the order detail highlights the failure.

### Why htmx instead of a SPA

- The state changes observed in the UI are 100% server-driven; there's no client-only state to manage.
- Forms already work server-side; htmx adds progressive enhancement (no full page reload) at zero template cost.
- No build step → no `node_modules` → CI stays `Go` only.
- Pagination, validation, error rendering all remain server-rendered HTML, exactly like the rest of the project's existing service handlers.

## Error handling

| Failure mode                            | Behavior                                                                                          |
|-----------------------------------------|---------------------------------------------------------------------------------------------------|
| Backend service unreachable (e.g. :8080 down) | `/readyz` returns 503; pages render with a "backend not reachable" banner; in-flight POSTs return 502 with retry button. |
| Backend returns 4xx                     | Form pages re-render the form with an error region populated from the upstream `Error` JSON.      |
| Backend returns 5xx                     | Handler logs + returns 502 to browser with a generic "upstream error" page.                       |
| Kafka unreachable                       | Web service starts in "events disabled" mode: `/events/stream` returns `503 events unavailable`, sidebar shows "Live events: disconnected (Kafka down)". Other pages continue to work. |
| SSE client disconnect                   | Per-client cleanup (remove from bus); consumer goroutine unaffected.                               |
| Invalid form input                      | Server-side validation in handlers; rendered inline above the offending field (htmx swap target). |
| Unknown route                           | 404 page reusing the layout.                                                                      |
| Embedding / template parse bug at startup | Caught by `embed.FS` parse + `template.Must`; binary fails to start (fail-fast in `main`).        |

Logging uses `pkg/platform/logging` with the service name `web`. All errors carry `order.id` (if known), `route`, and a generated `request_id` from chi middleware.

## Testing

This is a playground — testing strategy deliberately stops short of full E2E.

- **Unit tests** for `internal/backend/*` against a `httptest.Server` representing each backend service (one per client, total ~6 tests). Coverage: happy path, 4xx, 5xx, network error.
- **Unit tests** for `internal/handlers/*` (~10 tests) using fake `backend.OrderClient` etc. — form validation, error rendering, htmx-specific redirects (`HX-Redirect` header).
- **Unit tests** for `internal/kafkatail` (~3 tests) using testcontainers Kafka per existing `tests/harness` pattern (reuse `tests/harness.StartKafka`).
- **Unit tests** for `internal/events/bus.go` (~3 tests): subscribe / publish / unsubscribe / buffered-drop semantics.
- **Smoke test (manual)** in `services/web/README.md`: `make run-web`, then a curl of `/` and `/orders/new` to confirm HTML renders. No automated E2E.
- **The existing `make e2e` testcontainers suite is untouched.** It continues to test the backend end-to-end and provides confidence that the web UI's data sources remain correct.

## Risks & mitigations

1. **Two services in the build pipeline that need to start in the right order** — mitigation: `docker-compose.yml` uses `depends_on: { order: { condition: service_healthy } }` etc., mirroring how `order` already depends on `postgres-order`.
2. **Kafka events for the SSE stream might not include all state transitions** — mitigation: the order detail page also polls directly (`GET /orders/{id}`), so SSE is additive, not load-bearing.
3. **html/template auto-escaping is strict** — any client-supplied JSON strings rendered in templates must go through `template.JS` only when truly safe; otherwise default escaping is the goal, not a problem.
4. **htmx version drift** — pin to `htmx.org@2.0.3` (or current 2.x at implementation time) via SRI hash in the layout template.
5. **Web service binary size / startup time** — `html/template`, `chi`, `pkg/platform`, `pkg/consumer`, `franz-go`. Expected build size ~25 MB, cold start < 1s. Acceptable for a playground.

## Out-of-scope follow-ups (not blocked by this spec)

These are explicitly NOT part of this work; they're noted so they don't sneak in:

- Auth (API key for `/v1/orders` admin endpoint etc.).
- Rate limiting or DDoS protection in front of the web service.
- TLS / reverse proxy / production k8s manifests for `web`.
- Web UI for saga state visualization beyond the events sidebar.
- Replacing the existing curl-based CLI demo with web-driven flow.
- A Grafana-dashboard-style metrics page (Prometheus is already scraped by the platform; a "metrics tab" page in the web UI is a separate design).
