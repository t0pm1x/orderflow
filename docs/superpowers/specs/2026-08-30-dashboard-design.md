# orderflow-web — Dashboard + Health spec (Spec #1 of 4)

**Created:** 2026-08-30
**Status:** approved (brainstorming complete; "Делаем")
**Project:** orderflow (`C:\Users\t0p_m\projects\orderflow`)
**Sequence:** Spec #1 of 4 UX passes — Dashboard → Seed → Saga viz → Events sidebar.
**Stack today:** SvelteKit SPA embedded in `services/web` (Go BFF), listening on `:8085`.

## 1. Context

The orderflow playground UI currently redirects `/` to `/orders`, which is a list view, not an at-a-glance overview. A developer or demo viewer who opens `http://localhost:8085` for the first time sees a potentially-empty table with no indication that the platform is even running, no health context, and no obvious "what to do next" affordance. The existing live-events sidebar hints at the system's heartbeat but only after Kafka events have started flowing.

This spec introduces a real dashboard at `/dashboard` that becomes the new landing page. It surfaces three things at once:

1. **System health** — is orderflow itself up, and are all the upstream services reachable?
2. **Activity pulse** — are orders being created? Are sagas succeeding or compensating?
3. **A clear next step** — a path into the rest of the UI even when nothing has happened yet.

This is the first of four planned UX passes. The other three — demo seed + onboarding, saga visualization v2, live events sidebar v2 — are designed to compose with this dashboard but are deliberately not in scope here.

## 2. Goals

1. **`/` lands on `/dashboard`** — the SPA's root redirects to a real overview, not the orders list.
2. **Health is visible at a glance** — five chips (Order, Payment, Inventory, Saga, Kafka tail) with `ok` / `degraded` / `down` state and per-probe latency.
3. **Activity is summarized, not listed** — four KPI tiles (Orders today, Success rate, In-flight, Avg completion) replace "scroll through a table to see what's happening".
4. **Recent orders is a quick list, not the main view** — a compact last-10 table with click-through to detail.
5. **Backend changes stay inside the BFF** — only `services/web/**` is touched. Domain services (order/payment/inventory/saga) are not modified.
6. **Empty state is honest** — when there is no data, the dashboard shows a Welcome card with `+ Create order` primary action and a reserved, disabled slot for the future Seed button.
7. **Degraded state is loud** — if any health probe is `down`, a red banner appears at the top of the page in addition to the chip going red.

## 3. Non-goals (explicitly out of scope for this spec)

- **Demo seed / data generation** — reserved CTA slot only; full implementation is Spec #2.
- **Saga visualization v2** — Spec #3. The order-detail page is not touched here.
- **Live events sidebar v2** — Spec #4. The sidebar keeps its current dense, monochromatic stream for now.
- **Per-service deep links from the health chip** — click only shows a tooltip with latency + probe time; no drill-down yet.
- **Historical charts (orders/day, success rate over time)** — out of scope; would need time-series storage or client-side aggregation across many pages of orders.
- **Real p50/p95 latency** — we report a rough "avg completion" derived from `created_at → completed_at` of orders returned by the recent-orders endpoint. A true p50/p95 requires server-side aggregation.
- **Kafka consumer lag** — would require a Prometheus query against the JMX exporter; not added here.
- **Light theme** — design tokens in `app.css` already support it, but the toggle is not built.
- **Authentication, multi-tenant scoping, RBAC.**
- **Modifications to Order / Payment / Inventory / Saga domain code, OpenAPI, or Helm charts.**
- **New end-to-end tests for the dashboard** — its value is interactive; manual smoke only.
- **Vitest / Playwright setup** — deferred until a later spec actually needs it.

## 4. Approach

**Approach A (chosen): client-side aggregation + BFF health probe.**

The dashboard SvelteKit page does the aggregation itself: it fetches the last 10 orders via the existing `GET /v1/orders?limit=10` and computes the four KPI tiles from that small window. It fetches system health via a new `GET /api/health/all` endpoint that the BFF exposes, which fans out to each upstream service's `/healthz` and reports its own Kafka-tail state. KPI aggregation never goes through a new server endpoint — the dashboard is a pure consumer of existing data plus one new health endpoint.

Why this approach:
- Stays inside the BFF (no domain-service modifications).
- Single new server endpoint, single new client page.
- Latency is a derived metric that needs no new storage or caching.
- Easy to iterate: if a KPI needs more data later (e.g. true p95), we add a server endpoint then.

**Approaches B and C (rejected):**
- **B (BFF-aggregated `/api/dashboard/summary`)** — premature. The current order volume in a playground is tiny; client-side aggregation over 10 rows is free.
- **C (hybrid `?deep=1` mode)** — YAGNI. Re-evaluate if the dashboard ever needs server-side aggregations.

## 5. Architecture

```
┌────────────────────────────────────────────────────────────────────┐
│  services/web/frontend/src/routes/dashboard/+page.svelte  (new)    │
│                                                                    │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │
│  │  KpiTiles    │  │ HealthPanel  │  │ RecentOrders │              │
│  │  (4 cards)   │  │ (5 chips)    │  │ (last 10)    │              │
│  └──────────────┘  └──────────────┘  └──────────────┘              │
│                                                                    │
│  • polling: 5s for health, 2s for orders (visibility-aware)        │
└────────────────┬──────────────────────────────────┬────────────────┘
                 │ HTTP                              │ HTTP
                 ▼                                  ▼
        GET /api/health/all              GET /v1/orders?limit=10
                 │                                  │
┌────────────────▼──────────────────────────────────▼────────────────┐
│  services/web/internal/server/                                      │
│   • probe.go — extends HealthAll handler (parallel probe w/ ctx)   │
│   • api.go   — registers GET /api/health/all                       │
│   • backend/client.go — adds getHealthAll() thin wrapper           │
└────────────────┬───────────────────────────────────────────────────┘
                 │ parallel probes (each with 2s timeout)
                 ▼
   Order :8081/healthz            Payment :8082/healthz
   Inventory :8083/healthz         Saga :8084/healthz
   In-process Kafka tail state
```

## 6. Backend changes (`services/web` only)

### 6.1 `internal/server/probe.go` — extend `HealthAll`

Current state: `probe.go` exists with at least a `/healthz` self-check. Extend it with:

```go
type ServiceHealth struct {
    Status     string `json:"status"`     // "ok" | "degraded" | "down"
    LatencyMS  int64  `json:"latency_ms"`
    TakenAt    string `json:"taken_at"`   // RFC3339
    Detail     string `json:"detail,omitempty"` // error message when not ok
}

type HealthSnapshot struct {
    Order     ServiceHealth `json:"order"`
    Payment   ServiceHealth `json:"payment"`
    Inventory ServiceHealth `json:"inventory"`
    Saga      ServiceHealth `json:"saga"`
    Kafka     ServiceHealth `json:"kafka"` // in-process tail state
    SnapshotAt string       `json:"snapshot_at"`
}

func (h *Handlers) HealthAll(w http.ResponseWriter, r *http.Request)
```

Rules:
- Each upstream probe runs in its own goroutine with `context.WithTimeout(r.Context(), 2*time.Second)`.
- Per-probe latency is measured from request start to response body close.
- Status assignment (in this order, first match wins):
  1. `down` — connection refused, timeout (>2s), HTTP 5xx, or body `{"status":"down"}`.
  2. `degraded` — HTTP 200 AND (latency ≥ 1s) OR body `{"status":"degraded"}`.
  3. `ok` — HTTP 200 AND latency < 1s AND (body absent OR body `{"status":"ok"}`).
- Kafka tail state is read from an in-process struct that already exists (`kafkatail.Tail` exposes a `Healthy()` method; if not, add a tiny accessor). The Kafka chip has only `ok` / `down` — there is no `degraded` middle ground for the in-process consumer.
- Always return HTTP 200 with the snapshot; never 5xx — degraded/down are valid states the dashboard renders.
- Cache the snapshot for 1 second to avoid probe storms if multiple clients hit the endpoint within the same window. (Single user for now, but cheap insurance.)

### 6.2 `internal/server/api.go` — register route

Add `r.Get("/api/health/all", h.HealthAll)` to the existing router. Place alongside the other `/api/*` routes.

### 6.3 No new client wrapper — probe inline

`HealthAll` (in `probe.go`) runs all upstream probes inline via `http.Client.Do` with per-probe `context.WithTimeout`. The BFF does not call itself through `backend.Client` for the dashboard snapshot, so no new wrapper method is needed in `internal/backend/client.go`. Upstream base URLs are read from the same env vars (`ORDER_URL`, `PAYMENT_URL`, `INVENTORY_URL`, `SAGA_URL`) that the existing clients already use — expose them as a small package-level helper (e.g. `var URLs = struct{ Order, Payment, Inventory, Saga string }{...}` set in `cmd/web/main.go`) so the probe and the client stay in lock-step.

### 6.4 `internal/server/probe_test.go` — new tests

Table-driven test using `httptest.NewServer`:
- All upstreams healthy → all `ok`, latencies present.
- One upstream returns 500 → that probe `down`, others `ok`.
- One upstream slow (>2s) → that probe `down` (timeout), others `ok`.
- One upstream connection refused → that probe `down`, others `ok`.
- Kafka tail `Healthy()` returns false → Kafka `down`.

Each row checks the full `HealthSnapshot` JSON shape.

## 7. Frontend changes (`services/web/frontend`)

### 7.1 New route — `src/routes/dashboard/+page.svelte`

Layout (CSS grid):

```
┌─ Topbar (existing) ──────────────────────────────────────────────┐
├─ Health degraded banner (conditional, red, full width) ─────────┤
├─ KPI tiles row (4 cards, equal width) ───────────────────────────┤
│  [Orders today] [Success rate] [In-flight] [Avg completion]     │
├─ Two-column row:                                                   │
│  ┌─ HealthPanel (5 chips + latency tooltip) ─┐ ┌─ RecentOrders ┐│
│  │ Order / Payment / Inventory / Saga / Kafka │ │ last 10 rows  ││
│  │ (click → tooltip with latency + timestamp) │ │ click → /ord/…││
│  └────────────────────────────────────────────┘ └───────────────┘│
├─ Empty state card (conditional, centered) ───────────────────────┤
│  Welcome to OrderFlow                                              │
│  [+ Create order]  [Seed demo data — coming soon, disabled]        │
└────────────────────────────────────────────────────────────────────┘
```

State management:
- `health: HealthSnapshot | null` — populated from `getHealthAll()`, refreshed every 5s.
- `orders: Order[]` — populated from `listOrders({limit: 10})`, refreshed every 2s.
- Derived: `kpis = kpiFromOrders(orders)`; `banner = health && hasDown(health)`.
- `document.visibilityState !== 'visible'` → pause both intervals; resume on `visibilitychange` event.

### 7.2 `$lib/dashboard.ts` — new derivation helpers

```ts
export interface KpiSummary {
  ordersToday: number | null;
  successRatePct: number | null;     // null when no terminal orders
  inFlight: number;                  // pending + reserved
  avgCompletionMs: number | null;    // null when no completed orders today
}

export function kpiFromOrders(orders: Order[]): KpiSummary;
export function hasDown(snap: HealthSnapshot): boolean;
export function statusClass(status: 'ok'|'degraded'|'down'): string;
```

Date comparisons use the browser's local time. "Today" means `created_at >= startOfToday()`.

### 7.3 `$lib/types.ts` — new types

```ts
export interface ServiceHealth {
  status: 'ok' | 'degraded' | 'down';
  latency_ms: number;
  taken_at: string;
  detail?: string;
}
export interface HealthSnapshot {
  order: ServiceHealth;
  payment: ServiceHealth;
  inventory: ServiceHealth;
  saga: ServiceHealth;
  kafka: ServiceHealth;
  snapshot_at: string;
}
```

### 7.4 `$lib/api.ts` — add `getHealthAll()`

```ts
export async function getHealthAll(): Promise<HealthSnapshot>
```

Reuses the existing `apiFetch` helper and `ApiError` machinery.

### 7.5 `src/routes/+page.svelte` — change redirect target

Replace `goto('/orders', ...)` with `goto('/dashboard', ...)`.

### 7.6 `src/routes/+layout.svelte` — extend nav

Current nav: `Orders | Inventory | Payments sim`.
New nav: **`Dashboard` | Orders | Inventory | Payments sim** — Dashboard first.
Active-state logic adds: `class:active={$page.url.pathname === '/' || $page.url.pathname.startsWith('/dashboard')}`.

### 7.7 Empty / error / loading states

- **No orders, all health ok** → centered Welcome card. Primary button `+ Create order` (links to `/orders/new`). Secondary `Seed demo data` rendered as a disabled button with `title="Coming soon — Spec #2"`.
- **No orders, health down** → red banner explaining one or more services are unreachable; KPI tiles show `—` with `title="Service unreachable"`; Welcome card still shown.
- **Health probe fails entirely** (BFF itself unreachable) → full-page banner "Backend unreachable — retry"; KPI tiles grey; recent-orders table hidden.
- **Orders fetch error but health ok** → show a small inline error next to the recent-orders table; KPI tiles fall back to `—`.

## 8. Data flow

```
onMount(dashboard page):
  ├─ refreshHealth()          // GET /api/health/all
  ├─ refreshOrders()          // GET /v1/orders?limit=10
  ├─ setInterval(refreshHealth, 5_000)        if visible
  └─ setInterval(refreshOrders, 2_000)        if visible

document.addEventListener('visibilitychange'):
  visible → resume both intervals (immediate fetch + restart timers)
  hidden  → clear both intervals

cleanup onDestroy:
  clearInterval(health)
  clearInterval(orders)
```

KPI recomputation is reactive: `let kpis = $derived(kpiFromOrders(orders))`. The derivation runs whenever `orders` is reassigned.

## 9. Error handling

| Surface | Behavior |
|---|---|
| One upstream probe times out | That service's chip is `down` (red); others unaffected; dashboard continues rendering. |
| All upstream probes fail | Banner: "All upstream services unreachable"; KPI tiles empty; recent-orders query also likely fails → show its own error inline. |
| BFF `/api/health/all` itself returns 5xx | Dashboard treats as `Backend unreachable` full-page banner with manual retry button. |
| `/v1/orders` returns 401/403 (shouldn't happen — no auth) | Treat as `Backend unreachable` (same banner). |
| `/v1/orders` returns 5xx | Small inline error next to recent-orders table; KPI tiles stay on last known value with `stale` tooltip. |
| Kafka tail reports `down` | Kafka chip red; everything else continues. |
| Network offline (browser-side) | Both fetches reject → both fall into their respective error paths. |

## 10. KPI formulas

| KPI | Formula | When null |
|---|---|---|
| Orders today | `orders.filter(o => new Date(o.created_at) >= startOfToday()).length` | Never (0 is valid) |
| Success rate | `confirmed.length / (confirmed.length + cancelled.length + failed.length) * 100` over terminal orders in the window | When no terminal orders in the window |
| In-flight | `orders.filter(o => o.state === 'pending' || o.state === 'reserved').length` | Never (0 is valid) |
| Avg completion | `mean(completed_at − created_at in ms)` over orders in the window where `completed_at` is set | When no completed orders in the window |

All four KPIs are derived from the `limit=10` window returned by the BFF. "Today" means `created_at >= startOfToday()` in the browser's local time. Success rate and avg completion use the full window (terminals / completed within the 10 most recent orders), not today-only, because a 10-row sample filtered to today is usually empty in a playground. This is intentionally approximate. Spec for true aggregates is deferred.

## 11. Health semantics

| Chip state | Trigger |
|---|---|
| `ok` (green) | HTTP 200, latency < 1s, body (if any) reports `status:"ok"`. |
| `degraded` (warn/yellow) | HTTP 200 with latency ≥ 1s, OR body reports `status:"degraded"`. Not used for the Kafka chip. |
| `down` (red) | Connection refused, timeout (>2s), HTTP 5xx, or body reports `status:"down"`. Also used for the Kafka chip when `kafkatail.Tail.Healthy() == false`. |

Tooltip on click shows `latency_ms` and `taken_at` (RFC3339).

If any chip is `down`, the dashboard shows a top banner: `"<N> service(s) unreachable: <names>"`. The Kafka chip never triggers `degraded`; it is binary `ok | down`.

## 12. Testing

### Backend (Go)
- `make test` runs the BFF test suite; new tests in `internal/server/probe_test.go` must pass.
- Coverage: `ok + down + timeout + 5xx + kafka-down + all-up + snapshot-shape` (≥6 table rows).
- `go vet ./...` clean; `golangci-lint run ./services/web/...` clean (matches CI).

### Frontend
- Manual smoke checklist documented in the implementation plan:
  1. `make run-web` after `bash scripts/run.sh`.
  2. Visit `/` → redirects to `/dashboard`.
  3. All chips green, KPI tiles show `0` or `—` on a fresh DB.
  4. Create an order via `/orders/new`; return to `/dashboard`; KPI "Orders today" ticks to 1; recent-orders row appears.
  5. Force-fail the payment via `/payments/sim`; "Success rate" eventually appears.
  6. Stop the Order service; within 5s the Order chip turns red; banner appears.
  7. Hide the tab; wait 30s; show tab again; verify no console errors and timers resumed.
  8. Reload with all services down; verify "Backend unreachable" full-page state.

### Out of test scope
- Vitest / Playwright / new e2e harness.
- Visual regression / snapshot testing.
- Load testing the dashboard.

## 13. Risks & open questions

1. **Probe caching** — 1-second cache is enough for a single developer; if multiple browser tabs hit `/api/health/all` simultaneously, consider raising it. Not a real concern for now.
2. **Health semantics for individual services** — we need to confirm each upstream's `/healthz` actually returns `{"status":"..."}` payload vs. plain 200. If they only return HTTP status, the `degraded` state is unreachable; `HealthAll` collapses to `ok | down`. Spec assumes the former but is safe with the latter.
3. **KPI window of 10** — success rate and avg completion are unreliable until 10+ terminal orders have accumulated. Tile UI shows `—` in that window; this is acceptable for a playground.
4. **Visibility-aware polling** — depends on the browser supporting `visibilitychange`. All evergreen browsers do; no polyfill needed.
5. **Nav order change** — adding Dashboard as the first nav item is a small UX regression for muscle memory of users who already know `/orders` is the default landing. Acceptable trade-off because the redirect is silent and `Orders` remains one click away.

## 14. Out of scope reminders for downstream specs

- **Spec #2 (Demo seed + onboarding):** implements the disabled "Seed demo data" button on the dashboard. Likely a `POST /api/demo/seed` BFF endpoint that creates a deterministic mix of pending/reserved/confirmed/cancelled orders.
- **Spec #3 (Saga visualization v2):** the saga timeline at `/orders/[id]` becomes a spatial state-machine view with compensation flow and per-step latency. Independent of this spec.
- **Spec #4 (Live events sidebar v2):** the existing sidebar in `+layout.svelte` becomes filterable, click-through, with payload drill-down. Independent of this spec.

These specs compose — they share components and types — but each is its own brainstorming → spec → plan → implementation cycle.

## 15. Definition of done

- `services/web` builds (`make build`), tests pass (`make test`), lint clean (`make lint`).
- `make run-web` followed by visiting `/` lands on `/dashboard` showing health + KPIs + recent orders.
- All five health chips reflect actual upstream state within 5 seconds of a service starting or stopping.
- The empty-state Welcome card appears on a fresh DB; the disabled "Seed demo data" slot is visible.
- The degraded banner appears within 5 seconds of any service becoming unreachable.
- Visibility-aware polling stops and resumes without console errors.
- No changes outside `services/web/**`. Verified with `git diff --stat origin/main` after the work is committed.
- A short demo runbook entry is added to `RUN.md` (one paragraph + curl example for `/api/health/all`).