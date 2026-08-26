# orderflow-web

SvelteKit SPA + thin Go BFF. The single binary serves:

- the embedded SvelteKit SPA (built into `frontend/dist/` via `//go:embed`)
- `/api/*` JSON proxy to the four backend services (`order`, `payment`, `inventory`, `saga`)
- `/events/stream` SSE bridge from the in-process event bus (populated by the Kafka tail goroutine)
- `/healthz`, `/readyz` probes

```
Browser (SvelteKit SPA, :8085 same-origin)
   |
   | fetch /api/orders            ─┐
   | fetch /api/orders/{id}        │
   | POST /api/orders             ─┼─> Go BFF (services/web)
   | DELETE /api/orders/{id}       │      ├─> order:8081
   | GET /api/inventory/stock/{sku}│      ├─> payment:8082
   | POST /api/payments/webhook    │      └─> inventory:8083
   | GET /events/stream (SSE)     ─┘      (kafkatail → in-process bus → SSE)
```

## Why a SPA (and not htmx)?

Pre-v1.1.7 the UI was server-rendered Go templates + htmx 2.0.3 +
a hand-rolled inline JS bootstrap. The architecture was prone to
two classes of bugs that surfaced repeatedly:

1. **DOMContentLoaded races** — the inline `<script>` in `<head>`
   ran before `document.body` existed; an exception in one
   listener aborted every subsequent listener and partially
   broke htmx's swap-target cache, so every link/form click
   appeared to do nothing while manual URL entry worked.
2. **htmx-sse version skew** — the vendored SSE plugin was the
   htmx-1.x build and used a removed internal API; a runtime
   warning fired on every page load and the live-event sidebar
   silently never received messages.

A SvelteKit SPA side-steps both: htmx is gone, the SPA is a
real client with a real event loop, and SSE is just a normal
`EventSource` in `lib/sse.ts`. Type-safe TypeScript catches the
class of "typo in attribute name" / "wrong JSON field" bugs at
build time that the old template layer couldn't.

## Build

Prerequisites:

| Tool | Version |
|------|---------|
| Go   | 1.25+ (matches the rest of the orderflow workspace) |
| Node.js | 20+ (only for building the SPA; runtime doesn't need it) |
| npm  | bundled with Node.js |

Build the single binary:

```sh
make web-frontend-install   # one-time: cd services/web/frontend && npm ci
make web-frontend-build     # cd services/web/frontend && npm run build
make web-build              # go build with the SPA embedded
```

The artifacts:

- `services/web/frontend/dist/` — SvelteKit static build
  (committed placeholder `index.html`, real output from `npm
  run build`; gitignored for `_app/` and any code-split chunks
  that would otherwise churn git history on every build).
- `bin/web.exe` (or `bin/web` on POSIX) — single binary, embeds
  the SPA + serves `/api/*` + `/events/stream`.

A fresh checkout where `make web-frontend-build` hasn't been run
yet still builds the Go binary, but the SPA fallback page returns
"SPA not built yet — run `make web-frontend-build`" and only the
`/api/*` + `/events/stream` + probes work. The Go server logs a
warning at startup so operators see what's missing.

## Dev workflow

Two-process dev (Vite + Go):

```sh
# terminal 1: Go backend (proxies + SSE + serves SPA placeholder)
go run ./cmd/web

# terminal 2: Vite SPA dev server with HMR
cd services/web/frontend && npm run dev
```

Open http://localhost:5173 — Vite serves the SPA with HMR and
proxies `/api/*` + `/events/stream` to the Go backend on
:8085 (configured in `vite.config.ts`). The Go binary only
needs to be restarted when `cmd/web/main.go` or `internal/...`
changes; SPA edits hot-reload via Vite.

## Routes (Go BFF)

| Method | Path                          | Notes |
|--------|-------------------------------|-------|
| GET    | `/healthz`                    | liveness |
| GET    | `/readyz`                     | readiness (always 200; Kafka tail not required) |
| GET    | `/api/orders`                  | proxy `Order.List`; SKU filter is client-side |
| GET    | `/api/orders/{id}`             | proxy `Order.Get`; 400 on invalid UUID |
| POST   | `/api/orders`                  | proxy `Order.Submit`; BFF-level replay guard via `idempotency_key` |
| DELETE | `/api/orders/{id}`             | proxy `Order.Cancel`; idempotent (404 → 204) |
| GET    | `/api/inventory/stock/{sku}`   | proxy `Inventory.GetStock` |
| POST   | `/api/payments/webhook`        | proxy `Payment.FireWebhook` |
| GET    | `/events/stream`               | SSE from in-process bus; 503 if Kafka disabled |
| GET    | `/_app/*`                      | SvelteKit code-split assets |
| GET    | `/static/*`                    | SvelteKit static dir (favicon) |
| GET    | `/favicon.svg`                 | favicon |
| GET    | `/` + `/*` (fallback)          | SPA `index.html` for client-side routes |

## SPA routes (SvelteKit)

| Path                       | Notes |
|----------------------------|-------|
| `/`                        | redirect → `/orders` |
| `/orders`                  | list + state/SKU filter chips |
| `/orders/new`              | form, `?prefill=happy\|fail` shortcuts |
| `/orders/{id}`             | detail + timeline + cancel button |
| `/inventory`               | per-SKU stock + clickable SKUs |
| `/payments/sim`            | in-flight orders + force succeed/fail with error_code selector |

State filtering on `/orders` is URL-driven (`?state=...&sku=...&sku=...`)
so back-navigating preserves the filter context. The SPA
subscribes to `/events/stream` once on layout mount and shares
the event list with the timeline view via a `writable` store.

## Tests

```sh
make -C services/web test
```

Go tests cover the API-gateway handlers via httptest with fake
backend clients (`internal/server/api_test.go`). Svelte UI is
tested implicitly via integration with the Go handlers — there
are no separate `npm test` runs for the SPA today. Add Svelte
component tests with `vitest` + `@testing-library/svelte` when
the SPA grows beyond the 5 routes it covers now.

## Architecture decisions

- **No CORS.** SPA hits same-origin `/api/*`; the Go BFF proxies
  to the backend services. Backend URLs stay server-side secrets
  and the SPA doesn't need any CORS config on the backends.
- **Single binary.** The SPA is embedded into the Go binary via
  `//go:embed`; no separate static-file container, no CDN, no
  asset-cache invalidation problems. Operators ship one image.
- **Kafka tail stays in Go.** Native Kafka client is much simpler
  than a Node port, and the SSE bridge (EventSource → Svelte
  store) is one HTTP handler.
- **Replay guard at the BFF, not upstream.** Per-process map keyed
  by `idempotency_key`; sufficient for the playground (single
  replica). Multi-replica deployment would move this to Redis
  (mirroring the pattern in `services/payment/internal/idempotency/`).
