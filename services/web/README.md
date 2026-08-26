# orderflow-web

Tactile playground UI for the orderflow platform. Server-rendered HTML
(`html/template`) + a sprinkle of `htmx` (vendored, offline-capable) for
progressive enhancement, plus Server-Sent Events for live saga telemetry.

## Quick start (against the full platform)

The one-command launcher brings the playground up alongside order / payment /
inventory / saga:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\run.ps1
```

Browse to **<http://127.0.0.1:8085>**.

The launcher pins the web binary to host port `:8085` (see
`scripts/run.ps1`: `$ports = @{ ...; web=8085 }`). All other services use
their standard ports — order `:8081`, payment `:8082`, inventory `:8083`,
saga `:8084` — so the playground can sit on `:8085` without colliding with
anything upstream.

The same flow on POSIX shells:

```bash
./scripts/run.sh
```

For the end-to-end demo with deterministic Kafka traffic, see
[`scripts/run-demo.ps1`](../../scripts/run-demo.ps1) (orchestrates
`docs/demo/demo.sh` in the background, polls until ready, prints the URL).

## Standalone run (against already-running services)

```powershell
cd services\web
go run .\cmd\web
# → listens on :8083 by default (this is the bare-binary default; in the
#   full-platform run above the launcher overrides HTTP_ADDR to :8085 to
#   avoid colliding with the inventory service, which also binds :8083)
```

Override with env: `HTTP_ADDR=:9090 ORDER_URL=http://...:8081 PAYMENT_URL=http://...:8082 INVENTORY_URL=http://...:8083 KAFKA_BROKERS=localhost:9092`.
Leave `KAFKA_BROKERS` empty to disable the live event tail (the SSE stream
will simply stay quiet; everything else still works).

## Pages

| Path | Purpose |
|------|---------|
| `/` | Orders list (htmx auto-refreshes every 2 s) |
| `/orders/new` | Create-order form |
| `/orders/{id}` | Order detail + saga timeline (htmx auto-refreshes every 1 s while non-terminal) |
| `/orders/{id}/events` | Order events timeline fragment (htmx polling target used by the detail page) |
| `/inventory` | Per-SKU stock viewer (htmx auto-refreshes every 3 s) |
| `/payments/sim` | Force-success / force-fail webhook simulator |
| `/events/stream` | SSE stream of Kafka events (`text/event-stream`; consumed by the live-event sidebar via the vendored `htmx-sse.js` extension) |
| `/healthz` | Liveness |
| `/readyz` | Readiness (parallel `GET /healthz` probes against order, payment, and inventory upstreams — all three must answer within 2 s) |
| `/static/*` | CSS + vendored `htmx.min.js` + vendored `htmx-sse.js` |

## Actions

| Method | Path | Effect |
|--------|------|--------|
| POST | `/v1/orders` | Submit a new order. On success returns `HX-Redirect: /orders/{id}` so htmx navigates to the detail page; the form also carries a server-issued `Idempotency-Key` for double-submit protection. |
| POST | `/v1/orders/{id}` | Cancel a non-terminal order (`DELETE` proxied upstream). |
| POST | `/payments/sim/fire` | Fire a synthetic payment webhook with a chosen outcome (success / fail) against a chosen order. |

## Architecture

See [`docs/superpowers/specs/2026-08-18-orderflow-web-design.md`](../../docs/superpowers/specs/2026-08-18-orderflow-web-design.md).

Highlights:

- Single chi router. Page handlers (`internal/handlers`) and probes / static
  (`internal/server`) are composed in `internal/web.Main()`.
- Templates live in `internal/templates/` and are embedded via `//go:embed`
  so the binary is self-contained — no template path needed at runtime.
- `htmx` (and the SSE extension) are vendored under `internal/static/vendor/`
  and served from `/static/*`, so the playground works offline.
- The Kafka event tail (`internal/kafkatail`) fans Kafka events into an
  in-process bus (`internal/events`) that `PageEventsStream` drains over SSE.

### Two-layer binary layout

The web binary is built from **two** `cmd/web` modules by design:

| Layer | Path | Package | Purpose |
|-------|------|---------|---------|
| Outer | [`cmd/web`](../../cmd/web/main.go) | `package main` | Tiny `main()` that calls the inner `Main()`. Listed in `go.work` so the outer binary is buildable in isolation; this is what `make build` (root Makefile) compiles into `bin/web.exe`. |
| Inner | [`cmd/web`](./cmd/web/main.go) (this dir) | `package web` | Owns the `Main()` exported function and the `Version` variable (`-ldflags -X`). Lives next to the service's `internal/*` packages so it can import `services/web/internal/web` directly. The Release story (SIGTERM-aware shutdown, structured startup log, env overrides) is implemented here. |

The outer `package main` is a one-line delegation so a single `go build ./cmd/web`
produces a working binary without exporting internal types from the service
package. The same pattern is used by `cmd/{order,payment,inventory,saga}` —
each has a 10-line `main.go` that imports its service's `Main()` and calls it.

## Smoke

After `scripts\run.ps1` (or `scripts\run-demo.ps1`):

```powershell
powershell -ExecutionPolicy Bypass -File scripts\smoke-web.ps1
```

Asserts happy path + compensation + 4xx + 5xx. The smoke script defaults
`WebUrl` to `http://127.0.0.1:8085` (override with `-WebUrl`).