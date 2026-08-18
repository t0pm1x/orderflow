# orderflow-web

Tactile playground UI for the orderflow platform. Server-rendered HTML
(`html/template`) + a sprinkle of `htmx` for progressive enhancement.

## Run locally (against already-running services)

```powershell
cd services\web
go run .\cmd\web
# → listens on :8083 by default
```

Override with env: `HTTP_ADDR=:9090 ORDER_URL=http://orders.example.com:8080 ...`.

## Run via docker-compose

```powershell
docker compose -f deploy/docker-compose.yml up web
```

The compose `web` service depends on `order`, `payment`, `inventory`
being healthy.

## Smoke recipe

```powershell
curl http://localhost:8083/healthz                       # → {"status":"ok"}
curl http://localhost:8083/                              # → orders list HTML
curl -X POST http://localhost:8083/v1/orders -d "sku=SKU-001&quantity=2"
# → 200 + HX-Redirect: /orders/<id>
```

Open <http://localhost:8083> in a browser.

## Architecture

See `docs/superpowers/specs/2026-08-18-orderflow-web-design.md`.
