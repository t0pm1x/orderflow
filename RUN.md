# orderflow — Easy run

One-command local stack: 8 infra containers + 5 service binaries, end-to-end runnable on a laptop. For a deeper walkthrough of the codebase see [README.md](README.md); this file is just the "press go" button.

## Quickstart

| OS      | Command                                                                  |
|---------|--------------------------------------------------------------------------|
| Windows | `pwsh -ExecutionPolicy Bypass -File scripts\run.ps1`                      |
| macOS   | `bash scripts/run.sh`                                                    |
| Linux   | `bash scripts/run.sh` (incl. WSL)                                         |

Tear down:

| OS      | Soft (keep volumes)                                  | Hard (wipe Postgres state)                       |
|---------|------------------------------------------------------|--------------------------------------------------|
| Windows | `pwsh -ExecutionPolicy Bypass -File scripts\stop.ps1` | `pwsh -ExecutionPolicy Bypass -File scripts\stop.ps1 -Volumes` |
| macOS/Linux | `bash scripts/stop.sh`                            | `bash scripts/stop.sh --volumes`                 |

Followed by `docker compose -f deploy/docker-compose.yml down` if you also want the compose-managed infra gone (the stop scripts already call `docker compose down`, so this is only needed if you started compose manually).

## Prerequisites

- **Docker Desktop** (or `dockerd` + Compose v2 on Linux). The script checks `docker info` at the start and tells you how to start the daemon if it's down.
- **Go 1.25+** with the workspace (`go.work`) recognized at the repo root. CI uses Go 1.25.13.
- **GNU Make** on PATH. (`make` is on most Linux/macOS systems by default; on Windows install via `choco install make` or use the bundled `make.exe` from Git for Windows.)
- **~4 GB RAM** for the compose stack (postgres x3 + redpanda + observability containers).

## What gets started

### Infra (docker compose, from `deploy/docker-compose.yml`)

| Service          | Port (host) | Notes                                                |
|------------------|-------------|------------------------------------------------------|
| `postgres-order` | 5432        | `db/orderflow/orderflow` creds, db `order_order`     |
| `postgres-payment` | 5433     | db `payment_payment`                                 |
| `postgres-inventory` | 5434   | db `inventory_inventory`                             |
| `redis`          | 6379        | webhook idempotency                                   |
| `redpanda`       | 9092        | Kafka API (advertised host port)                     |
| `kafka-init`     | —           | one-shot, creates `order-events` / `payment-events` / `inventory-events` / `orderflow-dlq` |
| `otel-collector` | 4317, 4318  | OTLP gRPC + HTTP; services default to `OTEL_EXPORTER=stdout` and don't dial it |
| `prometheus`     | 9091        | scrapes services                                      |
| `tempo`          | 3200        | trace storage                                        |
| `grafana`        | 3000        | admin / admin                                        |

### Services (local binaries, started by `scripts/run.{sh,ps1}`)

| Service   | Port | DATABASE_URL                                                                    | KAFKA_BROKER / KAFKA_BROKERS |
|-----------|------|---------------------------------------------------------------------------------|------------------------------|
| `order`   | 8081 | `postgres://orderflow:orderflow@127.0.0.1:5432/order_order?sslmode=disable`     | `127.0.0.1:9092`             |
| `payment` | 8082 | `postgres://orderflow:orderflow@127.0.0.1:5433/payment_payment?sslmode=disable` | `127.0.0.1:9092`             |
| `inventory` | 8083 | `postgres://orderflow:orderflow@127.0.0.1:5434/inventory_inventory?sslmode=disable` | `127.0.0.1:9092`        |
| `saga`    | 8084 | shares `postgres-order` (same DB as the order service)                           | `127.0.0.1:9092`             |
| `web`     | 8085 | n/a (BFF, talks to order/payment/inventory via REST)                             | `127.0.0.1:9092`             |

Inventory also needs `REDIS_URL=redis://127.0.0.1:6379`; web reads `ORDER_URL` / `PAYMENT_URL` / `INVENTORY_URL` to fan out REST calls.

Logs land in `tests/logs/<svc>.log` (stdout + stderr merged for each service). Tail them with any editor or `Get-Content -Wait` / `tail -F`.

## Smoke tests

### Happy path
```bash
# creates an order, returns 201 with order_id
curl -X POST http://127.0.0.1:8081/v1/orders \
     -H 'Content-Type: application/json' \
     -d @examples/order.json

# poll for state; should reach "confirmed" within ~5s
curl http://127.0.0.1:8081/v1/orders/<id-from-201>
```

### Compensation
```bash
curl -X POST http://127.0.0.1:8081/v1/orders \
     -H 'Content-Type: application/json' \
     -d '{"customer_id":"8d2f1a40-cf51-4a8b-8e72-1a4d2c8e6b3f","items":[{"sku":"SKU-001","quantity":1,"unit_price_cents":1999}],"payment":{"last_four":"0001"}}'
```
Last-four `0001` triggers the mock provider's decline branch (`services/payment/internal/provider/provider.go`); the saga emits `StockReleaseRequested` + `OrderCancelled`; final order state is `cancelled`.

### Web UI
Browse to [http://127.0.0.1:8085](http://127.0.0.1:8085) — list, create, cancel, and force-webhook from the sidebar.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `docker daemon is not reachable` (script aborts) | Docker Desktop isn't running | Start it from the system tray (Windows) / `open -a Docker` (macOS) / `sudo systemctl start docker` (Linux) |
| `order_sagas missing in order_order` | The `postgres-order` named volume predates the v1.1.5 saga-migrations fix | `docker compose -f deploy/docker-compose.yml down -v` to wipe the volume, then re-run |
| `infra failed to become healthy in 60s` | Image pull is slow on first run (redpanda is ~700 MB) or CPU is starved by other workloads | re-run the script; on a cold first run the redpanda pull alone can take a couple minutes |
| `port already in use` for `:8081..8085` | A previous `scripts/run.*` invocation left stale binaries behind | run `scripts/stop.*` first (or just kill the offending process) |
| `Kafka topic ... doesn't exist` in service logs | `kafka-init` didn't run before the services booted (shouldn't happen — the script waits for redpanda health before starting services) | `bash scripts/stop.sh && bash scripts/run.sh` to restart cleanly |
| `make build` fails on Windows | GNU Make missing | `choco install make` (Windows) or use `make.exe` from a Git for Windows install |
| Web UI loads with no orders | `web` started before `order` / `payment` / `inventory` finished booting | refresh; the BFF's HTTP probes only check `/healthz` liveness, not the order service's REST readiness |

## How the script handles upgrades

- **Build is always run** unless you pass `-NoBuild` (PowerShell) / `--no-build` (bash). The binary at `bin/order.exe` etc. is what gets launched; re-runs of the script pick up new code immediately.
- **Kafka topics are pre-created** by `kafka-init` on first run and re-checked on subsequent runs (idempotent).
- **Saga migrations** are applied to the order DB on first run and re-checked on subsequent runs (idempotent).
- **Service binaries** are killed before the new ones start, so re-running the script is safe (also handles stale processes from a previous session).

## Related artifacts

- `docs/demo/demo.sh` — scripted end-to-end demo with narration (used to record `docs/demo/RECORDING.md`)
- `scripts/demo*.ps1` — Windows versions of the demo scripts
- `make e2e` — full E2E test suite via testcontainers (CI also runs this on Ubuntu)
- `make load` — k6-driven 100 RPS load test
- `make kind-up` / `make smoke-k8s` — kind-cluster smoke test (separate infra)