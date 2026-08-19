# orderflow

> Event-driven order processing platform. 3 microservices (Order, Payment, Inventory), Postgres per service, Kafka (Redpanda) events, Redis reservations, saga pattern, outbox pattern, OpenTelemetry tracing.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## Status: v1.2.0

**First stable release.** The platform is end-to-end functional, saga recovery is wired, deployment is GitOps-ready, and CI is in place. Demo script + recording runbook included.

### ✅ What works
- All 4 services (`order`, `payment`, `inventory`, `saga`) compile + run with PGRepository + REST API + real consumer handlers
- **Full event flow**: `POST /v1/orders` → OrderCreated → saga → StockReserveRequested → inventory reserves → StockReserved → saga → PaymentRequested → payment → PaymentCompleted → saga → OrderConfirmed → order=confirmed
- **Saga recovery**: cross-restart TTL sweep compensates stuck sagas
- W3C tracecontext through Kafka (kafkaprop + outbox + consumer + chi middleware)
- Helm charts for all 4 services + 3 infra deps
- Kustomize overlays (dev/staging/prod) with HPA + PDB for prod
- ArgoCD ApplicationSet for GitOps delivery
- testcontainers harness + E2E tests (happy + compensation + chaos) + k6 load test
- kind smoke test (`make smoke-k8s`) + cluster config (`make kind-up/down/load`)
- CI: build matrix + E2E job (ubuntu-only, `needs: build`)
- 4 ADRs + 5 C4 diagrams + demo script + asciinema recording runbook
- Binaries report real git version (LDFLAGS injection)

### Quickstart
```bash
# Easy run — full stack on your laptop in one command (requires Docker + Go + Make)
bash scripts/run.sh                                                       # macOS / Linux / WSL with a real Linux distro
powershell -ExecutionPolicy Bypass -File scripts\run.ps1                  # Windows (PowerShell 5.1 default; or `pwsh ...` if you have 7+)
# tear down:  bash scripts/stop.sh  /  powershell scripts\stop.ps1

# Narrated demo (scripted happy-path with live event tail in the web UI)
bash docs/demo/demo.sh                                                    # macOS / Linux / WSL
powershell -ExecutionPolicy Bypass -File scripts\run-demo.ps1             # Windows (uses Git Bash at C:\Program Files\Git\bin\bash.exe)

# E2E test suite (requires docker)
make e2e

# Build all 5 binaries with version injection
make build

# k8s smoke (requires kind + docker)
make smoke-k8s

# Demo recording (requires asciinema)
make record
```

> **Windows + bash:** `bash` on a default Windows PATH routes through
> the WSL shim, which only works if a real Linux distro is installed
> (the docker-desktop WSL2 backend alone is not enough). If you have
> Git Bash installed (`C:\Program Files\Git\bin\bash.exe`) it's used by
> `scripts/run-demo.ps1` automatically; for one-off bash invocations
> call it by full path or add Git's `bin` to your PATH. PowerShell
> scripts (`scripts\run.ps1`, `scripts\run-demo.ps1`) have no such
> dependency.

See [RUN.md](RUN.md) for prerequisites, what gets started, smoke-test
curl commands, and the troubleshooting matrix. The easy-run script
brings up the same infra as `docs/demo/demo.sh` but wraps the
`docker compose up` + `make build` + 5-binary launch into a single
command with healthchecks.

### Web playground (optional)

After the easy-run script (or `bash docs/demo/demo.sh` /
`powershell scripts\run-demo.ps1`), the orderflow-web UI is also
available at [http://localhost:8085](http://localhost:8085) — list
orders, create new ones, fire a forced-fail payment webhook, and
watch `order-events` arrive in the sidebar.

Build it on its own: `make run-web` (requires Order/Payment/Inventory
services to already be running on :8081/:8082/:8083).

### Local verification (pre-push)

Run the same checks CI runs, locally:

    make verify

Runs `tidy` (go mod tidy per-module), `build` (5 binaries with version injection), `test` (all 13 workspace modules), and `lint` (golangci-lint, requires v2.x locally — matches the GitHub Actions version). Catches most issues before pushing.

### ⬜ Deferred to v1.1
- Full outbox-retry chaos assertion (services cache `KAFKA_BROKER`)
- kind smoke: actual image loading into cluster (currently validates Helm rendering only)
- ghcr.io publishing pipeline

## Architecture

3 microservices + saga coordinator, communicating via Kafka topics:

```
              ┌────────────┐
              │  Client    │
              └─────┬──────┘
                    │ REST/gRPC
        ┌───────────▼───────────┐
        │    Order Service       │
        └────┬──────────────┬────┘
             │              │
             │ Events       │
             ▼              ▼
       ┌──────────┐  ┌──────────┐
       │ Payment  │  │Inventory │
       └────┬─────┘  └────┬─────┘
            │             │
       ┌────▼─────┐  ┌────▼─────┐
       │Postgres  │  │Postgres  │
       │(payment) │  │(inventory)│
       └──────────┘  └────┬─────┘
                          │
                    ┌─────▼─────┐
                    │   Redis   │
                    │(reserv.)  │
                    └───────────┘

        ┌────────────────────────────────────┐
        │     Kafka / Redpanda               │
        │  topics:                           │
        │   - order-events                   │
        │   - payment-events                 │
        │   - inventory-events               │
        └────────────────────────────────────┘

        ┌────────────────────────────────────┐
        │     Observability                  │
        │   Prometheus + Grafana + Tempo     │
        │   OTel Collector                   │
        └────────────────────────────────────┘
```

See [`docs/architecture/c4-level-2.puml`](docs/architecture/c4-level-2.puml) for container-level detail.

## Stack

- **Go 1.25.13**
- **Kafka:** Redpanda (single binary, Kafka API compatible)
- **HTTP:** chi
- **DB:** PostgreSQL 16 (one per service)
- **Cache:** Redis 7
- **Tracing:** OpenTelemetry (OTLP)
- **Workspace:** `go.work` (1 platform module + 4 service modules + 5 cmd stubs; `services/web` hosts the orderflow-web playground UI)

## Building

```bash
# Build all 5 binaries
make build
# → bin/order, bin/payment, bin/inventory, bin/saga, bin/web

# Run a single service
make run-order    # → POST /v1/orders on :8081
make run-payment  # → :8082
make run-inventory # → :8083
make run-saga     # → :8084

# Test
make test
```

## Project structure

```
orderflow/
├── cmd/{order,payment,inventory,saga,web}/  # service entry points
├── pkg/platform/          # shared library (logging, otel, types, events, errors)
├── services/
│   ├── order/             # Order Service (most complete)
│   ├── payment/           # Payment Service (mock provider only)
│   ├── inventory/         # Inventory Service (stock model only)
│   ├── saga/              # Saga orchestrator
│   └── web/               # Orderflow-web playground UI (server-rendered HTML + htmx)
├── api/openapi.yaml        # REST API contract
├── deploy/
│   ├── docker-compose.yml  # full platform stack
│   ├── postgres/           # per-service init scripts
│   ├── kafka/              # redpanda topic init
│   ├── observability/      # prometheus, tempo, grafana, otel-collector
│   └── k8s/base/           # namespace, rbac, network-policies
├── docs/
│   ├── architecture/       # C4 diagrams (PlantUML)
│   ├── adr/                # ADRs
│   └── superpowers/        # specs, portfolio (substages doc)
├── tests/                  # E2E/chaos/load (planned)
└── examples/               # (planned)
```

## ADRs

- [0001-saga-vs-choreography.md](docs/adr/0001-saga-vs-choreography.md) — Saga orchestration over choreography
- [0002-outbox-pattern.md](docs/adr/0002-outbox-pattern.md) — Transactional outbox + Kafka EOS
- [0003-rest-vs-grpc.md](docs/adr/0003-rest-vs-grpc.md) — REST external + gRPC internal
- [0004-w3c-tracecontext.md](docs/adr/0004-w3c-tracecontext.md) — W3C tracecontext propagation through Kafka

## Decision log

| #    | Title                                | Status   | Date       |
|------|--------------------------------------|----------|------------|
| 0001 | Saga vs choreography                 | Accepted | 2026-08-17 |
| 0002 | Outbox pattern                       | Accepted | 2026-08-17 |
| 0003 | REST vs gRPC                         | Accepted | 2026-08-17 |
| 0004 | W3C tracecontext through Kafka       | Accepted | 2026-08-17 |

## License

MIT
