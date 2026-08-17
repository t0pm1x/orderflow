# orderflow

> Event-driven order processing platform. 3 microservices (Order, Payment, Inventory), Postgres per service, Kafka (Redpanda) events, Redis reservations, saga pattern, outbox pattern, OpenTelemetry tracing.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## Status: v1.0.0

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
# Local happy-path demo (requires docker + 8GB RAM)
bash docs/demo/demo.sh

# E2E test suite (requires docker)
make e2e

# Build all 4 binaries with version injection
make build

# k8s smoke (requires kind + docker)
make smoke-k8s

# Demo recording (requires asciinema)
make record
```

### Local verification (pre-push)

Run the same checks CI runs, locally:

    make verify

Runs `tidy` (go mod tidy per-module), `build` (4 binaries with version injection), `test` (all 13 workspace modules), and `lint` (golangci-lint, requires v2.x locally — matches the GitHub Actions version). Catches most issues before pushing.

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
- **Workspace:** `go.work` (1 platform module + 3 service modules + 4 cmd stubs)
- **Migrations:** goose (planned, not yet wired)

## Building

```bash
# Build all 4 binaries
make build
# → bin/order, bin/payment, bin/inventory, bin/saga

# Run a single service
make run-order    # → POST /v1/orders on :8080
make run-payment
make run-inventory
make run-saga

# Test
make test
```

## Local development (planned, not yet wired)

```bash
docker compose -f deploy/docker-compose.yml up
```

This brings up postgres ×3, redis, redpanda (KRaft), kafka-init (creates 4 topics), otel-collector, prometheus, tempo, grafana. **Not yet functional** — services don't connect to it.

## Project structure

```
orderflow/
├── cmd/{order,payment,inventory,saga}/  # service entry points
├── pkg/platform/          # shared library (logging, otel, types, events, errors)
├── services/
│   ├── order/             # Order Service (most complete)
│   ├── payment/           # Payment Service (mock provider only)
│   ├── inventory/         # Inventory Service (stock model only)
│   └── saga/              # Saga orchestrator (stub)
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

## Decision log

| #    | Title                                | Status   | Date       |
|------|--------------------------------------|----------|------------|
| 0001 | Saga vs choreography                 | Accepted | 2026-08-17 |
| 0002 | Outbox pattern                       | Accepted | 2026-08-17 |
| 0003 | REST vs gRPC                         | Accepted | 2026-08-17 |
| 0004 | W3C tracecontext through Kafka       | Accepted | 2026-08-17 |

## License

MIT
