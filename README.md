# orderflow

> Event-driven order processing platform. 3 microservices (Order, Payment, Inventory), Postgres per service, Kafka (Redpanda) events, Redis reservations, saga pattern, outbox pattern, OpenTelemetry tracing.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## Status: v0.4.0

This release adds Payment Service webhook handler + PGRepository (3.5.g), Inventory Service PGRepository + REST stock endpoint (3.6.g), and `make e2e` aggregate target + harness.RestartKafka helper (3.11.polish). All 4 services now have PGRepositories. v0.3.0 features remain.

### ✅ What works
- All 4 services (`order`, `payment`, `inventory`, `saga`) compile and produce binaries
- `pkg/platform` library: logging (with OTel trace correlation), middleware, types (Money, IDs), events envelope, typed errors, W3C tracecontext propagation
- Order Service: full domain, REST API (`POST /v1/orders`, `GET /v1/orders/{id}`, `GET /v1/orders`), **PGRepository** (tx-atomic with outbox), outbox writer, PGSource, DB migrations
- Payment Service: mock provider, **`POST /v1/payments/webhook`** with idempotency middleware, **PGRepository** (tx-atomic UpdateStatus + outbox), outbox writer, DB migrations
- Inventory Service: Stock model with optimistic locking, **`GET /v1/inventory/stock/{sku}`**, **PGRepository** (GetStock/ReserveStock/ReleaseStock), outbox writer, DB migrations
- Saga Service: state machine, compensation, watchdog, DB migrations
- Outbox poller + KafkaPublisher + KafkaDLQ + Prometheus metrics
- Consumer base: franz-go, idempotent handler, DLQ on error, per-service handler registries
- Docker-compose stack: 3 Postgres, Redis, Redpanda, OTel Collector, Prometheus, Tempo, Grafana
- K8s base + Helm charts for all 4 services + 3 infra deps + values-dev overlays
- Kustomize overlays (dev/staging/prod) with HPA + PDB for prod
- ArgoCD ApplicationSet with AppProject RBAC
- 4 ADRs + 5 C4 diagrams
- testcontainers-go harness + E2E tests (happy + compensation + chaos) + k6 load test + `make e2e` aggregate
- kind cluster config + `make kind-up/down/load`
- Demo script

### ⬜ Deferred to v0.5.0
- kind smoke test (requires `kind` binary installation)
- asciinema recording of the demo
- Full outbox-retry chaos assertion (services cache `KAFKA_BROKER` at startup)

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
