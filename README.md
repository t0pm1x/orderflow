# orderflow

> Event-driven order processing platform. 3 microservices (Order, Payment, Inventory), Postgres per service, Kafka (Redpanda) events, Redis reservations, saga pattern, outbox pattern, OpenTelemetry tracing.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## Status: v0.5.0

This release wires up the **saga runtime** and all **consumer handlers** — the platform now actually processes orders end-to-end. Plus LDFLAGS injection (binaries report real git version) and saga go.sum housekeeping.

### ✅ What works
- All 4 services (`order`, `payment`, `inventory`, `saga`) compile and produce binaries
- **Full event flow**: `POST /v1/orders` → OrderCreated outbox → saga starts → StockReserveRequested → inventory reserves → StockReserved → saga → PaymentRequested → payment charges → PaymentCompleted → saga → OrderConfirmed → order state=confirmed
- `pkg/platform` library: logging (with OTel trace correlation), middleware, types, events envelope, typed errors, W3C tracecontext propagation
- All 4 services have PGRepository, REST API endpoints, real consumer handlers
- Saga runtime: consumer + outbox + repository + state machine + compensation
- Outbox poller + KafkaPublisher + KafkaDLQ + Prometheus metrics
- Consumer base: franz-go, idempotent handler, DLQ on error
- Docker-compose stack + Helm charts + Kustomize overlays + ArgoCD manifests
- testcontainers-go harness + E2E tests (happy + compensation + chaos) + k6 load test + `make e2e`
- 4 ADRs + 5 C4 diagrams + demo script
- Binaries report real version (LDFLAGS injection via `git describe`)

### ⬜ Deferred to v0.6.0
- kind smoke test (requires `kind` binary installation)
- asciinema recording of the demo
- Saga watchdog cross-restart TTL sweep
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
