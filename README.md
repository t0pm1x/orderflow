# orderflow

> Event-driven order processing platform. 3 microservices (Order, Payment, Inventory), Postgres per service, Kafka (Redpanda) events, Redis reservations, saga pattern, outbox pattern, OpenTelemetry tracing.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## Status: v0.2.0

This release closes stages 3.10 (tracing), 3.11.a (E2E harness), 3.12 (Helm + kind), 3.13 (docs + demo). Tracing, outbox, saga, and migrations from v0.1.0-MVP remain. E2E individual tests, Kustomize, ArgoCD, and asciinema deferred to v0.3.0.

### ✅ What works
- All 4 services (`order`, `payment`, `inventory`, `saga`) compile and produce binaries
- `pkg/platform` library: logging (with OTel trace correlation), middleware, types (Money, IDs), events envelope, typed errors, W3C tracecontext propagation
- Order Service: full domain (state machine: pending→reserved→confirmed/cancelled/failed), REST API (POST/GET/LIST), outbox writer, PGSource, DB migrations
- Payment Service: mock provider with deterministic success/decline/insufficient-funds/timeout; outbox writer; Redis-backed idempotency; DB migrations
- Inventory Service: Stock model with optimistic locking version column, reserve/release, outbox writer, DB migrations
- Saga Service: state machine (initiated → stock_reserved → completed/compensated), compensation, watchdog, DB migrations
- Outbox poller: FetchPending / MarkSent / MarkFailed with retry + per-topic DLQ + Prometheus metrics; KafkaPublisher and KafkaDLQ adapters
- Consumer base: franz-go consumer group, idempotent handler wrapper (event_id dedupe), DLQ on error, per-service handler registries
- Docker-compose stack: 3 Postgres, Redis, Redpanda (KRaft), OTel Collector, Prometheus, Tempo, Grafana
- K8s base: namespace, RBAC, default-deny + intra-namespace NetworkPolicies
- 4 ADRs (saga vs choreography, outbox pattern, REST vs gRPC, W3C tracecontext)
- 4-level C4 architecture diagrams (System Context, Container, 3 Component diagrams + new Saga component diagram)
- Helm charts for all 4 services + 3 infra deps (postgres/redis/redpanda), `values-dev.yaml` overlays
- kind cluster config (`deploy/kind/kind.yaml`) + `make kind-up/down/load`
- testcontainers-go E2E harness (3 Postgres + Redis + Kafka) at `tests/harness/`
- End-to-end demo script at `docs/demo/demo.sh`

### ⬜ Deferred to v0.3.0
- Individual E2E / chaos / load tests (harness ready, tests to be added)
- Kustomize overlays + ArgoCD Application manifests
- kind smoke test (requires `kind` binary installation)
- asciinema recording of the demo

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
