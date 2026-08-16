# orderflow — Status

**Last updated:** 2026-08-17 (post v0.1.0-MVP, 20 sub-stages added in this session)

## Sub-stages

| Stage | Title                      | Status   | Commit    |
|-------|----------------------------|----------|-----------|
| 3.0   | Bootstrap monorepo         | done     | 9c0b11e   |
| 3.0.b | pkg/platform initial module | done    | 28aca48   |
| 3.1.a-c | C4 architecture diagrams  | done     | 2cfc06a   |
| 3.1.d-f | ADRs (saga/outbox/REST-gRPC) | done  | 4c9e396   |
| 3.1.g | OpenAPI spec               | done     | b7e1006   |
| 3.1.h | Domain events spec         | done     | b7e1006   |
| 3.2.a | docker-compose full stack  | done     | 267216b   |
| 3.2.b | Redpanda config + topic init | done   | 7dbeec0   |
| 3.2.c | Postgres per-service init  | done     | 071bbeb   |
| 3.2.e-h | observability configs (prom/tempo/otel/grafana) | done | d11b36b |
| 3.2.i | k8s base manifests (namespace, rbac, netpol, kustomize) | done | 47b170d |
| 3.3.a-b | logging (slog+trace correlation) + OTel init | done | 2c52231 |
| 3.3.c-d | chi middleware stack + shared types (Money/IDs) | done | b85d10f   |
| 3.3.e-f | events envelope (franz-go) + typed errors | done | 823d267   |
| 3.4.a | Order Service skeleton | done | 9ffb1cc |
| 3.4.b | Order Domain (state machine + Order aggregate) | done | cec01b9 |
| 3.4.c | Order REST API (POST/GET/List) | done | c63785b |
| 3.4.d | Order outbox (PGWriter, handler emits OrderCreated, PGSource) | done | e4a9ec1+ |
| 3.4.e | Order migrations (orders + order_outbox) | done | 38d8028  |
| 3.4.f | Order tests (record builder coverage) | done | 30847d8  |
| 3.5.a | Payment Service skeleton | done | 67c399f |
| 3.5.b | Payment mock provider (Charge/Refund by last-4) | done | e7a0a3f |
| 3.5.c | Payment idempotency middleware (Redis-backed dedupe, Begin/Complete/Release, chi middleware) | done | 63b09a0  |
| 3.5.d | Payment outbox (PGWriter) | done     | 4b474ad  |
| 3.5.e | Payment migrations (payments + idempotency_keys + payment_outbox) | done | 38d8028 |
| 3.5.f | Payment tests (duplicate-webhook + Begin/Release integration) | done | bb5d113 |
| 3.6.a | Inventory Service skeleton | done | 65ec9cf |
| 3.6.b | Inventory Stock model + Reservation | done | ef156cc |
| 3.6.c | Inventory optimistic locking (UPDATE WHERE version, ErrStaleVersion vs ErrInsufficientStock) | done | 72d9c4b |
| 3.6.d | Inventory outbox (PGWriter)         | done   | 1080750  |
| 3.6.e | Inventory migrations (stock_items + inventory_outbox) | done | 38d8028 |
| 3.6.f | Inventory tests (concurrent reserve race test) | done | f044584 |
| 3.7.a | pkg/outbox poller (FetchPending / MarkSent / MarkFailed + retry/DLQ) | done | 621b349 |
| 3.7.b | pkg/outbox KafkaPublisher (at-least-once) | done | 9fad452 |
| 3.7.c | pkg/outbox KafkaDLQ (per-topic .DLQ) | done | 9fad452 |
| 3.7.d | pkg/outbox PrometheusMetrics | done | cf53af3 |
| 3.7.e | outbox wired into 3 service binaries + PGSource per service | done | 2df88a5 / 0e78c61 |
| 3.7.f | outbox integration tests (testcontainers) | DEFERRED | — |
| 3.8.a | pkg/consumer base (franz-go consumer group) | done | 52a527e |
| 3.8.b | pkg/consumer idempotent handler wrapper (event_id dedupe) | done | 52a527e |
| 3.8.c | pkg/consumer DLQ (handler error → retry → DLQ) | done | 52a527e |
| 3.8.d | per-service handler registries + consumer runners | done | 032df4c |
| 3.8.e | consumer integration tests (testcontainers) | DEFERRED | — |
| 3.9.a | saga state machine (initiated → stock_reserved → completed/compensated) | done | ae30a44 |
| 3.9.b | saga compensation (idempotent compensators + ReleaseStock/RefundPayment factories) | done | ae30a44 |
| 3.9.c | saga timeout watchdog (in-memory + TTL row for restart sweep) | done | ae30a44 |
| 3.9.d | saga migrations (order_sagas + TTL index) | done | cb8cae8 |
| 3.9.e | saga unit tests (state transitions, compensation, watchdog expiry) | done | ae30a44 |
| v0.1.0-MVP | README + CHANGELOG | done | cd8b2f5 |

## Next up (deferred — requires infrastructure)

- 3.10.a–d W3C tracecontext through Kafka + Tempo wire-up + service map
- 3.11.a–f E2E tests (happy / compensation / chaos / load) — testcontainers
- 3.12.a–f Helm + Kustomize + ArgoCD + kind smoke
- 3.13.a–d README + ADR log + demo script + asciinema
- 3.14    final whole-branch review

## Session handoff

A compact session-resume document lives at
`docs/superpowers/portfolio/orderflow-checkpoint.md`.