# orderflow — Status

**Last updated:** 2026-08-17 (post v0.2.0 release; 3.10–3.13 closed; 3.14 review done)

## Sub-stages

| Stage | Title                      | Status   | Commit    | Plan ref |
|-------|----------------------------|----------|-----------|----------|
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
| 3.10.a | kafkaprop module (Inject / Extract / SpanFromEnvelope) | done | b4f784e |
| 3.10.b | outbox publisher populates TraceID/SpanID from active span | done | 9d1fae1 |
| 3.10.c | consumer dispatch restores traceparent + creates per-message span | done | c735586 |
| 3.10.d | chi middleware on /healthz and /metrics for all 4 service binaries | done | f640c3d / ea983ee / eaeb4c2 / 7545b64 |
| 3.10.e | service.version resource attribute on every service | done | 4a2fdd5 |
| 3.10.f | Tempo runbook + OTLP env defaults | done | 6a6477e |
| 3.11.a | testcontainers harness (postgres×3 + redis + kafka) | done | 1bfe584 |
| 3.12.a.order | Helm chart for Order Service | done | be6daec |
| 3.12.a.payment | Helm chart for Payment Service | done | 279a39f |
| 3.12.a.inventory | Helm chart for Inventory Service | done | 548319f |
| 3.12.a.saga | Helm chart for Saga orchestrator | done | d239bbd |
| 3.12.b | infra Helm charts (postgres/redis/redpanda) | done | c37d081 |
| 3.12.e | kind cluster config + make kind-up/down/load | done | 0b03254 |
| 3.13.a | ADR-0004 (W3C tracecontext) + decision log index | done | c254cb3 |
| 3.13.b | C4 component diagram for Saga orchestrator | done | 601c59c |
| 3.13.c | end-to-end demo script + README | done | adfcffe |
| 3.13.e | sub-stages index doc (fixes README broken link) | done | c2b0598 |
| 3.14 | final review + gofmt cleanup | done | 4fb1100 |
| v0.2.0 | CHANGELOG + README + tag | done | — |
| 3.4.g | Order Service PGRepository + wire REST handler | done | f67b33a |
| 3.11.prep | harness.StartService helper | done | 50524d6 |
| 3.11.b | E2E happy path test | done | a402cf2 |
| 3.11.c | E2E compensation test (payment declined) | done | 1e0fd65 |
| 3.11.d | chaos test — redpanda kill mid-order (simplified to order-survives assertion) | done | a651fc5 |
| 3.11.e | load test — 100 RPS p95<1s via k6 | done | 3925070 |
| 3.11.f | CI job for E2E (ubuntu, needs:build) | done | b1411a5 |
| 3.12.c | Kustomize overlays (dev/staging/prod) | done | ddf421b |
| 3.12.d | ArgoCD Application manifests | done | bc142ce |
| 3.5.g | Payment Service webhook handler + PGRepository + REST mount | done | 90dc049 |
| 3.6.g | Inventory Service PGRepository + GET /v1/inventory/stock endpoint | done | 3d95b86 |
| 3.11.polish | make e2e aggregate target + harness.RestartKafka helper | done | b3bc2c2 |
| 3.4.1 | commit missing saga go.sum files | done | 99ba4bc |
| 3.4.2 | LDFLAGS injection for binary Version | done | cf3195d |
| v0.5.0.saga | saga runtime — consumer + outbox + repository + watchdog | done | ae611b0 |
| v0.5.0.inventory | real consumer handlers + POST /v1/inventory/reserve | done | a238e77 |
| v0.5.0.payment | real PaymentRequested handler with mock provider | done | 1921628 |
| v0.5.0.order | real consumer handlers (StockReserved/OrderConfirmed/etc) | done | f42f71e |
| v0.6.0 | saga cross-restart TTL sweep for crashed-saga recovery | done | a3cf23f |
| v1.0.kind | kind smoke test (cluster create + Helm template validation) | done | 5ce2be7 |
| v1.0.demo | asciinema recording script + runbook | done | 19d5ec0 |
| v1.0 | final CHANGELOG/README/tag | done | — |
| v1.1.pre | Saga shutdown goroutine leak + `mustMarshal` panic fix | done | c8a396898d639defb4738793fedb1aaff0255b41 | this plan |
| web.1    | bootstrap web module skeleton | done | d38cf27 | this plan |
| web.2    | backend clients + types | done | 4fe476b | this plan |
| web.3    | server scaffolding + probes | done | 9190081 | this plan |
| web.4    | layout + stylesheet | done | b87f643 | this plan |
| web.5    | orders list page | done | 7d401e7 | this plan |
| web.6    | create-order page | done | 9119964 | this plan |
| web.7    | order detail + cancel | done | b93ddac | this plan |
| web.8    | inventory viewer | done | 497b12f | this plan |
| web.9    | payment webhook simulator | done | 3895c51 | this plan |
| web.10   | live event tail + SSE | done | 912e2b2 | this plan |
| web.11   | compose + Makefile + README | done | a52edef   | this plan |

## Deferred to v1.1

- Full outbox-retry chaos assertion (services cache `KAFKA_BROKER` at startup)
- kind smoke: actual image loading into cluster (currently only validates Helm template rendering, not full deploy)
- ghcr.io publishing pipeline (binaries are built locally; CI publishing is a separate concern)

## Session handoff

A compact session-resume document lives at
`docs/superpowers/portfolio/orderflow-checkpoint.md`. The full sub-stages index
is at `docs/superpowers/portfolio/orderflow-substages.md`.