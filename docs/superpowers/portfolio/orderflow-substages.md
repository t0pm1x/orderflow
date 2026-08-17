# orderflow Sub-Stages Index

> **Canonical list of all planned sub-stages across stages 3.0 - 3.14.**
> The authoritative live status is always [STATUS.md](../../../STATUS.md) at the
> repo root - this file is the reference for what was planned and where each
> sub-stage lives in the design.

Status legend: done = committed to main; DEFERRED = planned for v0.2.0,
implementation pending. Plan refs point at per-stage plans under
docs/superpowers/plans/ (v0.2.0 stages only) or fall back to STATUS.md for
stages already shipped in v0.1.0-MVP.

| Stage | Title | Status | Commit | Plan ref |
|-------|-------|--------|--------|----------|
| 3.0 | Bootstrap monorepo | done | 9c0b11e | STATUS.md |
| 3.0.b | pkg/platform initial module | done | 28aca48 | STATUS.md |
| 3.1.a-c | C4 architecture diagrams | done | 2cfc06a | STATUS.md |
| 3.1.d-f | ADRs (saga/outbox/REST-gRPC) | done | 4c9e396 | STATUS.md |
| 3.1.g | OpenAPI spec | done | b7e1006 | STATUS.md |
| 3.1.h | Domain events spec | done | b7e1006 | STATUS.md |
| 3.2.a | docker-compose full stack | done | 267216b | STATUS.md |
| 3.2.b | Redpanda config + topic init | done | 7dbeec0 | STATUS.md |
| 3.2.c | Postgres per-service init | done | 071bbeb | STATUS.md |
| 3.2.e-h | observability configs (prom/tempo/otel/grafana) | done | d11b36b | STATUS.md |
| 3.2.i | k8s base manifests (namespace, rbac, netpol, kustomize) | done | 47b170d | STATUS.md |
| 3.3.a-b | logging (slog+trace correlation) + OTel init | done | 2c52231 | STATUS.md |
| 3.3.c-d | chi middleware stack + shared types (Money/IDs) | done | b85d10f | STATUS.md |
| 3.3.e-f | events envelope (franz-go) + typed errors | done | 823d267 | STATUS.md |
| 3.4.a | Order Service skeleton | done | 9ffb1cc | STATUS.md |
| 3.4.b | Order Domain (state machine + Order aggregate) | done | cec01b9 | STATUS.md |
| 3.4.c | Order REST API (POST/GET/List) | done | c63785b | STATUS.md |
| 3.4.d | Order outbox (PGWriter, handler emits OrderCreated, PGSource) | done | e4a9ec1+ | STATUS.md |
| 3.4.e | Order migrations (orders + order_outbox) | done | 38d8028 | STATUS.md |
| 3.4.f | Order tests (record builder coverage) | done | 30847d8 | STATUS.md |
| 3.5.a | Payment Service skeleton | done | 67c399f | STATUS.md |
| 3.5.b | Payment mock provider (Charge/Refund by last-4) | done | e7a0a3f | STATUS.md |
| 3.5.c | Payment idempotency middleware (Redis-backed dedupe, Begin/Complete/Release, chi middleware) | done | 63b09a0 | STATUS.md |
| 3.5.d | Payment outbox (PGWriter) | done | 4b474ad | STATUS.md |
| 3.5.e | Payment migrations (payments + idempotency_keys + payment_outbox) | done | 38d8028 | STATUS.md |
| 3.5.f | Payment tests (duplicate-webhook + Begin/Release integration) | done | bb5d113 | STATUS.md |
| 3.6.a | Inventory Service skeleton | done | 65ec9cf | STATUS.md |
| 3.6.b | Inventory Stock model + Reservation | done | ef156cc | STATUS.md |
| 3.6.c | Inventory optimistic locking (UPDATE WHERE version, ErrStaleVersion vs ErrInsufficientStock) | done | 72d9c4b | STATUS.md |
| 3.6.d | Inventory outbox (PGWriter) | done | 1080750 | STATUS.md |
| 3.6.e | Inventory migrations (stock_items + inventory_outbox) | done | 38d8028 | STATUS.md |
| 3.6.f | Inventory tests (concurrent reserve race test) | done | f044584 | STATUS.md |
| 3.7.a | pkg/outbox poller (FetchPending / MarkSent / MarkFailed + retry/DLQ) | done | 621b349 | STATUS.md |
| 3.7.b | pkg/outbox KafkaPublisher (at-least-once) | done | 9fad452 | STATUS.md |
| 3.7.c | pkg/outbox KafkaDLQ (per-topic .DLQ) | done | 9fad452 | STATUS.md |
| 3.7.d | pkg/outbox PrometheusMetrics | done | cf53af3 | STATUS.md |
| 3.7.e | outbox wired into 3 service binaries + PGSource per service | done | 2df88a5 / 0e78c61 | STATUS.md |
| 3.7.f | outbox integration tests (testcontainers) | DEFERRED | — | STATUS.md |
| 3.8.a | pkg/consumer base (franz-go consumer group) | done | 52a527e | STATUS.md |
| 3.8.b | pkg/consumer idempotent handler wrapper (event_id dedupe) | done | 52a527e | STATUS.md |
| 3.8.c | pkg/consumer DLQ (handler error → retry → DLQ) | done | 52a527e | STATUS.md |
| 3.8.d | per-service handler registries + consumer runners | done | 032df4c | STATUS.md |
| 3.8.e | consumer integration tests (testcontainers) | DEFERRED | — | STATUS.md |
| 3.9.a | saga state machine (initiated → stock_reserved → completed/compensated) | done | ae30a44 | STATUS.md |
| 3.9.b | saga compensation (idempotent compensators + ReleaseStock/RefundPayment factories) | done | ae30a44 | STATUS.md |
| 3.9.c | saga timeout watchdog (in-memory + TTL row for restart sweep) | done | ae30a44 | STATUS.md |
| 3.9.d | saga migrations (order_sagas + TTL index) | done | cb8cae8 | STATUS.md |
| 3.9.e | saga unit tests (state transitions, compensation, watchdog expiry) | done | ae30a44 | STATUS.md |
| v0.1.0-MVP | README + CHANGELOG | done | cd8b2f5 | STATUS.md |
| 3.10.a | Add otelfranzgo / kafkaprop module | DEFERRED | вЂ” | docs/superpowers/plans/2026-08-17-orderflow-v0.2.0-3.10.md |
| 3.10.b | Populate TraceID/SpanID in outbox publisher | DEFERRED | вЂ” | docs/superpowers/plans/2026-08-17-orderflow-v0.2.0-3.10.md |
| 3.10.c | Restore traceparent in consumer dispatch | DEFERRED | вЂ” | docs/superpowers/plans/2026-08-17-orderflow-v0.2.0-3.10.md |
| 3.10.d | chi middleware on /healthz and /metrics for all 4 service binaries | DEFERRED | вЂ” | docs/superpowers/plans/2026-08-17-orderflow-v0.2.0-3.10.md |
| 3.10.e | service.version resource attribute on every service | DEFERRED | вЂ” | docs/superpowers/plans/2026-08-17-orderflow-v0.2.0-3.10.md |
| 3.10.f | Tempo wire-up runbook + OTLP env defaults | DEFERRED | вЂ” | docs/superpowers/plans/2026-08-17-orderflow-v0.2.0-3.10.md |
| 3.11.a | testcontainers harness + shared tests/ module | DEFERRED | вЂ” | docs/superpowers/plans/2026-08-17-orderflow-v0.2.0-3.11.md |
| 3.11.b | E2E happy path (Order confirmed in 30s) | DEFERRED | вЂ” | docs/superpowers/plans/2026-08-17-orderflow-v0.2.0-3.11.md |
| 3.11.c | E2E compensation (payment declined -> cancel) | DEFERRED | вЂ” | docs/superpowers/plans/2026-08-17-orderflow-v0.2.0-3.11.md |
| 3.11.d | Chaos: redpanda kill mid-flow | DEFERRED | вЂ” | docs/superpowers/plans/2026-08-17-orderflow-v0.2.0-3.11.md |
| 3.11.e | Load: 100 RPS for 60s with k6 (p95 < 1s) | DEFERRED | вЂ” | docs/superpowers/plans/2026-08-17-orderflow-v0.2.0-3.11.md |
| 3.11.f | CI integration (e2e job) | DEFERRED | вЂ” | docs/superpowers/plans/2026-08-17-orderflow-v0.2.0-3.11.md |
| 3.12.a | Helm charts per service (4 charts) | DEFERRED | вЂ” | docs/superpowers/plans/2026-08-17-orderflow-v0.2.0-3.12.md |
| 3.12.b | Infra Helm charts (postgres / redis / redpanda) | DEFERRED | вЂ” | docs/superpowers/plans/2026-08-17-orderflow-v0.2.0-3.12.md |
| 3.12.c | Kustomize overlays (dev / staging / prod) | DEFERRED | вЂ” | docs/superpowers/plans/2026-08-17-orderflow-v0.2.0-3.12.md |
| 3.12.d | ArgoCD Application manifests + ApplicationSet | DEFERRED | вЂ” | docs/superpowers/plans/2026-08-17-orderflow-v0.2.0-3.12.md |
| 3.12.e | kind cluster config + make kind-up/down/load | DEFERRED | вЂ” | docs/superpowers/plans/2026-08-17-orderflow-v0.2.0-3.12.md |
| 3.12.f | kind smoke test (all services healthy) | DEFERRED | вЂ” | docs/superpowers/plans/2026-08-17-orderflow-v0.2.0-3.12.md |
| 3.13.a | ADR-0004 (W3C tracecontext) + ADR log index in README | DEFERRED | вЂ” | docs/superpowers/plans/2026-08-17-orderflow-v0.2.0-3.13.md |
| 3.13.b | C4 component diagram for Saga orchestrator | DEFERRED | вЂ” | docs/superpowers/plans/2026-08-17-orderflow-v0.2.0-3.13.md |
| 3.13.c | Demo script (docs/demo/demo.sh + README) | DEFERRED | вЂ” | docs/superpowers/plans/2026-08-17-orderflow-v0.2.0-3.13.md |
| 3.13.d | Asciinema recording of happy-path demo | DEFERRED | вЂ” | docs/superpowers/plans/2026-08-17-orderflow-v0.2.0-3.13.md |
| 3.13.e | Sub-stages index doc (this file) | DEFERRED | вЂ” | docs/superpowers/plans/2026-08-17-orderflow-v0.2.0-3.13.md |
| 3.14.a | Review checklist pass + CHANGELOG + v0.2.0 tag | DEFERRED | вЂ” | docs/superpowers/plans/2026-08-17-orderflow-v0.2.0-3.14.md |

## Stages

### Stage 3.0 - Bootstrap monorepo (2 sub-stages)

- 3.0 - Bootstrap monorepo (`go.work` + 9 modules + 4 cmd stubs)
- 3.0.b - pkg/platform initial module (slog + OTel init)

### Stage 3.1 - Spec phase: C4 / ADRs / OpenAPI / events (8 sub-stages)

- 3.1.a-c - C4 architecture diagrams (System Context + Container + per-service Component)
- 3.1.d-f - ADRs (saga, outbox, REST vs gRPC)
- 3.1.g - OpenAPI spec for Order/Payment/Inventory endpoints
- 3.1.h - Domain events spec (11 events + envelope)

### Stage 3.2 - Platform infrastructure (8 sub-stages)

- 3.2.a - docker-compose full stack (3 postgres, redis, redpanda, otel-collector, prometheus, tempo, grafana)
- 3.2.b - Redpanda config + topic init
- 3.2.c - Postgres per-service init
- 3.2.e-h - observability configs (prometheus/tempo/otel/grafana)
- 3.2.i - k8s base manifests (namespace, RBAC, NetworkPolicies, Kustomize)

### Stage 3.3 - Common library: logging / middleware / events / errors (6 sub-stages)

- 3.3.a-b - logging (slog + trace correlation) + OTel init
- 3.3.c-d - chi middleware stack + shared types (Money / typed UUIDs)
- 3.3.e-f - events envelope (franz-go) + typed errors

### Stage 3.4 - Order Service (6 sub-stages, all done)

- 3.4.a - Order Service skeleton
- 3.4.b - Order Domain (state machine + Order aggregate)
- 3.4.c - Order REST API (POST/GET/List)
- 3.4.d - Order outbox (PGWriter, handler emits OrderCreated, PGSource)
- 3.4.e - Order migrations (orders + order_outbox)
- 3.4.f - Order tests (record builder coverage)

### Stage 3.5 - Payment Service (6 sub-stages, all done)

- 3.5.a - Payment Service skeleton
- 3.5.b - Payment mock provider (Charge/Refund by last-4)
- 3.5.c - Payment idempotency middleware (Redis-backed dedupe, Begin/Complete/Release, chi middleware)
- 3.5.d - Payment outbox (PGWriter)
- 3.5.e - Payment migrations (payments + idempotency_keys + payment_outbox)
- 3.5.f - Payment tests (duplicate-webhook + Begin/Release integration)

### Stage 3.6 - Inventory Service (6 sub-stages, all done)

- 3.6.a - Inventory Service skeleton
- 3.6.b - Inventory Stock model + Reservation
- 3.6.c - Inventory optimistic locking (UPDATE WHERE version, ErrStaleVersion vs ErrInsufficientStock)
- 3.6.d - Inventory outbox (PGWriter)
- 3.6.e - Inventory migrations (stock_items + inventory_outbox)
- 3.6.f - Inventory tests (concurrent reserve race test)

### Stage 3.7 - Outbox polling + Kafka publish (6 sub-stages)

- 3.7.a - pkg/outbox poller (FetchPending / MarkSent / MarkFailed + retry/DLQ)
- 3.7.b - pkg/outbox KafkaPublisher (at-least-once)
- 3.7.c - pkg/outbox KafkaDLQ (per-topic .DLQ)
- 3.7.d - pkg/outbox PrometheusMetrics
- 3.7.e - outbox wired into 3 service binaries + PGSource per service
- 3.7.f - outbox integration tests (testcontainers) [DEFERRED - moved into 3.11]

### Stage 3.8 - Consumer base + idempotent handlers + DLQ (5 sub-stages)

- 3.8.a - pkg/consumer base (franz-go consumer group)
- 3.8.b - pkg/consumer idempotent handler wrapper (event_id dedupe)
- 3.8.c - pkg/consumer DLQ (handler error -> retry -> DLQ)
- 3.8.d - per-service handler registries + consumer runners
- 3.8.e - consumer integration tests (testcontainers) [DEFERRED - moved into 3.11]

### Stage 3.9 - Saga orchestrator (5 sub-stages, all done)

- 3.9.a - saga state machine (initiated -> stock_reserved -> completed/compensated)
- 3.9.b - saga compensation (idempotent compensators + ReleaseStock/RefundPayment factories)
- 3.9.c - saga timeout watchdog (in-memory + TTL row for restart sweep)
- 3.9.d - saga migrations (order_sagas + TTL index)
- 3.9.e - saga unit tests (state transitions, compensation, watchdog expiry)

### Stage 3.10 - W3C tracecontext through Kafka (6 sub-stages, all DEFERRED)

- 3.10.a - Add otelfranzgo / kafkaprop module (SEQ)
- 3.10.b - Populate TraceID/SpanID in outbox publisher (PAR)
- 3.10.c - Restore traceparent in consumer dispatch (PAR)
- 3.10.d - chi middleware on /healthz and /metrics for all 4 service binaries (PAR)
- 3.10.e - service.version resource attribute on every service (PAR)
- 3.10.f - Tempo wire-up runbook + OTLP env defaults (SEQ; depends on b/c/d/e)

### Stage 3.11 - E2E/chaos/load (6 sub-stages, all DEFERRED)

- 3.11.a - testcontainers harness + shared tests/ module (SEQ)
- 3.11.b - E2E happy path: Order confirmed in 30s (PAR)
- 3.11.c - E2E compensation: payment declined -> cancel (PAR)
- 3.11.d - Chaos: redpanda kill mid-flow (PAR)
- 3.11.e - Load: 100 RPS for 60s with k6, p95 < 1s (PAR)
- 3.11.f - CI integration: e2e job (SEQ)

### Stage 3.12 - Helm / Kustomize / ArgoCD / kind (6 sub-stages, all DEFERRED)

- 3.12.a - Helm charts per service (4 charts, PAR a.1..a.4)
- 3.12.b - Infra Helm charts: postgres / redis / redpanda (PAR)
- 3.12.c - Kustomize overlays: dev / staging / prod (PAR with 3.12.d)
- 3.12.d - ArgoCD Application manifests + ApplicationSet (PAR with 3.12.c)
- 3.12.e - kind cluster config + make kind-up/down/load (PAR; independent)
- 3.12.f - kind smoke test (SEQ; depends on a..e)

### Stage 3.13 - Docs + demo + asciinema + sub-stages index (5 sub-stages, all DEFERRED)

- 3.13.a - ADR-0004 (W3C tracecontext) + ADR log index in README (PAR)
- 3.13.b - C4 component diagram for Saga orchestrator (PAR)
- 3.13.c - Demo script (docs/demo/demo.sh + README) (PAR)
- 3.13.d - Asciinema recording of happy-path demo (SEQ; depends on 3.13.c)
- 3.13.e - Sub-stages index doc (this file) (PAR)

### Stage 3.14 - Final whole-branch review (1 sub-stage, DEFERRED)

- 3.14.a - Review checklist pass + CHANGELOG + v0.2.0 tag (SEQ; single task)

### v0.1.0-MVP release marker

- v0.1.0-MVP - README + CHANGELOG (tag pending, see [orderflow-checkpoint.md](orderflow-checkpoint.md))

## Deferred / partial

Items pulled into v0.2.0 scope (stages 3.10-3.14 above). Rationale per
[orderflow-checkpoint.md](orderflow-checkpoint.md):

- **3.7.f** (outbox integration tests) and **3.8.e** (consumer integration tests):
  were scoped into v0.1.0-MVP but blocked on testcontainers. Landed in stage 3.11
  instead, where the shared harness from 3.11.a covers both.
- **3.10.x** tracing: required chi middleware + outbox producer span + consumer
  dispatch span. Blocked on Tempo wire-up; resolved by stage 3.10 itself.
- **3.11.x** E2E / chaos / load: required the v0.1.0-MVP feature set (3.4-3.9) plus
  chi middleware (3.10.d) to be in place before testcontainers could exercise the
  full stack.
- **3.12.x** Helm/Kustomize/ArgoCD/kind: required chi-based /healthz on all 4
  service binaries (3.10.d) so k8s probes actually work.
- **3.13.x** docs + demo: required tangibles from 3.10-3.12 to be real before
  screenshotting them; the asciinema step (3.13.d) depends on 3.13.c.
- **3.14.a** final review: single-shot check after everything else lands; closes
  v0.2.0 by tagging.

## References

- [STATUS.md](../../../STATUS.md) - authoritative live status (commit hashes, deferred markers)
- [orderflow-checkpoint.md](orderflow-checkpoint.md) - session-resume handoff; what was done and what is open
- [2026-08-17-orderflow-v0.2.0.md](../../plans/2026-08-17-orderflow-v0.2.0.md) - top-level v0.2.0 plan + global constraints
- Per-stage plans under docs/superpowers/plans/2026-08-17-orderflow-v0.2.0-3.X.md (where X is 10-14)

