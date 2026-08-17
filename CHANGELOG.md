# Changelog

All notable changes to orderflow will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0-MVP] - 2026-08-17

### Added
- Monorepo bootstrap with `go.work` (1 platform module + 3 service modules + 4 cmd stubs)
- pkg/platform library: slog logging (with OTel trace correlation), chi middleware stack, types (Money, OrderID, etc.), event envelope, typed errors
- Order Service: domain (state machine: pending→reserved→confirmed/cancelled/failed), REST API (POST/GET/LIST) with 5 tests
- Payment Service: mock provider with deterministic success/decline/insufficient-funds/timeout
- Inventory Service: Stock model with optimistic locking version column
- Docker-compose stack: 3 Postgres, Redis, Redpanda (KRaft), OTel Collector, Prometheus, Tempo, Grafana
- K8s base: namespace, RBAC, default-deny + intra-namespace NetworkPolicies
- 3 ADRs: saga-vs-choreography, outbox-pattern, REST-vs-gRPC
- 3-level C4 architecture diagrams (PlantUML)
- OpenAPI 3.0 spec for Order/Payment/Inventory REST APIs
- Domain events spec (11 events + envelope type)
- Makefile with build/test/lint/run/run-<svc>/clean/tidy targets
- GitHub Actions CI (3-OS matrix)

### Known Limitations (MVP scope)
- No database migrations implemented (services have no DB wired)
- No Kafka producers/consumers (events not flowing)
- No outbox pattern implementation (3.7 deferred)
- No saga orchestrator (3.9 deferred)
- HTTP API only for Order Service; Payment and Inventory have REST stubs only
- docker-compose stack defined but services don't connect to it
- No E2E / chaos / load tests
- No Helm charts

## [Unreleased]

## [0.3.0] - 2026-08-17

### Added
- **3.4.g** Order Service PGRepository (`Insert/Get/List` over `pgxpool.Pool`, tx-atomic with outbox writer). REST handler wired into the order binary's chi router at `/v1/orders`. Compile-time `var _ api.Repository = (*PGRepo)(nil)` assertion.
- **3.11.prep** `tests/harness.StartService(t, name, binName, env)` — launches a service binary as a child process with full env wiring (`DATABASE_URL`, `KAFKA_BROKER`, `REDIS_URL`, `HTTP_ADDR`) + `OTEL_EXPORTER=stdout` for hermeticity. Service logs to `tests/logs/<name>.log`. Graceful SIGTERM on stop.
- **3.11.b** E2E happy path test — POSTs `examples/order.json`, polls until `confirmed` within 60s. Boots order/payment/inventory/saga against testcontainers harness.
- **3.11.c** E2E compensation test — POSTs order with `last_four=0001` (forces `card_declined`), polls until `cancelled`/`failed`.
- **3.11.d** Chaos test — kills Kafka container mid-order, asserts order service stays healthy and order does not spuriously progress to `confirmed`. (Full outbox-retry recovery assertion deferred — requires `RestartKafka` harness helper.)
- **3.11.e** Load test — k6 50 VUs × 60s, thresholds `p(95)<1000` + `rate<0.05`. Go wrapper in `tests/load/load_test.go` shells out to k6 binary. New `make load` target.
- **3.11.f** CI job — new `e2e` GitHub Actions job (ubuntu-only, `needs: build`, 30-min timeout) running happy/compensation/chaos. Load test excluded (manual `make load`).
- **3.12.c** Kustomize overlays — `deploy/kustomize/{base,overlays/{dev,staging,prod}}`. Dev: replicas=1, ingress, reduced resources. Staging: replicas=2. Prod: replicas=3, HPA (cpu 70%, 3→10), PDB (`minAvailable: 2`), larger resources. Base references `all-services.yaml` (regenerated via `helm template`).
- **3.12.d** ArgoCD manifests — `projects.yaml` (AppProject RBAC constraining destinations to `orderflow-*`), `appset.yaml` (ApplicationSet per env), per-env `overlays/{dev,staging,prod}.yaml`. Automated prune + selfHeal, exponential backoff retry (5 attempts, 10s→5m).

### Deferred to v0.4.0
- 3.12.f kind smoke test (`kind` binary not installed locally).
- 3.13.d asciinema recording.
- `RestartKafka` helper for full outbox-retry chaos assertion.

## [0.2.0] - 2026-08-17

### Added
- **Tracing (3.10)**: W3C tracecontext propagation through Kafka. New `pkg/platform/instrumentation/kafkaprop` module (Inject / Extract / SpanFromEnvelope). Outbox publisher populates `Envelope.TraceID`/`SpanID` from active span. Consumer restores traceparent and creates `consumer.<EventType>` child span. chi middleware on `/healthz` and `/metrics` for all 4 service binaries. `service.version` resource attribute on every service. OTLP env defaults (`OTEL_EXPORTER=otlp`, `OTEL_EXPORTER_OTLP_ENDPOINT=otel-collector:4317`).
- **E2E harness (3.11.a)**: `tests/harness` testcontainers-go harness starting 3 Postgres + Redis + Kafka (confluent-local) + optional OTel collector. Self-test PASS in ~18s. Per-service migrations applied on startup. Individual E2E/chaos/load tests deferred to v0.3.0.
- **Helm charts (3.12.a, 3.12.b)**: Production-ready Helm 3 charts for all 4 services + 3 infra deps (postgres with 3 databases, redis, redpanda). `values.yaml` (production defaults) + `values-dev.yaml` (replicas=1, debug logs). Security defaults (runAsNonRoot, readOnlyRootFilesystem). ServiceAccounts + ConfigMaps. Kustomize overlays + ArgoCD manifests deferred to v0.3.0.
- **kind cluster (3.12.e)**: `deploy/kind/kind.yaml` with 10 host→container port mappings matching prod docker-compose. Makefile targets `kind-up / kind-down / kind-load / kind-status` with prereq check.
- **ADR-0004 (3.13.a)**: W3C tracecontext decision. Decision log table added to README.
- **Saga C4 diagram (3.13.b)**: `docs/architecture/c4-level-3-saga.puml` (33 lines) — state machine, consumer, publisher, watchdog, Postgres writer/source.
- **Demo script (3.13.c)**: `docs/demo/demo.sh` runs the full happy path (compose up → build → start services → POST order → poll until confirmed) in ~60s. `docs/demo/README.md` with prerequisites + troubleshooting.
- **Sub-stages index (3.13.e)**: `docs/superpowers/portfolio/orderflow-substages.md` — 73-row table; fixes broken README link.

### Changed
- `outbox.Record` gains `Headers map[string]string` (JSONB column added per service via `0002_outbox_headers.sql`).
- `events.Client.PublishRaw` takes headers map.
- `platform.InitTracing` signature now `(ctx, name, version)` (was `(ctx, name)`).

### Deferred to v0.3.0
- 3.11.b–e individual E2E tests (harness ready; tests to be added).
- 3.11.f E2E CI job.
- 3.12.c Kustomize overlays.
- 3.12.d ArgoCD Application manifests.
- 3.12.f kind smoke test (requires `kind` binary).
- 3.13.d asciinema recording (manual).
