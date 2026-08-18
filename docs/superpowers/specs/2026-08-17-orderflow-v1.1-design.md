# orderflow v1.1 — Production-ready baseline design

**Created:** 2026-08-17
**Status:** approved (brainstorming complete)
**Goal:** Close the largest gaps between current documentation and runtime behavior so that v1.1 can credibly be deployed and operated.

## Context

orderflow reached v1.0.0 on 2026-08-17. The platform is end-to-end functional (Order → Payment → Inventory → saga) with Helm/Kustomize/ArgoCD, CI, and E2E/chaos/load tests. The codebase is in good shape overall but contains:

- Two real production bugs (saga shutdown goroutine leak, `mustMarshal` panic in compensation path).
- Significant architectural drift (ADR-0003 promises gRPC that was never built; dead stub packages; OpenAPI documents endpoints that are not implemented).
- Three deferred v1.1 items: full outbox-retry chaos assertion, kind smoke with real image loading, ghcr.io publishing pipeline.
- Reliability gaps that conflict with multi-replica Helm defaults (outbox has no `SKIP LOCKED`, consumer runners do not configure a durable deduper).
- Missing Dockerfiles (blocker for both kind smoke and ghcr.io publishing).
- Observability drift: dashboard queries metrics that do not exist; Prometheus scrape ports are stale; tracing service names are outbox table names instead of service names.

v1.0 also still uses the original stage numbering (`3.x`). v1.1 introduces a new convention: `v1.1.pre` plus `v1.1.{a..e}`.

## Goals

1. No-loss Kafka recovery — orders accepted while Kafka is unavailable reach `confirmed` after recovery.
2. Reproducible signed images — every release produces four immutable GHCR images with SBOM, provenance, and Trivy scan.
3. One-command Kubernetes deployment — `make kind-up && make smoke-k8s` builds images, deploys dependencies, runs migrations, deploys services, and confirms a real order end-to-end.

## Non-goals (explicitly out of scope)

- Multi-tenancy / tenant isolation.
- mTLS service mesh.
- GraphQL / partial-response selection.
- Loki / structured log search (structured JSON logs are sufficient for v1.1).
- GDPR erasure orchestration.
- External auth provider integration (OIDC/JWT). v1.1 ships only HMAC webhook verification and an API-key middleware as the minimum viable subset.
- Customer-scoped RBAC on Order reads.
- Generic circuit breakers (Kafka retry + readiness covers current bottlenecks).
- Broad configuration hot reload (rolling restart is sufficient; log-level reload is the only dynamic knob).

## Stages

v1.1 is structured as six sequential stages with explicit dependencies.

### Stage v1.1.pre — Critical bug fixes

Hotfix branch, shipped before any other v1.1 work.

| Change | File |
|---|---|
| Add `sync.WaitGroup` to saga TTL sweep and outbox poller goroutines; wait on shutdown | `services/saga/cmd/saga/main.go:128-131, 203-213` |
| Replace `mustMarshal` panic with `return error` | `services/saga/internal/watchdog/ttl_sweep.go:159-164` |

**Exit criteria:** both bugs fixed; `go test ./services/saga/...` green; tag `v1.1.pre`.

**Why before everything else:** these bugs can mask or invalidate other tests if not fixed first. Saga shutdown behavior leaks in-flight transactions on rolling deploys; the panic can leave a pgx tx indeterminate on marshal failure (currently impossible in practice but the panic is still a foot-gun).

### Stage v1.1.a — Drift cleanup + Quick wins

**Drift cleanup:**

- Refactor `docs/adr/0003-rest-vs-grpc.md` to describe the actual architecture: REST external, events-via-Kafka internal, no gRPC.
- Delete dead packages: `services/order/internal/saga/doc.go`, `services/inventory/internal/redis/doc.go`.
- Sync OpenAPI spec → code: remove `DELETE /v1/orders/{id}`, `/readyz`, `POST /v1/inventory/release` from `api/openapi.yaml` (these are not implemented; this removes 16+ drift points).
- Remove stale "sub-stage 3.x.x" comments from 75+ files; replace with `// (v1.0+)` or simply delete.
- Update `README.md`: remove "services don't connect to compose", remove "planned" sections, correct workspace counts (currently says 1 platform + 3 services, actually 1 platform + 4 services + shared modules).

**Quick wins (bundled in the same PR to limit churn):**

- Replace `slog.Default()` with `platform.NewLogger()` in all four service binaries.
- Set OTel `service.name` to `order` / `payment` / `inventory` / `saga` (currently outbox table names).
- Fix LDFLAGS linker target: inject `services/order/cmd/order.Version` etc. (currently injects `main.Version` but the variable lives in an imported package).
- Add HTTP server timeouts (`ReadHeaderTimeout`, `ReadTimeout`, `WriteTimeout`, `IdleTimeout`) on Order, Payment, Inventory. Saga already has `ReadHeaderTimeout`; standardize.
- Delete `var _ = time.Second` workaround at `pkg/consumer/deduper.go:54`.
- Delete `TestEnvOrDefault_NotExported` placeholder at `services/order/cmd/order/main_test.go:86`.
- Remove hardcoded `C:\Users\t0p_m\...` path at `tests/k8s/smoke_test.go`; use `PATH` lookup.

**Exit criteria:** `make verify` green; OpenAPI lint clean; ADR-0003 and `STATUS.md` updated; tag `v1.1.a`.

### Stage v1.1.b — Delivery reliability

**Outbox SKIP LOCKED + retry classification:**

- Add `lease_until TIMESTAMPTZ` column to all four service outbox tables (`order_outbox`, `payment_outbox`, `inventory_outbox`, `saga_outbox`) via new migrations.
- Rewrite all four `fetchPending.sql` queries to claim rows with `FOR UPDATE SKIP LOCKED` and set `lease_until = NOW() + lease_duration` (default 30s).
- `pkg/outbox/poller.go`: claim → publish → `MarkSent` OR release lease (no row loss on process crash mid-publish; another replica picks it up after lease expiry).
- Error classification: `IsTransient(err)` for Kafka connectivity (broker down, timeout, network). `IsPermanent(err)` for invalid payload, unknown topic, schema mismatch.
- Capped exponential backoff with jitter for transient errors. Do not consume retry-budget on transient errors.
- Only route to DLQ on permanent errors. Only mark `FAILED` after successful DLQ publish.

**Durable consumer dedupe:**

- New `consumer_dedupe (consumer_group TEXT, event_id UUID, processed_at TIMESTAMPTZ, PRIMARY KEY (consumer_group, event_id))` table per service database.
- Replace `pkg/consumer/deduper.go` in-memory implementation with `PGDeduper` (Postgres-backed; uses `INSERT ... ON CONFLICT DO NOTHING` for dedupe marking).
- Wire `PGDeduper` into all four consumer runners.
- Mark dedupe rows atomically with the business state mutation in the same transaction (`INSERT INTO consumer_dedupe ... ; UPDATE orders ...` in one `BEGIN/COMMIT`).

**Test coverage (closed gaps):**

- Unit tests for all six saga consumer handlers in `services/saga/internal/consumer/handlers.go`: `OrderCreated`, `StockReserved`, `PaymentCompleted`, `PaymentFailed`, `StockReleased`, `StockReservationFailed`.
- Unit tests for `PaymentRequested` handler in `services/payment/internal/consumer/handlers.go`.
- Unit tests for `StockReserveRequested` handler in `services/inventory/internal/consumer/handlers.go`.
- Tests for `services/payment/internal/repository/pg_repo.go` (currently no tests).
- Tests for compensator idempotency in `services/saga/compensate.go`.
- Test for saga TTL sweep concurrency: two sweep goroutines against the same expired saga produce exactly one compensation.
- Prometheus alert rules at `deploy/observability/alerts.yml`: outbox-oldest-pending-age, kafka-consumer-lag, saga-compensation-rate, dlq-count, http-5xx-rate.

**Chaos test (Toxiproxy-based):**

- New `tests/chaos/kafka_outage_test.go`:
  1. Start services through Toxiproxy endpoint for Kafka.
  2. Disable Kafka via Toxiproxy **before** POST /v1/orders.
  3. POST /v1/orders → assert 201.
  4. Assert `order_outbox` has a `PENDING` row.
  5. Assert `/healthz` returns 200.
  6. Wait longer than several normal poll intervals.
  7. Assert row is still `PENDING`, no DLQ rows.
  8. Re-enable Kafka via Toxiproxy.
  9. Assert row → `SENT`.
  10. Assert order → `confirmed`.
  11. Assert no unintended DLQ rows.
- New `tests/chaos/process_restart_test.go`: order in flight, kill -9 saga binary, restart, assert order reaches `confirmed`.

**ADR-0002 update:** describe the actual semantics — `FOR UPDATE SKIP LOCKED` claim, lease-based ownership, transient vs permanent classification, durable Postgres-backed dedupe.

**Files (main):**
- `pkg/outbox/{poller,kafka,types}.go`
- `pkg/consumer/{consumer,deduper}.go`
- `services/*/internal/outbox/{source,postgres}.go`
- New migrations for lease and dedupe tables
- `services/saga/internal/watchdog/ttl_sweep.go`
- New handler tests
- `tests/chaos/{kafka_outage,process_restart}_test.go`
- `tests/harness/harness.go` (add Toxiproxy integration)
- `docs/adr/0002-outbox-pattern.md`
- `deploy/observability/alerts.yml`

**Exit criteria:** chaos test passes deterministically; SKIP LOCKED proven with 2 replicas; dedupe idempotent across restart; alert rules defined; ADR-0002 updated; tag `v1.1.b`.

### Stage v1.1.c — Images + GHCR

**Dockerfile (multi-stage):**

- Single parameterized `Dockerfile` at repo root with `ARG SERVICE=order|payment|inventory|saga`.
- Stage 1: `golang:1.25.13-alpine` builder.
- Stage 2: `gcr.io/distroless/static:nonroot` runtime.
- `CGO_ENABLED=0`, static binary.
- OCI labels: `org.opencontainers.image.revision`, `.source`, `.version`.
- LDFLAGS injection uses fully qualified linker symbols per service.
- Multi-arch: `linux/amd64` + `linux/arm64`.

**`.dockerignore`** at repo root: exclude `.git`, `bin/`, `tests/logs/`, `*.exe`, `docs/superpowers/sdd/`.

**Makefile targets:**

- `make docker-build` (all four)
- `make docker-build-<svc>` (one)
- Local tag: `dev-<short-sha>`

**GHCR publish workflow:**

- New `.github/workflows/publish-images.yml`.
- Triggers: PR (build only, no push), push to `main` (push `sha-<commit>`), tag `v*.*.*` (push semver + `1.1` + `1` + `latest`).
- Matrix over four services.
- Permissions: `contents: read`, `packages: write`, `id-token: write` (for cosign keyless), `attestations: write`.
- SBOM: `docker/build-push-action` with `provenance: true`, `sbom: true`.
- Vulnerability scan: Trivy, fail release on CRITICAL.
- Cosign keyless signing for tag-releases.

**Helm update:**

- Bump `image.tag` from `0.2.0` to `1.1.0` in all four service values.
- `imagePullPolicy: IfNotPresent` for local kind, `Always` for prod (or digest-pinned).

**Files (main):**
- New `Dockerfile`, `.dockerignore`
- New `.github/workflows/publish-images.yml`
- Updated `Makefile`
- All four `deploy/helm/orderflow-*/values.yaml`

**Exit criteria:** all four images build from a clean checkout; version injection works (visible in `--version` flag or labels); PR image-build job green; image digest published on main push; tag `v1.1.c`.

### Stage v1.1.d — Deployable Kubernetes

**Production migrations (goose):**

- New `cmd/migrate/main.go` per-service binary: `go run ./cmd/migrate -service order`.
- Convert existing SQL files to goose format (`+goose Up` annotation; one up-block per file).
- Helm pre-install/pre-upgrade Job per service: `orderflow-migrate-order`, `orderflow-migrate-payment`, `orderflow-migrate-inventory`, `orderflow-migrate-saga`.
- Helm chart Job template at each service Helm chart with `backoffLimit`, `activeDeadlineSeconds`.

**Real `/readyz`:**

- Chi handler `/readyz` in each of the four `main.go`: pings DB, calls franz-go metadata fetch (Kafka readiness), pings Redis if configured.
- `/healthz` remains liveness (process up).
- Saga, Order, Payment, Inventory all add the handler.

**Redpanda topic init:**

- InitContainer or Job: creates three topics (`order-events`, `payment-events`, `inventory-events`) idempotently on install.
- Reuse `deploy/kafka/create-topics.sh` as the base script.

**Helm secrets:**

- `existingSecret` support for `DATABASE_URL`, `KAFKA_BROKER`, `REDIS_URL`, `OTEL_EXPORTER_OTLP_ENDPOINT` in all four service charts.
- Postgres chart: switch to `existingSecret` instead of plaintext password in values.
- RBAC narrowing: remove broad `secrets/pods/configmaps get` from `deploy/k8s/base/rbac.yaml`.

**HTTP server timeouts (if not done in v1.1.a):** standardize across all four services.

**NetworkPolicies tightening:**

- Egress rules by port: 5432 (postgres), 6379 (redis), 9092 (kafka), 4317 (otel).
- Per-service NetworkPolicies (not only namespace-wide).

**kind smoke upgrade:**

- Rewrite `tests/k8s/smoke_test.go`:
  1. Build all four images.
  2. Create kind cluster.
  3. Load images via `kind load docker-image`.
  4. Install infra charts (postgres, redis, redpanda).
  5. Run migration Jobs.
  6. Install four service charts.
  7. Wait for all rollouts (`kubectl rollout status`).
  8. Port-forward Order Service.
  9. POST order.
  10. Poll until `confirmed`.
  11. On failure: collect `kubectl get all`, pod descriptions, events, current/previous logs, helm release status → upload as artifacts.
- Linux-only CI job (`tests/k8s:` job, independent of build matrix).

**Files (main):**
- New `cmd/migrate/main.go`
- Converted migrations in all four `services/*/migrations/`
- Migration Job templates in each Helm chart
- Redpanda init job
- `/readyz` handler in all four `services/*/cmd/*/main.go`
- `deploy/k8s/base/network-policies.yaml`, `rbac.yaml`
- All four service Helm `values.yaml`
- `deploy/helm/orderflow-postgres/values.yaml`
- `tests/k8s/smoke_test.go`
- `.github/workflows/ci.yml`

**Exit criteria:** `make kind-up && make smoke-k8s` brings up a real kind cluster with real images and confirms an order end-to-end. Migration Jobs idempotent. `/readyz` returns 503 when Kafka is unavailable. Tag `v1.1.d`.

### Stage v1.1.e — Release gates

- Update `STATUS.md`: add `v1.1.{pre,a,b,c,d,e}` rows.
- Update `CHANGELOG.md`: new `## [1.1.0] - 2026-XX-XX` section.
- Update `README.md`: "Status: v1.1.0", quickstart with GHCR images.
- All gates green: `make verify`, `make e2e`, `make smoke-k8s`.
- Update ADR log table in README.
- Tag `v1.1.0-rc.1`, run all gates, tag `v1.1.0`.
- GHCR publishes all four immutable images with digest in release output.
- Closing commit.

**Exit criteria:** tag `v1.1.0` on `main`; all four image digests published; `README`/`CHANGELOG`/`STATUS` synchronized.

## Cross-cutting decisions

| Decision | Value |
|---|---|
| Stage naming | `v1.1.pre` plus `v1.1.{a,b,c,d,e}` |
| OpenAPI sync direction | spec → code (remove unimplemented endpoints) |
| Migration tool | goose |
| Chaos strategy | Toxiproxy |
| ADR-0003 outcome | refactor — "events-via-Kafka" for service-to-service |
| ADR-0002 outcome | update for SKIP LOCKED + lease + durable PG dedupe |
| Multi-replica support | enabled with v1.1.b (two or more replicas safe) |
| Logger | `platform.NewLogger()` JSON across all services |
| OTel service.name | `order` / `payment` / `inventory` / `saga` (not table names) |
| LDFLAGS symbol | fully qualified per-binary (`services/order/cmd/order.Version`) |

## Critical path

```
v1.1.pre  (critical bugs)
   │
   ▼
v1.1.a  (drift + quick wins)
   │
   ▼
v1.1.b  (reliability + chaos + dedupe)
   │
   ▼
v1.1.c  (Dockerfiles + GHCR)
   │
   ▼
v1.1.d  (migrations + readiness + Helm secrets + kind smoke)
   │
   ▼
v1.1.e  (release gates + tag v1.1.0)
```

Each stage produces a tag. v1.1.d requires v1.1.c; everything else is strictly sequential.

## Risk register

| Risk | Mitigation |
|---|---|
| Goose migration conversion breaks existing data | Convert to idempotent-only; test on testcontainers; add upgrade test from v1.0 schema |
| Toxiproxy may not work on Windows CI runners | Chaos test runs only on ubuntu CI; local Windows execution is optional |
| Durable dedupe migration adds latency | Measure in e2e; if p99 > 50ms, consider batch claim |
| Cosign keyless requires OIDC trust | Document; fall back to shared-secret signing if GHCR rejects |
| Two-replica SKIP LOCKED may have issues with pgx 5.10 | Run concurrency test in v1.1.b before commit; pin pgx version or add workaround if broken |
| GHCR namespace `t0pm1x` may not match publish permissions | Confirm with user before stage v1.1.c starts |

## Open questions

These are not blocking but should be answered before the relevant stage starts:

1. **Migration tool integration** — goose embedded as a library, or shell-out to goose CLI in the Job? Recommendation: embed.
2. **Cosign signing** — keyless (OIDC) or shared-secret? Recommendation: keyless; fall back if rejected.
3. **GHCR visibility** — public or private packages? Recommendation: public for OSS portfolio.
4. **Kustomize status** — keep alongside Helm, or deprecate (base currently only deploys a namespace)? Recommendation: keep but document as "Helm is canonical; Kustomize is for ArgoCD overlays only".
5. **Saga database** — current code points saga at Order Postgres URL. Should saga have its own database in v1.1? Recommendation: yes, but defer to v1.2 (out of scope for v1.1).
6. **API-key middleware scope** — protect only POST /v1/orders, or also GET endpoints? Recommendation: POST /v1/orders + POST /v1/payments/webhook (HMAC, not API key); GETs remain open within the cluster.

## References

- `STATUS.md` — current v1.0.0 status
- `CHANGELOG.md` — version history
- `docs/superpowers/portfolio/orderflow-substages.md` — closed v1.0 sub-stage index
- `docs/superpowers/portfolio/orderflow-checkpoint.md` — session handoff
- `docs/adr/0002-outbox-pattern.md` — to be rewritten in v1.1.b
- `docs/adr/0003-rest-vs-grpc.md` — to be rewritten in v1.1.a
- `api/openapi.yaml` — to be trimmed in v1.1.a