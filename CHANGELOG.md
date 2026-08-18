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

## [1.1.2] - 2026-08-18

### Fixed — adversarial-audit follow-up

A senior/staff-level adversarial audit (see `docs/superpowers/portfolio/`)
uncovered one critical regression and three production-stopping bugs
that the v1.1.0-pre batch missed. All fixes preserve the existing
API surface; new handler-level tests close the testing gap that
allowed the regression to slip past.

- **CRITICAL — outbox poller never marked rows SENT** (v1.1.0-pre
  regression). The refactor that moved publish-and-mark into a
  RunInTx closure under FOR UPDATE SKIP LOCKED also moved the
  MarkSent call site out of the closure without wiring the new
  tx-aware MarkSentTx. The poller was returning nil from the
  closure, the row's status flip never happened, and the next
  poll re-fetched and re-published the same rows forever. Fix:
  collect the batch's event IDs and call src.MarkSentTx inside
  the closure on success — the row's status flip and the row's
  lock release commit atomically.

- **CRITICAL — saga handlers re-emitted downstream events on
  Kafka redelivery** (4 of 6 handlers). StockReservedHandler,
  PaymentCompletedHandler, PaymentFailedHandler, and
  StockReservationFailedHandler called an unguarded
  UpdateStateTx (matching any state) and then unconditionally
  emitted a downstream event. On every Kafka rebalance /
  restart / duplicate delivery, the handler:
  1. bumped updated_at (no-op on state)
  2. emitted a fresh outbox row

  Worst case: a second PaymentRequested for the same order
  caused a second provider.Charge() in the payment service
  (the DB-level payments.order_id dedupe runs AFTER the charge).
  Fix: new repository method TransitionStateTx(ctx, tx, orderID,
  from, to) that returns (true, nil) on transition, (false, nil)
  when state is already past `from` (idempotent replay — handler
  must NOT emit a downstream event in this branch). Each of the
  4 non-idempotent handlers now checks the result and skips
  outbox emission on a false return.

- **CRITICAL — TTL sweep `compensate` had no state guard**. A
  saga already in `state='compensated'` (e.g. set by
  PaymentFailedHandler between the sweep's SELECT and UPDATE)
  would still match the sweep's UPDATE, and the sweep would
  emit a second StockReleaseRequested + OrderCancelled. Fix:
  add `AND state NOT IN ('completed', 'compensated')` to the
  WHERE clause, check RowsAffected, and skip outbox emission
  when 0.

- **CRITICAL — `ReleaseStock` SQL had no `WHERE reserved >= qty`
  guard**. A buggy producer emitting a larger release than the
  original reservation would drive stock_items.reserved negative
  and inflate available on every subsequent release — permanent
  counter corruption. Fix: add `AND reserved >= \$2` to the
  WHERE clause; RowsAffected=0 (over-release or unknown SKU)
  returns ErrNotFound so the consumer ack-skips.

- **Payment webhook had no terminal-state guard** (P1-#1). A
  late webhook with status='failed' against a payment already
  in StatusCaptured would overwrite the status AND emit
  PaymentFailed. Fix: new Repository method
  UpdateStatusFromNonTerminal(id, to, events) that runs
  UPDATE … WHERE id=\$1 AND status NOT IN ('captured',
  'failed'); RowsAffected=0 when the row is already terminal, in
  which case no outbox row is emitted.

- **Order consumer's terminal-state guard excluded 'failed'**
  (P1-#2). Pre-v1.1.1 SQL excluded only 'confirmed' and
  'cancelled', letting a late OrderConfirmed event resurrect a
  'failed' order to 'confirmed'. StateFailed is reachable via
  the saga TTL sweep, so it must be protected by the same guard.
  Fix: add 'failed' to the NOT IN clause.

- **Idempotency middleware cached empty body on empty response**
  (P1-#7). A handler that returned without writing (panic
  recovered, ctx cancelled mid-flight, or simply a no-op
  handler) had its empty body cached via Complete(). The next
  retry got HTTP 200 with body='', falsely reporting success
  to the saga. Fix: if buf.status==0, call Release() instead of
  Complete() so the next retry hits the handler.

- **Idempotency middleware crashed on handler panic**. A panic
  inside next.ServeHTTP propagated out of the middleware,
  leaving the in-flight marker in Redis for the full TTL (24h
  by default) — operators saw 409s pile up. Fix: wrap
  next.ServeHTTP in defer recover(), log, and Release the
  reservation so a retry can succeed once the underlying bug
  is fixed.

- **Consumer dispatch crashed on handler panic** (P1-#6). A
  single panic killed the consumer goroutine silently and
  events piled up in Kafka retention until Kubernetes noticed
  the failed liveness probe. Fix: defer recover() in dispatch,
  log with event_id / event_type / offset / partition, mark the
  record for commit so the panic doesn't loop on retry.

- **Webhook had no max body size** (P1-#9). json.NewDecoder on
  r.Body would allocate the entire JSON object in memory before
  any size check. Fix: cap the body at 64 KiB via
  http.MaxBytesReader.

- **Global vars `globalHandler` / `globalDeps` were plain
  pointers** (P1-#10). Go's memory model doesn't guarantee that
  a pointer write is visible atomically to readers on other
  CPUs — a concurrent read in the consumer goroutine could
  observe a torn pointer. Fix: switch to atomic.Pointer[Handler]
  / atomic.Pointer[handlerDeps]. The Store/Load semantics are
  well-defined across goroutines and lock-free on the hot
  path (every Kafka record).

- **events.Client.Publish ignored ctx**. The convenience
  Publish method used context.Background() and a slow Kafka
  produce kept the service goroutine alive past the SIGTERM
  grace period. Fix: Publish now takes ctx and forwards it to
  PublishRaw.

### Tests
- `services/saga/internal/consumer/handlers_idempotency_test.go`:
  the first handler-level (not just registry-level) tests in
  the saga package. They exercise the FULL handler with a real
  PostgreSQL transaction and would have caught P0-#2 if they
  had existed.

- `services/payment/internal/webhook/handler_test.go`:
  TestWebhook_TerminalGuard_LateFailedAfterCaptured,
  TestWebhook_TerminalGuard_LateCapturedAfterFailed,
  TestWebhook_TerminalGuard_SameStatusReplay.

- `pkg/consumer/consumer_test.go`:
  TestDispatch_RecoversHandlerPanic.

- `services/payment/internal/idempotency/middleware_test.go`:
  TestMiddleware_EmptyBodyReleases,
  TestMiddleware_HandlerPanicRecovers.

- `pkg/outbox/poller_test.go`: extended
  TestPoller_PollsAndPublishesOnce to assert MarkSentTx was
  called. The test now FAILS if MarkSentTx is removed (catches
  the v1.1.0-pre regression).

### Deferred to v1.2+
- P1-#3: per-Pod attempts counter (sync.Map in the poller). The
  saga's DB attempts column is incremented by MarkFailedTx but
  unused for the DLQ decision; order/payment/inventory don't
  even have an attempts column. Fix requires schema changes
  (0002 migration for each of the 3 outbox tables) plus
  refactoring the poller to read attempts from the row.
- P1-#4: KafkaPublisher.Publish is documented as "batched into
  a single Kafka producer transaction" but actually makes N
  separate PublishRaw calls. Fix requires adding
  BeginTransaction/EndTransaction to the KafkaClient interface
  and adopting franz-go's transaction API.
- Webhook body-size-vs-strict-mode + idempotency-key body
  mismatch (Stripe-style 422).

## [1.1.1] - 2026-08-18

### Fixed — Senior-review pass

This batch addresses findings from a Staff/Senior-level audit
covering distributed-systems correctness, concurrency, and
observability. The platform was previously end-to-end functional
but unsafe under realistic failure modes (multiple replicas, restart
loops, partial writes, retry storms). All changes preserve the
existing API surface and saga state machine.

- **Consumer offsets never committed** (`pkg/consumer/consumer.go`):
  `CommitMarkedOffsets` was a no-op because no record was ever
  marked. Added `MarkCommitRecords(rec)` after every successful
  dispatch (and after DLQ exhaustion) so franz-go actually
  advances the offset on each commit.
- **No deduper wired in any service**: even with offsets fixed, the
  in-memory deduper lost state on restart. Added
  `pkg/consumer.RedisDeduper` (7-day TTL) and a
  `NewDeduperFromRedisURL` helper; all 4 service binaries accept
  and wire a deduper. Falls back to `NoopDeduper` when REDIS_URL
  is unset so local development keeps working.
- **Saga state-update and outbox.Append on separate transactions**:
  every saga handler in `services/saga/internal/consumer/handlers.go`
  could leave the saga advanced without the matching event emitted
  on a transient failure. Refactored to wrap state-update +
  emit in a single `pgx.BeginFunc`. Added tx-aware
  `InsertTx`/`UpdateStateTx`/`GetTx` to the saga repository.
- **Inventory never released stock on compensation**:
  `StockReleaseRequestedPayload` carried only `OrderID` and
  `ReservationID`, so the inventory handler couldn't decrement
  `stock_items.reserved`. Every cancelled order leaked a phantom
  reservation. Added `SKU` and `Quantity` to the payload; saga
  emits one StockReleaseRequested per item; inventory now calls
  `repo.ReleaseStock` atomically with the outbox row.
- **Outbox poller double-published on multi-replica deploys**:
  plain `SELECT ... WHERE status='PENDING'` had no row lock.
  Two replicas would both pick up the same rows. Added
  `FOR UPDATE SKIP LOCKED` and changed the `Source` interface to
  `RunInTx` so fetch + publish + mark happen under one row lock.
- **Payment events published to wrong topic**: `const topic = "order-events"`
  in the payment consumer contradicted the spec / Helm. Now
  publishes to `payment-events`; saga subscribes to it.
- **Idempotency in-flight duplicates returned 200 with `"in-flight"`
  body**: introduced `ErrInFlight` and mapped it to HTTP 409
  Conflict. Stripe-style concurrent-retry semantics.
- **`docker compose up` failed with "relation does not exist"**:
  init scripts only created extensions. Mounted per-service
  migrations and apply them after extensions.
- **`Repository.Insert` ignored request context**: client
  cancellation mid-write still committed. Added ctx to the
  Repository interface; PGRepo honors it.
- **Order consumer flipped cancelled orders back to confirmed** on
  redelivered events: added `WHERE state NOT IN ('confirmed', 'cancelled')`
  guard on `UPDATE orders`.
- **Payment consumer deduped on paymentID (fresh UUID per delivery),
  never on order_id**: added migration `0003_payment_order_unique.sql`
  (UNIQUE constraint) and switched to `ON CONFLICT (order_id) DO NOTHING`.
- **Consumer goroutines not tracked**: under Kubernetes rolling
  deploys, main could exit while the consumer was still processing.
  All 4 runners now take a `*sync.WaitGroup` and block shutdown on
  it.

### Housekeeping
- README port claims now match code/Helm defaults (8081/8082/8083/8084).
- Dropped `StatePaymentPending`/`StatePaymentComplete` saga constants
  (declared but never wired in `transitionTable`).
- Saga consumer `itemsJSON` error now propagated (was `_ =`).
- Repository `List` clamp widened to 500 to match handler.
- pkg/consumer swallowed errors now logged via slog.Warn.
- `.gitignore` extended for Windows redirect targets (`nul`),
  demo recording artifacts, and stray service logs.

### Deferred (v1.2+)
- Outbox-retry chaos assertion (currently asserts only that the
  order service stays healthy after Kafka kill; full recovery
  assertion blocked on broker-discovery in service binaries).
- ghcr.io publishing pipeline.

## [1.1.0-pre] - 2026-08-17

### Fixed
- **Saga shutdown goroutine leak**: The saga binary's outbox poller, TTL sweep, and HTTP server goroutines were launched without `sync.WaitGroup` tracking. On SIGTERM, `Run()` returned before the goroutines had exited, allowing a rolling Kubernetes deploy to kill a poller iteration or TTL sweep mid-transaction. The saga service now mirrors the `order`/`payment`/`inventory` shutdown pattern: a `sync.WaitGroup` tracks the poller, TTL sweep, and HTTP server goroutines, and the close path waits on a 5-second shutdown context before closing the DB pool.
- **`mustMarshal` panic in TTL sweep compensation**: `services/saga/internal/watchdog/ttl_sweep.go` called `panic(err)` from a `json.Marshal` failure inside `pgx.BeginFunc`. A panic there could leave the surrounding transaction in an indeterminate state. The helper has been removed; `compensate` now uses inline `json.Marshal` and propagates errors through the function's normal `error` return.

### Changed
- `startSagaOutbox` signature now takes a `*sync.WaitGroup` so its poller goroutine is tracked alongside the TTL sweep and HTTP server.
- New private helper `wgWait` consolidates the saga shutdown sequence (wait goroutines, close outbox, close consumer, shutdown HTTP, close pool).

## [1.0.0] - 2026-08-17

### Added — v1.0 release

- **kind smoke test** (`v1.0.kind`): New `tests/k8s/smoke_test.go` creates a real kind cluster using `deploy/kind/kind.yaml`, waits for nodes ready, validates that all 4 service Helm charts + the infra postgres chart render to valid YAML. Skippable via `KIND_SKIP=1` or `-short`. New `make smoke-k8s` target. CI will run it.
- **Asciinema recording** (`v1.0.demo`): `docs/demo/record.sh` + `docs/demo/RECORDING.md` document how to capture the demo as an asciinema `.cast` file. New `make record` target. Manual step (requires asciinema binary install — not automated in CI).

### Platform status at v1.0

- ✅ All 4 services (`order`, `payment`, `inventory`, `saga`) compile + run with PGRepository + REST API + real consumer handlers.
- ✅ End-to-end event flow: `POST /v1/orders` → OrderCreated → saga → StockReserveRequested → inventory reserves → StockReserved → saga → PaymentRequested → payment → PaymentCompleted → saga → OrderConfirmed → order=confirmed.
- ✅ Saga recovery: cross-restart TTL sweep compensates stuck sagas.
- ✅ W3C tracecontext through Kafka (kafkaprop module + outbox + consumer + chi middleware + service.version resource).
- ✅ Helm charts for all 4 services + 3 infra deps.
- ✅ Kustomize overlays (dev/staging/prod) with HPA + PDB for prod.
- ✅ ArgoCD ApplicationSet for GitOps delivery.
- ✅ testcontainers harness + E2E tests (happy + compensation + chaos) + k6 load test.
- ✅ CI: build matrix + E2E job (ubuntu-only, `needs: build`).
- ✅ 4 ADRs + 5 C4 diagrams + demo script + recording runbook.
- ✅ Binaries report real git version (LDFLAGS injection).

### Deferred to v1.1
- Full outbox-retry chaos assertion (services cache `KAFKA_BROKER`).
- kind smoke: actual image loading into cluster (currently validates Helm rendering only).
- ghcr.io publishing pipeline.

## [0.6.0] - 2026-08-17

### Added
- **Saga cross-restart TTL sweep**: New `services/saga/internal/watchdog` package with `TTLSweep` that periodically queries `order_sagas WHERE expires_at < NOW() AND state NOT IN ('completed', 'compensated')` and emits `StockReleaseRequested` + `OrderCancelled` (reason="timeout") for each expired saga. Wired into `cmd/saga/main.go` to run every 30s in the same goroutine as the consumer + outbox poller. Survives saga-binary restarts — any non-terminal saga past its 5-minute TTL gets compensated automatically.
- New repository method `PGRepo.ListExpired(ctx, limit)` — non-terminal expired sagas, ordered by `expires_at ASC`, bounded slice. TDD verified.

### Deferred to v1.0
- 3.12.f kind smoke test.
- 3.13.d asciinema recording.
- Full outbox-retry chaos assertion.

## [0.5.0] - 2026-08-17

### Added — platform now actually works end-to-end

The saga runtime and all consumer handlers are wired up. The platform can now process orders from `POST /v1/orders` through to `confirmed`.

- **Saga runtime** (`v0.5.0.saga`): Consumer subscribes to `order-events`, handles `OrderCreated` (start saga → emit `StockReserveRequested`), `StockReserved` (advance → emit `PaymentRequested`), `PaymentCompleted` (advance → emit `OrderConfirmed`), `PaymentFailed` (compensate → emit `StockReleaseRequested` + `OrderCancelled`), `StockReleased` (cleanup), `StockReservationFailed` (cancel). `PGRepository` over `order_sagas` table + new `saga_outbox` table + outbox poller publishing to Kafka. Watchdog code present but cross-restart TTL sweep deferred.
- **Inventory consumer** (`v0.5.0.inventory`): Real `StockReserveRequested` handler calls `repo.ReserveStock` and emits `StockReserved` (or `StockReservationFailed` on `ErrInsufficientStock`). Real `StockReleaseRequested` handler emits `StockReleased`. New `POST /v1/inventory/reserve` endpoint for synchronous reserve.
- **Payment consumer** (`v0.5.0.payment`): Real `PaymentRequested` handler calls `provider.Charge` (mock — last-4 derived from order_id), persists `payments` row + outbox event in same tx. Emits `PaymentCompleted` on `succeeded`, `PaymentFailed` on `failed`.
- **Order consumer** (`v0.5.0.order`): Real handlers for `StockReserved` (state=reserved), `StockReservationFailed` (state=cancelled), `OrderConfirmed` (state=confirmed), `OrderCancelled` (state=cancelled), `PaymentFailed` (state=cancelled). Order subscribes to all 3 topics (order-events, payment-events, inventory-events).

### Housekeeping
- `3.4.1` Committed missing `cmd/saga/go.sum` and `services/saga/go.sum` (existed since 3.10.d.saga but were untracked).
- `3.4.2` LDFLAGS injection: `make build` now bakes `git describe --tags --always --dirty` into each binary's `main.Version` field. Binaries report real version (e.g. `v0.4.0-3-gcf3195d-dirty`) instead of `0.0.0-dev`.

### Deferred to v0.6.0
- 3.12.f kind smoke test.
- 3.13.d asciinema recording.
- Saga watchdog cross-restart TTL sweep.
- Full outbox-retry chaos assertion (services cache `KAFKA_BROKER`).

## [0.4.0] - 2026-08-17

### Added
- **3.5.g** Payment Service complete: `POST /v1/payments/webhook` handler with `PaymentCompleted`/`PaymentFailed` event emission, `PGRepository` (tx-atomic UpdateStatus + outbox writer), wired into the payment binary's chi router at `/v1/payments/webhook` when DB+Redis wired. Idempotency middleware wraps the webhook when `REDIS_URL` is set (logs warning if not). 11 stdlib tests in `handler_test.go` cover happy path, status variants, error_code derivation from last-4 (matches `provider.Charge` table), 400/404/500 cases.
- **3.6.g** Inventory Service: `PGRepository` with `GetStock/ReserveStock/ReleaseStock` (optimistic version check via `UPDATE ... WHERE version = $1`, tx-atomic outbox INSERT via `svcoutbox.PGWriter.Append`), `GET /v1/inventory/stock/{sku}` mounted in inventory binary. 3 stdlib tests (round-trip, outbox-event atomicity, state-filtered List) — verified against live `postgres:16-alpine` via Docker.
- **3.11.polish** `make e2e` aggregate target + `e2e-happy`, `e2e-compensation`, `e2e-chaos` sub-targets. New `harness.RestartKafka(ctx)` helper for chaos recovery tests.

### Deferred to v0.5.0
- 3.12.f kind smoke test (`kind` binary not installed locally).
- 3.13.d asciinema recording.
- Full outbox-retry chaos assertion — services cache `KAFKA_BROKER` env at startup; restarting Kafka doesn't reconnect already-running service processes. Either the chaos test should restart services too, OR services should support dynamic broker discovery (separate concern).

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
