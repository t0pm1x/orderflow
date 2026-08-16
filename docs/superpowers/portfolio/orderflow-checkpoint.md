# orderflow MVP — Session Resume Checkpoint

**Created:** 2026-08-17
**Status:** v0.1.0-MVP reached (README + CHANGELOG committed; tag pending)
**Next session resumes at:** sub-stage 3.5.c (Payment idempotency)

## What's DONE (25 sub-stages)

### Bootstrap (3.0)
- [x] Monorepo: `go.work` with 9 modules (`pkg/platform` + 3 services + 4 cmd stubs)
- [x] All 4 service binaries compile (`make build` produces `bin/order.exe`, `bin/payment.exe`, `bin/inventory.exe`, `bin/saga.exe`)
- [x] `pkg/platform` initial module with logging

### Spec phase (3.1)
- [x] 3 C4 diagrams (System Context + Container + per-service Component)
- [x] 3 ADRs (saga, outbox, REST vs gRPC)
- [x] OpenAPI 3.0 for Order/Payment/Inventory endpoints
- [x] Domain events spec (11 events + envelope)

### Platform infrastructure (3.2)
- [x] docker-compose.yml (10 services: 3 postgres, redis, redpanda, kafka-init, otel-collector, prometheus, tempo, grafana)
- [x] postgres init scripts per service
- [x] redpanda topic init (4 topics)
- [x] prometheus/tempo/otel/grafana configs
- [x] k8s base manifests (namespace, RBAC, NetworkPolicies)

### Common library (3.3)
- [x] `pkg/platform/logging.go` — slog with OTel trace correlation
- [x] `pkg/platform/otel.go` — OTLP gRPC + stdout exporter init
- [x] `pkg/platform/middleware/middleware.go` — chi Stack
- [x] `pkg/platform/types/` — Money (int64 cents), typed UUIDs (OrderID, PaymentID, StockID, CustomerID)
- [x] `pkg/platform/events/` — Envelope + franz-go Client
- [x] `pkg/platform/errors/` — APIError type with sentinels

### Service skeletons + domain + 1 API
- [x] **Order Service** (most complete): domain state machine, REST API with 5 tests (in-memory mock repo), Order aggregate with NewOrder + Transition
- [x] **Payment Service** (partial): mock provider with deterministic behavior
- [x] **Inventory Service** (partial): Stock model with Reserve/Release + ErrInsufficientStock
- [x] **Saga Service**: stub only

## What's NOT done (next session's work)

### Continue from 3.5.c
- [ ] **3.5.c** Payment idempotency (Redis-backed dedupe)
- [ ] **3.6.c** Inventory optimistic locking (SQL UPDATE WHERE version=$1)
- [ ] **3.4.d/3.5.d/3.6.d** — outbox writers per service
- [ ] **3.4.e/3.5.e/3.6.e** — DB migrations
- [ ] **3.4.f/3.5.f/3.6.f** — tests

### Phases 7-13
- [ ] **3.7** Outbox poller + Kafka publisher with EOS
- [ ] **3.8** Consumer base + idempotent handler + DLQ + per-service handlers
- [ ] **3.9** Saga orchestrator (state machine + compensation + timeout)
- [ ] **3.10** Tracing propagation through Kafka headers
- [ ] **3.11** E2E tests (happy + compensation + chaos + load)
- [ ] **3.12** K8s Helm charts + Kustomize overlays + ArgoCD
- [ ] **3.13** Docs + demo script + asciinema

## Key contracts between sub-stages

When resuming, remember these interfaces that earlier sub-stages established:

### `pkg/platform` API (already exported)

```go
// Logging
logger := platform.NewLogger()  // JSON slog to stderr, respects LOG_LEVEL env
ctxLogger := platform.LogWithTrace(ctx, base)  // adds trace_id/span_id

// OTel
shutdown, err := platform.InitTracing(ctx, "service-name")
defer shutdown(ctx)
tracer := platform.Tracer("name")

// Middleware (chi)
r.Use(platform.Middleware.Stack("service-name", logger)...)

// Types
type Money int64  // cents, not float
type OrderID uuid.UUID  // typed wrappers

// Events
env, _ := platform.NewEnvelope("EventType", "Aggregate", "id", payload, traceID, spanID)
client.Publish("topic", env)

// Errors
var ErrNotFound = &apierrors.APIError{Status: 404, Code: "NOT_FOUND", ...}
apierrors.WriteError(w, err)
```

### Order Service repository interface (used by 3.4.c REST)

```go
type Repository interface {
    Insert(o *domain.Order) error
    Get(id types.OrderID) (*domain.Order, error)
    List(state domain.OrderState, limit int) ([]*domain.Order, error)
}
```

**When 3.4.d (outbox) implements this**, it must satisfy the interface AND wrap DB writes with event creation.

### Inventory Stock interface (used by future 3.6.c/d)

```go
type Stock struct {
    SKU, Available, Reserved, Version int64
}
func (s *Stock) Reserve(qty int) error  // returns ErrInsufficientStock or mutates
func (s *Stock) Release(qty int)
```

**3.6.c (lock)** wraps Reserve/Release in `UPDATE ... WHERE version = $1 RETURNING ...` SQL.

### Payment mock provider (3.5.b) contract

```go
func Charge(ctx context.Context, paymentID string, amountCents int64, lastFour string) (*Result, error)
```

Card last-4 behavior:
- `0001` → declined
- `0002` → insufficient funds
- `0003` → timeout
- anything else → success

**3.5.c idempotency** should wrap webhook calls in Idempotency-Key check before this provider.

## Architecture cheatsheet

```
orderflow/
├── cmd/{order,payment,inventory,saga}/  # service entry points
├── pkg/platform/                        # shared library
├── services/{order,payment,inventory,saga}/
│   ├── internal/{domain,api,outbox,consumer,saga}/
│   ├── migrations/
│   ├── cmd/<name>/main.go
│   └── go.mod (replace → ../../pkg/platform)
├── api/openapi.yaml
├── deploy/{docker-compose.yml, postgres/, kafka/, observability/, k8s/base/}
├── docs/{architecture/, adr/, superpowers/}
└── go.work
```

## Git/Repo state

- `main` branch
- 14 commits (3.0 → v0.1.0-MVP), one per sub-stage
- `v0.1.0-MVP` tag: **not yet created** (next-session housekeeping)
- Remote: confirm `github.com/t0pm1x/orderflow` exists; PAT stored at `C:\Users\t0p_m\Documents\dbguard-token.txt`

## Current open issues

- **gofmt:** all files need `gofmt -w .` — review found this; not yet applied
- **Untracked files:** `services/payment/internal/idempotency/{store.go, store_test.go}` from interrupted 3.5.c dispatch — functional, just not committed
- **Uncommitted changes:** `pkg/platform/go.{mod,sum}` and `services/{payment,inventory}/go.mod` modified by recent `go work sync` run during review
- **Tag missing:** `git tag v0.1.0-MVP` was planned but not run

## Quickstart for next session

```bash
cd C:\Users\t0p_m\projects\orderflow
git status              # see uncommitted changes
git log --oneline -10   # recent commits
git tag -l              # currently empty
cat docs/superpowers/portfolio/orderflow-checkpoint.md  # this file
```

Then resume at sub-stage 3.5.c:

```
Agent dispatch template:
"Execute orderflow sub-stage 3.5.c — Payment idempotency.
 Working directory: C:\Users\t0p_m\projects\orderflow
 Service module: services/payment
 Path: services/payment/internal/idempotency/
 Note: idempotency/store.go and store_test.go exist as untracked
 files from interrupted dispatch — verify they work, commit."
```

## Reference: critical files

- `pkg/platform/events/events.go` — Envelope type, Client (Kafka)
- `pkg/platform/errors/errors.go` — APIError + sentinels + WriteError
- `pkg/platform/middleware/middleware.go` — chi Stack
- `services/order/internal/domain/order.go` — Order aggregate
- `services/order/internal/api/handler.go` — chi router with Repository interface
- `services/order/internal/domain/state.go` — CanTransition table
- `services/payment/internal/provider/provider.go` — mock provider
- `services/inventory/internal/model/stock.go` — Stock aggregate

## Reference: docs

- `STATUS.md` — stage-by-stage status table (canonical)
- `docs/superpowers/specs/orderflow-spec.md` — canonical design
- `docs/superpowers/specs/orderflow-events.md` — event schemas
- `docs/superpowers/portfolio/orderflow-checkpoint.md` — this file
- `docs/superpowers/portfolio/orderflow-substages.md` — all 75 sub-stage cards (planned)
- `docs/superpowers/portfolio/REVIEW.md` — review log (planned)
- `docs/architecture/c4-level-{1,2,3-*}.puml` — C4 diagrams
- `docs/adr/{0001,0002,0003}-*.md` — ADRs
- `api/openapi.yaml` — REST contract