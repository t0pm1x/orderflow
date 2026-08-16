# orderflow — Status

**Last updated:** 2026-08-17 (3.6.a)

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
| 3.4.a | Order Service skeleton (cmd/order, internal package dirs, migrations) | done | 9ffb1cc |
| 3.5.a | Payment Service skeleton (cmd/payment, provider/idempotency/webhook/consumer/outbox stubs) | done | 67c399f |
| 3.6.a | Inventory Service skeleton (cmd/inventory, model/lock/redis/api/consumer/outbox stubs) | done | (this commit) |

## Next up

- 3.6.b Inventory domain (Stock aggregate with version column, optimistic lock SQL)

## Notes

- 3.1.g extended the prompt's "5 endpoints" surface slightly: spec covers
  POST/GET/DELETE on `/v1/orders`, GET `/v1/orders` (list), POST
  `/v1/payments/webhook`, POST `/v1/inventory/reserve`, plus
  `/healthz` and `/readyz`. Total: 8 endpoints across 3 services.
- 3.1.h: 11 events + 1 EventEnvelope + 1 shared OrderItem struct,
  all 13 Go code blocks compile under `go vet`. All 11 JSON examples
  parse as valid JSON.
- 3.3.c-d deviations from spec: (a) test file `types/middleware_test.go`
  in spec was mislabeled — placed at `middleware/middleware_test.go`
  because it tests the middleware package; (b) `NewMoneyFromMajor` now
  uses `math.Round` (spec comment said "bankers' rounding" but the
  literal impl would fail `TestMoney_FromMajor` for 19.99 due to float
  precision); (c) dropped unused `time` import from `types.go`.
- 3.4.a deviation from spec: spec listed `github.com/twmb/franz-go/pkg/kgo v1.21.6`
  as a require, but `pkg/kgo` is not a separate Go module at v1.21.6
  (it's a subpackage of the root `github.com/twmb/franz-go v1.21.6`
  module, which has no `pkg/kgo/go.mod`). Substituted the root module
  per pkg/platform convention. Subsequent sub-stages that import
  `github.com/twmb/franz-go/pkg/kgo` will resolve under this require.
  Spec's other requires (chi, pgx, goose, uuid, otelhttp) are declared
  as direct but unused by `cmd/order/main.go` — `go mod tidy` would
  strip them, so requires were re-added post-tidy to match the spec
  exactly; future sub-stages that actually import these packages will
  lock them in via their own tidy runs.