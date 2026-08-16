# orderflow — Status

**Last updated:** 2026-08-17 (3.3.a-b)

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

## Next up

- 3.3.c OTel HTTP/gRPC middleware

## Notes

- 3.1.g extended the prompt's "5 endpoints" surface slightly: spec covers
  POST/GET/DELETE on `/v1/orders`, GET `/v1/orders` (list), POST
  `/v1/payments/webhook`, POST `/v1/inventory/reserve`, plus
  `/healthz` and `/readyz`. Total: 8 endpoints across 3 services.
- 3.1.h: 11 events + 1 EventEnvelope + 1 shared OrderItem struct,
  all 13 Go code blocks compile under `go vet`. All 11 JSON examples
  parse as valid JSON.