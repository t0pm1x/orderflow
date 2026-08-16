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

### Planned for v0.2.0
- DB migrations for all 3 services (3.4.e, 3.5.e, 3.6.e)
- Outbox writers (3.4.d, 3.5.d, 3.6.d) + shared poller/publisher (3.7)
- Kafka consumers for saga events (3.8)
- Saga orchestrator with compensation (3.9)
- Tracing propagation through Kafka headers (3.10)
- E2E tests with testcontainers (3.11)
- Helm charts (3.12)
- Production docs + demo script (3.13)
