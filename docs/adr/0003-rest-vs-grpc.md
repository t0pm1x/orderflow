# ADR-0003: REST for All Traffic (No gRPC)

- **Status:** Accepted (2026-08-17) — REST-only confirmed after v1.1.5 audit
- **Date:** 2026-08-17
- **Deciders:** orderflow architecture team

## Context

orderflow is a multi-service platform with two distinct kinds of traffic:

1. **Client traffic** — external callers (web, mobile, CLI, integration tests) talk to the platform via `POST /v1/orders`, `GET /v1/orders/{id}`, and the `POST /v1/payments/webhook` (provider callback). The spec §"API Surface" lists these endpoints. Clients are heterogeneous, the network is the public internet or a corporate VPN, and the cost of a new client SDK is high.
2. **Service-to-service traffic** — there is **no direct request/response RPC** between Order, Payment, Inventory, and Saga today. Coordination happens through Kafka topics: the order service publishes `OrderCreated`; saga subscribes and emits `StockReserveRequested`, `PaymentRequested`, etc.; downstream services react by consuming those events. The web BFF (services/web) is the only in-cluster HTTP client of the order/payment/inventory HTTP handlers, and its calls are still REST. The service-to-service traffic is in-cluster, on the same Kubernetes pod network.

The spec §"Stack Choices" originally read "RPC: gRPC for service-to-service where proto defined," and an earlier version of this ADR (pre-audit) committed to **REST external, gRPC internal**, citing `api/proto/*.proto` files that were never created. The v1.1.5 audit confirmed that:

- **No proto files exist.** `api/proto/` is absent; the only artifact under `api/` is `api/openapi.yaml`.
- **No gRPC is wired.** None of the service binaries import a gRPC server/client library. All service-to-service interaction is either HTTP (web BFF → order/payment/inventory) or Kafka event pub/sub.
- **All public endpoints are REST + JSON over HTTPS.** Health, metrics, OTel, and webhook callbacks are all HTTP.

This ADR therefore supersedes the earlier REST-external / gRPC-internal decision with a single-protocol reality: **REST for everything**, with gRPC explicitly deferred.

## Decision

**Use REST + JSON over HTTPS for all traffic — client-facing APIs, the BFF-to-service calls in services/web, and any future in-cluster request/response traffic. Defer gRPC adoption to v1.2+.**

- **REST is the only wire format.** `/v1/orders`, `/v1/payments/webhook`, `/v1/inventory/reserve`, and the web BFF's calls to the order/payment/inventory HTTP handlers are all REST + JSON.
- **The OpenAPI spec is the single source of truth** for the public REST contract (`api/openapi.yaml`, sub-stage 3.1.g). Any new public endpoint is added to the OpenAPI spec first, then to the handler.
- **Service-to-service coordination is event-driven, not RPC.** Order → Saga → (Payment, Inventory) communicate via Kafka topics. There is no synchronous saga-to-service call to translate from REST to gRPC; the saga consumes events and emits events. Where a synchronous in-cluster call is needed (e.g., the web BFF reading stock), it is HTTP/REST against the same handler that serves the public API.
- **The webhook is REST.** The mock payment provider "calls back" via the same protocol it would use in production; that is HTTPS POST with a JSON body, so the only honest choice is REST.
- **All ingress to the platform from outside the cluster is REST.** gRPC is not exposed, and no in-cluster code path uses it.
- **Platform APIs (health, metrics, OTel) remain HTTP** regardless of client.

The boundary is therefore **simplified away entirely** — there is one protocol, REST, used at every layer. Future gRPC adoption, if any, is **deferred to v1.2+** and would require its own ADR, a `api/proto/*.proto` layout, generated Go stubs, and an opt-in migration path (e.g., dual-protocol via grpc-gateway). None of that work is scheduled for v1.

## Alternatives Considered

### A. REST everywhere (chosen)

Use REST for both client-facing and in-cluster traffic. No gRPC.

**Pros:**
- **One protocol.** Less to learn, less to operate, less to test.
- **Standard tooling.** curl, Postman, browser dev tools, OpenAPI, OpenTelemetry HTTP propagation all work out of the box.
- **Trivial debugging.** Plain HTTP, plain JSON, plain headers.
- **Schema validation via OpenAPI.** The single source of truth covers both public and in-cluster REST surfaces.
- **No proto pipeline.** No `protoc`, no generated Go stubs, no `api/proto/` directory to maintain. Adding a field is a one-line change in the handler and a one-line change in the OpenAPI spec.
- **Webhook fit is natural.** The mock payment provider calls back via HTTP POST, exactly as Stripe, PayPal, and Adyen do in production.
- **Cross-team integration is universal.** External teams integrating against orderflow only need an HTTP client.

**Cons:**
- **No streaming.** Server-side streaming (e.g., a saga reporting progress to a coordinator over a long-lived connection) is not natively expressible in REST. We don't need it today: the saga is event-driven via Kafka, and the web UI polls/hx-triggers for timeline updates.
- **No typed contracts for Go-to-Go paths.** Each Go-to-Go boundary is a place where a field can be renamed without the compiler noticing. Mitigated by sharing Go types in the `internal/` packages of each service and by validating against the OpenAPI schema in handler tests.
- **Higher per-request overhead.** HTTP/1.1 framing, JSON parsing, no header compression. At 100 RPS this is fine; at 10k RPS it adds up. We are nowhere near that scale, and the metrics pipeline (Prometheus + Tempo) makes the cost observable when we get there.
- **No native deadline propagation.** REST relies on the `timeout` query parameter or a separate header convention, which is convention-only. We use `context.WithTimeout` at the handler layer and rely on the underlying `http.Client` timeout (services/web's backend client is `http.Client{Timeout: 10s}` per `internal/backend/client.go`).

### B. gRPC everywhere (rejected)

Use gRPC for both client-facing and service-to-service traffic.

**Pros:**
- **One protocol, on the wire.** No RTI (request-translation-intermediary) at any boundary.
- **Typed contracts everywhere.** Proto files are the source of truth; clients get generated stubs.
- **High performance.** HTTP/2 multiplexing, binary framing, header compression.
- **Streaming.** Server, client, and bidirectional streaming all supported.
- **Native context propagation.** Deadlines, cancellation, metadata.

**Cons:**
- **Poor browser support.** gRPC requires HTTP/2 trailers, which most browsers handle inconsistently. grpc-web adds a proxy layer (envoy) that undermines the "simple" pitch.
- **Awkward tooling for humans.** `grpcurl` is good but not curl-level. No Postman equivalent. Hard to "just try it" from a browser dev tools panel.
- **Bad fit for webhooks.** The mock payment provider (per spec §"Non-Goals": "Real payment provider integration (mock only)") "calls back" via HTTP POST. Most real payment providers (Stripe, PayPal, Adyen) use REST + webhook signatures. Choosing gRPC here would be unrealistic.
- **Cross-team friction.** External teams integrating against orderflow would need to write a gRPC client in their stack of choice. REST is universal.
- **OpenAPI ecosystem.** The spec promises an OpenAPI 3.0 contract (sub-stage 3.1.g). gRPC and OpenAPI coexist awkwardly (via grpc-gateway or third-party conversion); REST is the natural home.
- **Service-to-service isn't even request/response.** The saga doesn't synchronously call Order/Payment/Inventory — it consumes events from Kafka. The biggest theoretical win of gRPC (typed Go-to-Go RPC) does not apply to the orderflow topology.

### C. REST external + gRPC internal (originally chosen; now reverted)

REST for `/v1/*` endpoints exposed to clients. gRPC for in-cluster service-to-service calls. This was the pre-audit decision; the v1.1.5 audit found it was never implemented and the cited `api/proto/*.proto` files do not exist.

**Pros (theoretical, since not built):**
- **Best tool for each job.** REST is the lingua franca of the public internet; gRPC is the lingua franca of typed inter-service communication in modern microservice stacks.
- **Strong typing where it matters.** The saga orchestrator and the order-flow compensate paths would benefit from proto-generated stubs.
- **Streaming where useful.** Saga progress events, outbox-poller streams, compacted-state streams — all expressible in gRPC.
- **Deadlines and cancellation.** gRPC's `context.Deadline` would propagate end-to-end.

**Cons (realised in practice):**
- **Two wire formats per service.** Every service binary would have to expose both an HTTP/REST handler and a gRPC server. The v1.1.5 audit found zero gRPC scaffolding; nobody was prepared to write it.
- **Two ports per service.** Extra Kubernetes Service definitions, extra readiness probes, extra observability surface.
- **Two schema sources of truth.** OpenAPI for REST, proto for gRPC. The REST surface and the gRPC surface would have to be kept in sync, with no compiler help.
- **Tooling proliferation.** Need both `curl`/`OpenAPI` tools and `grpcurl`/BloomRPC. Tests in two styles.
- **Deferred indefinitely.** No v1 schedule was ever set; the pre-audit ADR presented gRPC as already decided but never produced the proto files or the gRPC handlers. Reverting the decision surfaces the gap rather than papering over it.

### D. GraphQL (rejected)

Use GraphQL as the unified client-facing API.

**Pros:**
- **Single endpoint.** Clients request exactly the fields they need.
- **Schema-as-product.** The schema is the contract; clients and servers evolve independently as long as the schema is extended.
- **Strong fit for BFF (backend-for-frontend) patterns.** Mobile and web clients can share one endpoint.

**Cons:**
- **Wrong shape for our traffic.** orderflow clients are mostly doing one thing: create an order, get its state, receive a webhook. GraphQL's strength is aggregation across many entities; we have one entity per client request.
- **N+1 query problem.** A GraphQL "order with status and payment" query has to fan out to Order, Payment, Inventory services — exactly the cross-service problem we are trying to avoid. The saga pattern already handles this on the server side; exposing it as GraphQL would duplicate the orchestration in the client.
- **Webhook fit is poor.** GraphQL is pull-based; webhooks are push-based. Most real payment providers use REST webhooks.
- **Operational complexity.** Persisted queries, query cost analysis, schema stitching, federation — all real concerns that we'd inherit without a payoff.
- **Out of v1 scope.** The spec does not mention GraphQL. The OpenAPI commitment (3.1.g) already addresses the "single source of truth" concern.

## Consequences

- **One wire format.** Every service binary exposes only an HTTP/REST handler (for `/v1/*` and `/healthz`, `/metrics`, `/livez`, `/readyz`). The same domain layer backs every endpoint.
- **No `api/proto/` directory.** The pre-audit ADR referenced proto files; the directory was never created and is not on the v1 path. The only artifact under `api/` is `api/openapi.yaml`. Any future proto work is gated on a separate ADR.
- **REST handlers are thin.** Each REST endpoint maps the HTTP request to a domain call (a repository write, a Kafka publish, or a state-machine transition). The web BFF is the only cross-service HTTP client, and it speaks to the same handlers that serve the public API.
- **Service-to-service is Kafka-only.** The saga consumes `OrderCreated` from the order service's outbox topic, then emits `StockReserveRequested`, `PaymentRequested`, and their outcomes. There is no synchronous saga-to-service call to translate from REST to gRPC.
- **Auth is REST-only.** `X-API-Key` for client auth, mTLS or shared-secret tokens for in-cluster traffic, all over HTTP. Sub-stage 3.3.c (middleware) handles the auth surface; no separate gRPC interceptor is needed.
- **The webhook is REST.** Per spec §"API Surface": `POST /v1/payments/webhook`. The mock payment provider always calls back via REST + signed payload.
- **Tracing works in REST only.** OpenTelemetry propagation (sub-stage 3.10.a) injects `traceparent` headers into HTTP requests and Kafka message headers. Every trace hits Tempo regardless of whether the work crossed an HTTP boundary or a Kafka topic.
- **OpenAPI is the public contract.** Sub-stage 3.1.g defines `api/openapi.yaml`. Changes to the public REST API require an OpenAPI PR. There is no second contract to keep in sync.
- **Trade-off explicit.** We accept the lack of typed Go-to-Go contracts and streaming in exchange for a single protocol, a single schema, a single debugging story, and a single set of tools. v1 throughput is well within REST's comfort zone.
- **gRPC is explicitly deferred to v1.2+.** If a future ADR proposes gRPC, it must include: a `api/proto/*.proto` layout, generated Go stubs, a migration path for existing REST clients (grpc-gateway or dual-protocol), and a justification for what REST cannot do that gRPC would unlock. The current audit found no such justification.

## References

- Spec: `C:\Users\t0p_m\docs\superpowers\specs\orderflow-spec.md` §"Stack Choices", §"API Surface", §"Non-Goals"
- Sub-stages: 3.1.g (OpenAPI spec), 3.3.c (middleware with auth), 3.4.a (order skeleton, REST-only), 3.9.a (saga, event-driven via Kafka)
- OpenAPI 3.0.3: swagger.io/specification/v3 (the public REST contract)
- REST industry consensus: Google SRE Book, "Practical API Design at Netflix" (2012); Fielding, "Architectural Styles and the Design of Network-Based Software Architectures" (2000)
- gRPC deferred-analysis notes: https://github.com/grpc/grpc-web (consulted, not adopted)
- GraphQL trade-offs: Facebook engineering, "GraphQL: A Data Query Language" (2015); Apollo docs (consulted for trade-off analysis, not adopted)
- Webhook conventions: Stripe API docs (representative of real payment provider callbacks)
- v1.1.5 audit: confirmed `api/proto/` is absent, no gRPC import in any service binary, all endpoints are HTTP/REST, service-to-service coordination is Kafka event pub/sub
