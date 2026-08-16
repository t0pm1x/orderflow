# ADR-0003: REST for Clients, gRPC for Service-to-Service

- **Status:** Accepted
- **Date:** 2026-08-17
- **Deciders:** orderflow architecture team

## Context

orderflow is a multi-service platform, and there are two distinct kinds of traffic:

1. **Client traffic** — external callers (web, mobile, CLI, integration tests) talk to the platform via `POST /v1/orders`, `GET /v1/orders/{id}`, and the `POST /v1/payments/webhook` (provider callback). The spec §"API Surface" lists these endpoints. The clients are heterogeneous, the network is the public internet or a corporate VPN, and the cost of a new client SDK is high.
2. **Service-to-service traffic** — the saga orchestrator (sub-stage 3.9) calls `Order`, `Payment`, and `Inventory` to reserve stock, request payment, and confirm state. The saga and the order-flow compensate paths are tightly coupled to the participating services' internal APIs. The service-to-service traffic is in-cluster, on the same Kubernetes pod network, and the client is always another Go service we own.

We must pick a wire protocol for each traffic class. The spec §"Stack Choices" already commits: "RPC: gRPC for service-to-service where proto defined." This ADR documents the full decision: **REST external, gRPC internal** — and the boundary between them.

## Decision

**Use REST + JSON over HTTPS for client-facing APIs (`/v1/orders`, `/v1/payments/webhook`, `/v1/inventory/reserve`). Use gRPC with Protocol Buffers for service-to-service traffic.**

- **REST** is the client-facing contract. The OpenAPI spec (sub-stage 3.1.g) is the source of truth.
- **gRPC** is the in-cluster contract between Order, Payment, Inventory, and Saga. Proto files (sub-stage 3.4 onwards) live in `api/proto/` and are used to generate Go client/server stubs.
- **The webhook is REST.** The mock payment provider "calls back" via the same protocol it would use in production; that is HTTPS POST with a JSON body, so the only honest choice is REST.
- **All ingress to the platform from outside the cluster is REST.** gRPC is never exposed to the public internet.

The boundary is simplified: **outside the cluster → REST; inside the cluster between services → gRPC.** Platform APIs (health, metrics, OTel) remain HTTP regardless of client.

## Alternatives Considered

### A. REST everywhere (rejected)

Use REST for both client-facing and service-to-service traffic.

**Pros:**
- **One protocol.** Less to learn.
- **Standard tooling.** curl, Postman, browser dev tools, OpenAPI.
- **Trivial debugging.** Plain HTTP, plain JSON.
- **Schema validation via OpenAPI.** Good for loosely-typed clients.

**Cons:**
- **No streaming.** Service-to-service flows can benefit from server-side streaming (e.g., a saga reporting progress to a coordinator). REST has no first-class streaming beyond chunked transfer encoding, which is awkward.
- **No typed contracts for the Go-to-Go path.** The saga orchestrator must hand-marshal requests and responses. Every service boundary is a place where a field can be renamed without the compiler noticing.
- **Higher per-request overhead.** HTTP/1.1 framing, JSON parsing, no header compression. At 100 RPS this is fine; at 10k RPS it adds up.
- **No deadlined propagation.** gRPC carries `context.Deadline` natively; REST relies on the `timeout` query parameter or a separate header convention, which is convention-only.

### B. gRPC everywhere (rejected)

Use gRPC for both client-facing and service-to-service traffic.

**Pros:**
- **One protocol.** No RTI (request-translation-intermediary) at the boundary.
- **Typed contracts everywhere.** Proto files are the source of truth; clients get generated stubs.
- **High performance.** HTTP/2 multiplexing, binary framing, header compression.
- **Streaming.** Server, client, and bidirectional streaming all supported.
- **Native context propagation.** Deadlines, cancellation, metadata.

**Cons:**
- **Poor browser support.** gRPC requires HTTP/2 trailers, which most browsers handle inconsistently. grpc-web adds a proxy layer (envoy) that undermines the "simple" pitch.
- **Awkward tooling for humans.** `grpcurl` is good but not curl-level. No Postman equivalent. Hard to "just try it" from a browser.
- **Bad fit for webhooks.** The mock payment provider (per spec §"Non-Goals": "Real payment provider integration (mock only)") "calls back" via HTTP POST. Most real payment providers (Stripe, PayPal, Adyen) use REST + webhook signatures. Choosing gRPC here would be unrealistic.
- **Cross-team friction.** External teams integrating against orderflow (e.g., a future partner) would need to write a gRPC client in their stack of choice. REST is universal.
- **OpenAPI ecosystem.** The spec promises an OpenAPI 3.0 contract (sub-stage 3.1.g). gRPC and OpenAPI coexist awkwardly (via grpc-gateway or third-party conversion); REST is the natural home.

### C. REST external + gRPC internal (chosen)

REST for `/v1/*` endpoints exposed to clients (web, mobile, webhooks). gRPC for in-cluster service-to-service calls.

**Pros:**
- **Best tool for each job.** REST is the lingua franca of the public internet; gRPC is the lingua franca of typed inter-service communication in modern microservice stacks (Google, Netflix, Lyft, Uber all follow this pattern).
- **Strong typing where it matters.** The saga orchestrator and the order-flow compensate paths are the most bug-prone parts of the system. gRPC's proto-generated stubs turn wire-format errors into compile-time errors.
- **Streaming where useful.** The saga can stream progress; the outbox poller can stream events; the consumer can stream compacted state. None of these are required by v1, but the option is there.
- **Deadlines and cancellation.** gRPC's `context.Deadline` propagates end-to-end, so a 5-minute saga timeout (sub-stage 3.9.c) can be enforced as a deadline on every downstream call.
- **Single source of truth per protocol.** OpenAPI for REST contracts (sub-stage 3.1.g); proto for gRPC contracts. Each is generated, validated, and reviewed independently.
- **No external gRPC.** Even a sophisticated public client (mobile app) talks REST. gRPC clients are only services we control.

**Cons:**
- **Two protocols to operate.** Each service container exposes both an HTTP/REST handler and a gRPC server. Sub-stage 3.3.c (middleware) and 3.4.a (order skeleton) must wire both.
- **Translation surface.** The REST-to-gRPC boundary inside a service is one more place to keep schemas in sync. Mitigated by generating the gRPC stubs from the same proto file that defines the gRPC contract, and writing the REST handlers against the same domain types.
- **Slight operational overhead.** Two ports per service (one HTTPS, one gRPC). In Kubernetes, both are on the same pod, so this is mostly a config concern.
- **Tooling proliferation.** Need both `curl`/`OpenAPI` tools for REST and `grpcurl`/BloomRPC for gRPC.

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

- **Two wire formats per service.** Every service binary exposes both an HTTP/REST handler (for `/v1/*` and `/healthz`, `/metrics`, `/livez`, `/readyz`) and a gRPC server (for service-to-service calls). The same domain layer backs both.
- **Proto files in `api/proto/`.** Per spec §"Module Layout": `api/proto/*.proto`. Sub-stage 3.4 onwards will define and generate Go stubs.
- **REST handlers are thin.** Each REST endpoint translates the HTTP request to a gRPC call to the service's own internal gRPC handler — or, more pragmatically, to a direct call into the domain layer. Pure translation is preferred for endpoints that should be 1:1 with gRPC methods (e.g., `POST /v1/payments/webhook` could simply publish a `PaymentWebhookReceived` event).
- **gRPC is required for the saga.** Sub-stage 3.9.a (saga state machine) calls the participating services via gRPC. The `cmd/saga/main.go` binary is a gRPC client of the other three services.
- **Auth differs by protocol.** REST uses an `X-API-Key` header (or similar) for client auth; gRPC uses mTLS + service-account tokens for service-to-service auth. Sub-stage 3.3.c (middleware) handles both.
- **The webhook is REST.** Per spec §"API Surface": `POST /v1/payments/webhook`. The mock payment provider always calls back via REST + signed payload.
- **Tracing works in both.** OpenTelemetry propagation (sub-stage 3.10.a) injects `traceparent` headers into both REST (HTTP `traceparent`) and gRPC (gRPC metadata). The trace hits Tempo regardless of which protocol carried the request.
- **OpenAPI is the public contract.** Sub-stage 3.1.g defines `api/openapi.yaml`. Changes to the public REST API require an OpenAPI PR. The gRPC proto schema is the internal contract.
- **Trade-off explicit.** We accept two protocols and the operational overhead in exchange for the right tool in each layer, with strong typing at the service boundary.

## References

- Spec: `C:\Users\t0p_m\docs\superpowers\specs\orderflow-spec.md` §"Stack Choices", §"API Surface"
- Sub-stages: 3.1.g (OpenAPI spec), 3.3.c (middleware with auth), 3.4.a (order skeleton with both REST + gRPC), 3.9.a (saga → gRPC clients)
- REST vs gRPC industry consensus: Google SRE Book, "Practical API Design at Netflix" (2012); Lyft, "gRPC for Microservices" (2019); Uber engineering blog
- gRPC-web limitations: https://github.com/grpc/grpc-web (consulted, not adopted)
- GraphQL trade-offs: Facebook engineering, "GraphQL: A Data Query Language" (2015); Apollo docs (consulted for trade-off analysis, not adopted)
- Webhook conventions: Stripe API docs (representative of real payment provider callbacks)
- OpenAPI 3.0.3: swagger.io/specification/v3 (cross-referenced for REST contract)
- Proto3: developers.google.com/protocol-buffers/docs/proto3 (cross-referenced for gRPC contract)
