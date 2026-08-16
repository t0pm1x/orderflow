# ADR-0001: Saga Orchestration vs Choreography

- **Status:** Accepted
- **Date:** 2026-08-17
- **Deciders:** orderflow architecture team

## Context

The orderflow platform must coordinate a multi-step business transaction that spans three services (Order, Payment, Inventory) and four aggregates (order, stock reservation, payment, order state). The flow is triggered by `POST /v1/orders` and must reach a terminal state (`confirmed` or `cancelled`/`failed`) within bounded time, with compensation when any step fails.

The relevant flow (per spec §"Saga Flow"):

1. Order created (state=`pending`)
2. Inventory stock reserved in Redis with TTL
3. Payment charged (mock provider)
4. Order confirmed
5. On any failure: release stock, refund payment, cancel order

Failure paths are part of the contract, not an edge case. The chaos test (sub-stage 3.11.d) requires that a Kafka outage cannot lose order state, and the compensation test (3.11.b) requires that a payment failure releases stock and cancels the order deterministically.

We need to pick a coordination mechanism that makes compensation explicit, debuggable, and survive single-service crashes without leaving the saga stuck in a mid-state.

## Decision

**Use saga orchestration (central coordinator service).** The `saga` service (see `cmd/saga/main.go`, `services/saga/`) owns the order-saga state machine, issues commands to the three participating services, and listens for their reply events to advance state. Each step is a small, named transition in the orchestrator; each failure path is a named compensation action.

We rejected even-driven choreography for the order→confirm flow because the compensation graph is too tangled to maintain as implicit per-service reactions. (Choreography remains valid for purely fire-and-forget reactions such as audit logging — see Consequences.)

## Alternatives Considered

### A. Saga orchestration (chosen)

A dedicated `saga` service consumes `order-events` and `payment-events` / `inventory-events`, maintains a `saga_states` table (sub-stage 3.9.d), and drives the order-saga state machine (`initiated → stock_reserved → payment_completed → completed` or `→ compensated`). On reply events, the orchestrator issues the next command by writing an event to the outbox (sub-stage 3.4.d / 3.7.a).

**Pros:**
- **Explicit compensation graph.** Every failure path is a named function in the orchestrator (`compensate.go`, sub-stage 3.9.b). Reviewers can read the full saga in one file.
- **Observable state.** Each in-flight saga is one row in `saga_states` with a clear `state`, `updated_at`, and `attempts`. Operators can `SELECT * FROM saga_states WHERE state='stock_reserved' AND updated_at < NOW() - INTERVAL '5min'` to find stuck sagas.
- **Time-based compensation is trivial.** A background watchdog (sub-stage 3.9.c) finds sagas whose `updated_at` is older than the TTL and issues compensation. No coordination between services needed.
- **Debuggable.** A single trace shows the orchestrator's decisions; the saga flow can be replayed from the `saga_states` row.

**Cons:**
- **Coupling to the orchestrator.** All three services must know the orchestrator's event names and accept its commands. This is a real coupling cost but is contained inside the platform — there is no external client that must learn the saga.
- **Extra service to operate.** Adds one more deployment unit (one pod, one Postgres DB, one consumer group). Negligible at this scale.
- **Orchestrator is a single point of failure.** Mitigated by running ≥2 replicas with Postgres-backed state (any replica can pick up any saga).

### B. Choreography (rejected)

Each service reacts to events independently. Order emits `OrderCreated`; Inventory consumes and emits `StockReserved`; Order emits `PaymentRequested`; Payment consumes and emits `PaymentCompleted`; Order emits `OrderConfirmed`. No central coordinator.

**Pros:**
- **Loose coupling.** Services only know their own event subscriptions; no need to agree on a saga schema.
- **No orchestrator.** Fewer moving parts.
- **Scales naturally.** Adding a new listener to a topic is trivial.

**Cons:**
- **Compensation is implicit.** "Payment failed → release stock" is not a single named function; it must be discovered by reading three services' event handlers. The implicit control flow is hard to reason about and easy to break with new events.
- **No central place to track which saga is where.** Determining "what state is order X in?" requires joining state across three databases. The chaos test (3.11.d) — kill Kafka and recover — becomes nightmarish because no service knows what the other is doing.
- **Time-based compensation is hard.** "If we haven't heard back in 5 min, compensate" requires each service to know the saga's TTL, which means the TTL leaks into every participant.
- **New event types proliferate.** Each business process variation adds new events; each event must be versioned; each consumer must decide which saga version it speaks. Versioning is a known coordination problem (see Rosseta, 2020).

### C. Two-phase commit (2PC) (rejected)

A synchronous prepare/commit across all three services at the database level (XA transactions).

**Pros:**
- **Strong atomicity.** All-or-nothing across services.

**Cons:**
- **Synchronous blocking.** Held locks across network calls kill p99 latency (ADR-0001 in jobforge, "PG SKIP LOCKED", already cited this trade-off for queue claiming). Our acceptance criterion is p99 < 200ms at 100 RPS — 2PC cannot meet this.
- **Coordinator dependency.** Same single-point-of-failure as saga orchestration, but without the crash-recovery model.
- **Poor fit for external systems.** The mock payment provider is "external" by spec; we cannot include it in a 2PC anyway.
- **Practically unsupported.** Modern Postgres (and Kafka) make XA impractical. The PostgreSQL docs explicitly warn against long-running prepared transactions.

### D. Event sourcing with CQRS (rejected)

Store every state change as an event; current state is a fold over events. Reads go through a separate query model.

**Pros:**
- **Full audit trail.** Every state change is recorded.
- **Time-travel.** Can replay to any point.

**Cons:**
- **Heavier architecture.** Event store, projector, query model, snapshot strategy — adds 3+ components per service.
- **Out of scope for v1.** The spec does not require event sourcing; we already have an event log (Kafka + outbox) that gives us most of the audit-trail benefits.
- **Snapshot complexity.** Long-running orders would need periodic snapshots; we already have a `current_state` column in Postgres.
- **Compensation still requires an orchestrator.** Event sourcing does not solve the coordination problem; it only changes what the events look like.

## Consequences

- **The `saga` service exists.** It owns the order-saga state machine and its compensation actions. It is a peer service to Order, Payment, and Inventory — not a "framework" sitting inside any of them.
- **Compensation is a first-class concern.** Sub-stage 3.9.b (compensation actions) and 3.9.c (saga timeout) are non-negotiable. They must be implemented before any chaos test (3.11.d).
- **Other events may still use choreography.** Audit logging, metrics emission, notification fan-out — these are good choreography candidates because they have no compensation path. ADR-0001 covers only the order→confirm flow.
- **Trade-off acknowledged.** We accept coupling to the orchestrator in exchange for explicit, debuggable, time-bounded compensation. The orchestrator is a real cost (one more pod, one more DB) but is well-understood.
- **Crash recovery is a property of the orchestrator's state.** Every state transition is committed to `saga_states` before the event is emitted. A crashed orchestrator replica loses nothing; another replica (or a restarted one) finds in-flight sagas in the same state and continues.
- **The orchestrator pattern is reusable.** Future flows (refund, cancel-after-confirm) can either extend this orchestrator or spawn a new one. The pattern is the value; this ADR is the documented decision.

## References

- Spec: `C:\Users\t0p_m\docs\superpowers\specs\orderflow-spec.md` §"Saga Flow"
- Sub-stages: 3.9.a (state machine), 3.9.b (compensation), 3.9.c (timeout), 3.9.d (migrations), 3.9.e (tests)
- Architecture: `docs/architecture/c4-level-2.puml` (saga container), `docs/architecture/c4-level-3-order.puml` (saga event handler component)
- Saga pattern: Garcia-Molina & Salem, "Sagas" (1987); Hohpe & Woolf, "Enterprise Integration Patterns" (2003)
- Choreography pitfalls: Vernon, "Reactive Messaging Patterns with the Actor Model" (2016)
- 2PC critique: Gray, "Notes on Database Operating Systems" (1978); PostgreSQL docs on prepared transactions
- Event sourcing: Fowler, "Event Sourcing" (2005); cross-referenced, not chosen
- Pattern comparison: Richardson, "Microservices Patterns" (2018), ch. 4 (saga), ch. 5 (choreography vs orchestration)
