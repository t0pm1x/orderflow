# ADR-0002: Transactional Outbox + Kafka EOS

- **Status:** Accepted
- **Date:** 2026-08-17
- **Deciders:** orderflow architecture team

## Context

The orderflow platform has exactly-three services (Order, Payment, Inventory) and exactly-one event log (Kafka via Redpanda). Every state change that the spec describes as an event ("OrderCreated", "StockReserved", "PaymentCompleted", "OrderConfirmed", "OrderCancelled", "OrderFailed", "StockReservationFailed", "StockReleased", "PaymentRequested", "PaymentFailed") must be published to Kafka and consumed by other services.

The acceptance criteria are explicit:

- **Goal:** "Exactly-once event publishing via outbox + Kafka transactions" (spec §"Goals")
- **Goal:** "Survive Kafka outage (events queued in outbox)" (spec §"Goals")
- **Acceptance test 3.11.d:** "kill Kafka → orders still accepted → events buffered in outbox → catch up on Kafka recovery"
- **Acceptance test 3.11.f:** 100 RPS sustained, **no event loss**

The naive approach — write to the database, then publish to Kafka — has a fundamental race: if the DB write succeeds but the Kafka publish fails (or the service crashes between), the event is lost. If the DB write fails after the Kafka publish, downstream services react to a non-event. We must pick a publishing mechanism that is atomic between the DB and Kafka.

## Decision

**Use the transactional outbox pattern combined with Kafka exactly-once semantics (EOS).**

Every service writes a row to its `outbox` table in the same DB transaction as the business state change. A separate `outbox poller` (sub-stage 3.7.a) reads the outbox in batches, publishes to Kafka inside a Kafka transaction, and only marks the row published after the Kafka transaction commits. We use Kafka's transactional producer API (`initTransactions`, `beginTransaction`, `commitTransaction`, `abortTransaction`) so that:

1. Either the business state and the event are both durable, or neither is.
2. Downstream consumers see the event exactly once (no duplicate effects on retry).
3. A Kafka outage causes the poller to back off and retry; events stay in the outbox table until publish succeeds.

The implementation contract (sub-stage 3.7.b) explicitly couples `pgx.Tx` + Kafka transactional producer in a single coordination point.

## Alternatives Considered

### A. Transactional outbox (chosen)

Append an event row to the local `outbox` table inside the same DB transaction as the business state mutation. A poller reads outbox rows in batches, publishes to Kafka, and on success marks the rows as published (or deletes them, depending on retention policy).

**Pros:**
- **Atomicity via the DB.** The same transaction that writes "order state = confirmed" writes "outbox row containing OrderConfirmed". If the tx commits, both are durable. If it aborts, neither happens. No distributed transaction needed.
- **Outbox survives Kafka outage.** Events accumulate in the outbox table (Postgres) until Kafka is reachable. The chaos test (3.11.d) passes by construction.
- **Idempotent retry.** The poller reads, publishes, marks. If the poller crashes between publish and mark, the next poll re-publishes — consumers dedupe by `event_id` (sub-stage 3.8.b, idempotent handler).
- **No new infrastructure.** We already need Postgres per service (spec §"Module Layout"). No Debezium, no separate log shipper.
- **Testable.** Pure DB writes are easy to test with the existing testcontainers-based integration tests (sub-stage 2.2.f pattern). The poller is a tight loop, also easy to test.

**Cons:**
- **Polling lag.** Between business commit and Kafka publish, there is a small delay (default 100ms; configurable). This is acceptable for order events (humans are not waiting for ms) but eliminates polling if a true zero-latency event bus is required.
- **Doubles the DB write.** Each business mutation now writes two rows (business + outbox). Negligible overhead at our scale.
- **Poller is a new failure mode.** What if the poller dies? Mitigated by running ≥2 poller replicas with `SELECT ... FOR UPDATE SKIP LOCKED` (the same pattern as ADR-0001 in jobforge, "PG SKIP LOCKED for queue claiming").
- **Tail problem.** The outbox table grows; we need a retention policy (delete after publish, or archive after N days).

### B. Change Data Capture (CDC) with Debezium (rejected)

A separate process tails the Postgres write-ahead log (WAL) via logical replication, decodes row changes, and publishes them to Kafka. The application never writes an outbox row; Debezium reads every committed change.

**Pros:**
- **No application changes.** The service writes normal SQL; Debezium picks up the changes.
- **No polling.** Streams the WAL as it's written.
- **Lowest latency.** Kafka emission lag is ~tens of ms, dominated by WAL shipping.

**Cons:**
- **New infrastructure.** Requires a Kafka Connect cluster, Debezium connectors for each Postgres DB, schema history topics, and a connector config per service. Substantial additional ops surface.
- **Schema coupling.** Debezium publishes the full row (or diff) by default — noisy. To publish only business-meaningful events, the application must either use a separate "events" table that Debezium tails, or filter post-hoc on the connector. Either way, we're back to an outbox-like table.
- **Operational fragility.** Debezium connectors can fall behind, get stuck on schema changes, or require manual re-snapshot. All of these are real production scenarios.
- **Loses business intent.** A row update is not a business event. "Order state changed from pending to reserved" is a different event depending on the trigger (PaymentCompleted vs StockReserved); CDC cannot distinguish because it sees only the final row.
- **Not in v1 scope.** The spec explicitly lists outbox in §"Outbox Pattern"; CDC is not mentioned.

### C. Dual-write (rejected)

Application writes to the business DB, then immediately publishes to Kafka. No atomicity.

**Pros:**
- **Simplest implementation.** Just two function calls.
- **Lowest latency.** Publish is in-line.

**Cons:**
- **Cannot satisfy the acceptance criteria.** By construction, this is not atomic. A crash between DB write and Kafka write loses the event. This violates the "exactly-once event publishing" goal.
- **The wrong baseline.** Listing it as an alternative is mostly to be explicit about why we don't accept it.

### D. Listen-to-yourself (rejected)

The service writes to Kafka, then a local consumer reads its own writes and applies the side effect to the DB. The DB is materialized from events.

**Pros:**
- **Single source of truth.** The event log is the fact.
- **Naturally decoupled.** All writes go through the event log.

**Cons:**
- **Extra latency.** Every state change takes a round-trip through Kafka. For PG p99 < 200ms, this is too much.
- **Read-your-writes complexity.** The API must query the materialized view, and the consumer must keep that view current. Adds complexity without solving the original problem.
- **Best as a complement, not a replacement.** We *do* use Kafka events (via the outbox) to communicate between services. But a service's own API writes must be synchronous to Postgres for the API to return correct state.

## Consequences

- **The outbox table is part of every service's schema.** Sub-stage 3.4.e (order migrations), 3.5.e (payment), 3.6.e (inventory) all include an `outbox` table. The schema is fixed: `event_id UUID PRIMARY KEY`, `aggregate_id`, `aggregate_type`, `event_type`, `payload JSONB`, `occurred_at`, `published_at NULLABLE`. The `published_at` index is the poller's hot path.
- **Business code writes to the outbox in the same transaction.** Pattern: `tx := db.Begin(); tx.Exec("UPDATE orders SET state=$1 WHERE id=$2", ...); tx.Exec("INSERT INTO outbox (...) VALUES (...)", ...); tx.Commit()`. This is enforced by the `outbox.Writer` interface (sub-stage 3.4.d) — there is no API to emit an event without a DB transaction.
- **The outbox poller is a separate goroutine per service.** It runs in the same binary as the API (to keep operations simple) but is a separate component. Configurable poll interval (default 100ms) and batch size (default 100 events).
- **Kafka EOS is mandatory.** The poller uses `kafka-go` (or `confluent-kafka-go`) transactional producer. The Kafka transaction is opened around the publish call, committed after the publish acks, and aborted on any error. Combined with `read_committed` isolation on the consumer side, this gives us exactly-once end-to-end.
- **DLQ for poison events.** Sub-stage 3.7.c — after N retries, an event is moved to a Kafka DLQ topic for manual inspection. The poller must not block forever on a malformed event.
- **Trade-off explicit.** We accept the polling lag (default 100ms) and the doubled DB write in exchange for atomicity, idempotency, and Kafka-outage survival. The lag is acceptable because the spec's `p99 < 200ms` budget is for the API response, not for event propagation — the commit-then-poll-then-publish sequence typically finishes within 150ms.
- **Future evolution.** If p99 ever becomes dominated by outbox lag, we can replace the poller with CDC (Debezium) backed by the same `outbox` table. The contract is the same; only the transport changes.

## References

- Spec: `C:\Users\t0p_m\docs\superpowers\specs\orderflow-spec.md` §"Outbox Pattern"
- Sub-stages: 3.4.d (order outbox writer), 3.7.a (poller), 3.7.b (Kafka EOS), 3.7.c (DLQ), 3.7.d (metrics), 3.7.e (per-service integration), 3.7.f (tests)
- Architecture: `docs/architecture/c4-level-2.puml` (Outbox Poller per service), `docs/architecture/c4-level-3-order.puml` (Outbox Writer component)
- Outbox pattern: Microsoft, "Transactional Outbox pattern" (2020); Cloud Design Patterns, ch. 4
- Kafka EOS: confluent docs, "Exactly-Once Semantics Are Possible: Here's How Apache Kafka Does It" (2017)
- CDC: Debezium docs (consulted for trade-off analysis, not adopted)
- Idempotent consumer: sub-stage 3.8.b (idempotent handler with `event_id` dedupe)
- Cross-reference: ADR-0001 in jobforge uses the same `SELECT ... FOR UPDATE SKIP LOCKED` pattern for the outbox poller's multi-replica coordination
