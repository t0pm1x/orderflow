# ADR-0005: Outbox broker-recovery semantics

- **Status:** Accepted (documented gap)
- **Date:** 2026-08-20
- **Deciders:** orderflow architecture team
- **Supersedes:** none
- **Related:** ADR-0002 (transactional outbox), audit/FINAL_AUDIT.md NEW-P0-4

## Context

ADR-0002 commits the platform to a transactional outbox pattern: business mutations and outbox events are written in the same DB transaction, and a poller publishes the rows to Kafka. The implementation (v1.1.x) marks an outbox row `status='SENT'` after franz-go's `ProduceSync` returns `nil` — the producer's confirmation that the broker received the write and replicated it to the in-sync replica set.

Producer ACK is not the same as consumer read. Two failure windows remain:

1. **Broker death between producer ACK and consumer read.** When the Kafka broker process dies, every partition it leads is unavailable. A `ProduceSync` that succeeded moments before the death has reached the broker's local log file, but a follower that hasn't replicated yet loses the write when the leader's log is gone. The outbox row is `SENT`, the consumer never saw the event.

2. **Container death with no persistent storage.** The testcontainer harness used by `make e2e` and `make e2e-chaos` starts a fresh Kafka broker per test run. When the broker container is killed mid-test and a new one starts, the new broker has no partitions and no data. Any SENT rows from the previous broker's lifetime are lost.

The audit's v1.2 final-validation pass confirmed both windows via the chaos test (`TestChaos_KafkaKill_ChainRecoversAfterKafkaRestart`): the test asserted end-to-end chain recovery after a broker kill + restart, but the outbox's SENT marker is the producer's confirmation that the broker received the write — it does not (and cannot) guarantee the consumer processed it. The terminated container's data is unrecoverable in the current design. The test was deleted; the gap was documented in `audit/FINAL_AUDIT.md` as NEW-P0-4 (P0, NOT FIXED).

The two viable future mitigations are:

**A. Persistent Kafka volumes (operational).** Mount a persistent volume to the broker's log directory (`/var/lib/redpanda/data`). A broker restart preserves every partition's data. The audit's prior commit `f4c73b5` did this for the chaos test's `redpanda` container so `TestChaos_KafkaKill_OrderServiceSurvives` (the surviving "chain stalls when broker dies" half of the contract) can be re-run. Production deployments need the same: a StatefulSet with `volumeClaimTemplates`, or a managed Kafka (Confluent Cloud, MSK, Redpanda Cloud) where the provider handles persistence.

**B. Re-emit on broker recovery (architectural).** On every pod startup, after a successful readiness probe that includes a Kafka metadata fetch, re-emit every outbox row whose `status='SENT'` and whose `created_at` is within a configurable retention window (e.g. 24 h). Consumers already dedupe on `event_id` (ADR-0002 §"Idempotent retry"), so the re-emit is safe — duplicates are filtered at the consumer. The trade-off is that a long broker outage produces a re-emit storm on recovery, which the consumer's dedupe cache (currently 7-day TTL, CONSUMER-6) handles but the upstream broker may not enjoy.

True cross-system exactly-once is impossible without a 2PC between DB and Kafka (the textbook alternative ADR-0002 §"Alternatives Considered" already rejected on grounds of operational complexity). ADR-0005 explicitly does **not** attempt 2PC.

## Decision

**Acknowledge the gap. Defer the fix to two parallel future work items:**

1. **Operational track (preferred path).** Production deployments must use persistent Kafka volumes OR a managed Kafka service. The current `deploy/helm/orderflow-redpanda` chart's emptyDir is acceptable only for local development. The `STATUS.md` and `RUN.md` documents must reflect this requirement prominently.

2. **Architectural track (defense in depth).** Implement option B as a follow-up: add `reEmitSENTOnStartup` to the outbox poller, gated on `OUTBOX_REEMIT_ON_STARTUP=true` (default off). The re-emit path runs once per pod lifetime, after the Kafka readiness probe passes, and uses the same `BumpAttempts`/`MarkSentTx` plumbing so the existing test coverage applies.

The current single-broker dev environment is acceptable: `TestChaos_KafkaKill_OrderServiceSurvives` confirms the chain stalls when the broker dies (the "events queued in outbox" half of ADR-0002's contract), and the chaos test's NOTE comment documents the missing half.

**Out of scope for this ADR:**

- Changing the outbox row's `status='SENT'` semantics (producer ACK is still the trigger).
- Switching to a CDC approach (ADR-0002 §"Alternatives Considered B" — rejected).
- 2PC across DB+Kafka (textbook alternative — rejected on operational complexity).
- Consumer-side duplicate-window widening beyond the existing 7-day Redis dedupe TTL.

## Consequences

**Positive:**

- The audit's "NOT READY" verdict for production multi-broker is now backed by an explicit, time-bounded decision: this gap is known, the mitigation paths are scoped, and the operational path (persistent volumes / managed Kafka) is the standard answer for any serious deployment.
- Single-broker dev environments continue to work as today; no code change required for the gap.
- The architectural path (re-emit) is preserved as an opt-in for environments where the operational path is impractical.

**Negative:**

- A multi-broker deployment that loses one broker permanently loses every event that was SENT-but-not-yet-replicated. The audit's "Known production holes" list (CHANGELOG v1.2.0) explicitly carries this item.
- The re-emit path (architectural track) requires non-trivial work: the outbox poller would need a startup hook, a separate config knob, a new SQL query (`SELECT ... WHERE status='SENT' AND created_at > NOW() - INTERVAL '...'`), and integration tests that prove the re-emit does not corrupt the consumer's offset commits.
- The audit's "10 P0s are fixable in 5 days of focused work" estimate is unchanged; this ADR does not add P0 work.

**Mitigations documented for operators:**

- Run a single-broker Redpanda/Kafka with persistent volumes for now.
- Keep Kafka `log.retention.ms` at ≥ 7 days (matches the consumer dedupe TTL — see CONSUMER-6).
- Accept that mid-chain broker death loses in-flight outbox rows until the architectural track lands.
- Monitor the `outbox_pending_events` and `outbox_failed_events` gauges (OBS-9) — a sudden drop after a broker event indicates lost rows.

## Alternatives Considered

### A. Persistent Kafka volumes (operational — preferred)

Configure the Kafka broker's data directory on a persistent volume. A broker process restart preserves every partition's log; the outbox's SENT marker continues to mean "consumer can read this".

**Pros:**

- Standard operational practice for any production Kafka deployment.
- No code change required.
- The audit's prior commit `f4c73b5` already proved the pattern works in the test harness.

**Cons:**

- Requires infrastructure work (StatefulSet + PVC, or a managed Kafka).
- A datacenter-level failure still loses the broker's data; this ADR does not address that.

### B. Re-emit on broker recovery (architectural — defense in depth)

On pod startup, re-emit every SENT row whose `created_at` is within a retention window. Consumers dedupe on `event_id`, so duplicates are filtered.

**Pros:**

- Works even when the broker's data is gone (the current chaos-test failure mode).
- Opt-in via env knob; single-broker dev environments are unaffected.

**Cons:**

- New code path, new test surface, new failure modes (re-emit storm, dedupe-cache pressure).
- Does not help if both the broker AND the consumer's dedupe cache are gone (multi-failure scenario).

### C. Two-phase commit across DB + Kafka (rejected)

Use a distributed transaction coordinator to commit the business state, the outbox row, and the Kafka publish atomically.

**Pros:** Textbook EOS.

**Cons:** Adds a transaction coordinator (or uses XA), requires both DB and Kafka to participate, increases tail latency by 10-100x, fragile under network partitions. ADR-0002 §"Alternatives Considered" explicitly rejected this approach for the same reasons. Rejected here for the same reasons.

### D. CDC with Debezium (rejected)

ADR-0002 §"Alternatives Considered B" — Debezium tails the Postgres WAL and publishes to Kafka. The application never writes an outbox row.

**Pros:** No outbox table; WAL is the source of truth.

**Cons:** WAL is replayed from the last LSN; broker-death-between-LSN-and-Kafka loses the same data. The gap is not closed by CDC. ADR-0002 rejected CDC on different grounds (Debezium as new infrastructure); rejected here on grounds that it does not solve the problem.

---

*End of ADR-0005. The gap is documented; the mitigation paths are scoped; the operational track is the recommended next step for any production deployment.*
