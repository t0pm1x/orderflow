-- orderflow Order Service — outbox attempts/last_error columns.
-- v1.1.4 (closes v1.1.2 P1-#3 deferred).
--
-- The poller's retry budget was tracked only in a per-Pod in-memory
-- sync.Map. A pod restart wipes it, so a row that was about to cross
-- MaxAttempts is given a fresh budget, and a row that fired MarkFailedTx
-- but failed to commit (network blip between the UPDATE and the COMMIT)
-- would re-DLQ on the next pod. Reading the attempts counter from the
-- DB row itself — and letting MarkFailedTx increment it inside the
-- locked tx — means the budget survives restarts and stays consistent
-- across replicas. The hot path gets an `attempts` + `last_error`
-- column mirroring the saga_outbox schema (sub-stage 3.10.e).

ALTER TABLE order_outbox
    ADD COLUMN IF NOT EXISTS attempts   INT    NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_error TEXT;
