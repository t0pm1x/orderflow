-- orderflow Payment Service — outbox attempts/last_error columns.
-- v1.1.4 (closes v1.1.2 P1-#3 deferred).
--
-- See services/order/migrations/0003_outbox_attempts.sql for the
-- rationale: per-Pod in-memory counter is volatile across restarts;
-- the DB counter survives. The poller's MarkFailedTx is the same
-- SQL for every service, so all three outbox tables follow the same
-- shape.

ALTER TABLE payment_outbox
    ADD COLUMN IF NOT EXISTS attempts   INT    NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_error TEXT;
