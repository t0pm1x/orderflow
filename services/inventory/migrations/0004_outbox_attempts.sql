-- orderflow Inventory Service — outbox attempts/last_error columns.
-- v1.1.4 (closes v1.1.2 P1-#3 deferred).
--
-- See services/order/migrations/0003_outbox_attempts.sql for the
-- rationale.

ALTER TABLE inventory_outbox
    ADD COLUMN IF NOT EXISTS attempts   INT    NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_error TEXT;
