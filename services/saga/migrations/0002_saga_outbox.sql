-- orderflow Saga Service — v0.5.0 schema additions.
-- Sub-stage 3.10.e (saga runtime).
--
-- This migration does two things in one place to keep the v0.5.0
-- saga runtime on a single deploy step:
--
--   1. ALTER order_sagas to add the columns the runtime persists
--      on Insert and reads on Get (items, total_cents,
--      reservation_id). The original 0001_init.sql only carried
--      state + timestamps; the runtime now needs the order's
--      items, total, and reservation_id to drive the consumer
--      handlers.
--
--   2. CREATE saga_outbox — the transactional outbox table for
--      events the saga emits to order-events. Same shape pattern
--      as services/order/migrations/0001_init.sql's order_outbox,
--      with two v0.5.0 additions specific to saga:
--      * headers (JSONB) — for W3C tracecontext propagation
--      * attempts/last_error — surfaced for the eventual per-row
--        retry budget exposed by Prometheus.
--
-- The poller in pkg/outbox reads `saga_outbox` directly via
-- services/saga/internal/outbox.PGSource.

ALTER TABLE order_sagas
    ADD COLUMN IF NOT EXISTS items           JSONB,
    ADD COLUMN IF NOT EXISTS total_cents     BIGINT       NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS reservation_id  TEXT;

CREATE TABLE IF NOT EXISTS saga_outbox (
    id              BIGSERIAL    PRIMARY KEY,
    event_id        UUID         NOT NULL UNIQUE,
    aggregate_id    TEXT         NOT NULL,
    aggregate_type  TEXT         NOT NULL,
    event_type      TEXT         NOT NULL,
    payload         JSONB        NOT NULL,
    headers         JSONB        NOT NULL DEFAULT '{}'::jsonb,
    schema_version  INT          NOT NULL DEFAULT 1,
    status          TEXT         NOT NULL DEFAULT 'PENDING',
    attempts        INT          NOT NULL DEFAULT 0,
    last_error      TEXT,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    sent_at         TIMESTAMPTZ
);

-- Hot path: poller selects PENDING rows ordered by created_at.
CREATE INDEX IF NOT EXISTS idx_saga_outbox_pending
    ON saga_outbox (created_at)
    WHERE status = 'PENDING';