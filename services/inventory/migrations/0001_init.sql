-- orderflow Inventory Service — initial schema.
-- Sub-stage 3.6.e.
--
-- Two tables:
--   stock_items       — optimistic-locked stock rows (see model.Stock
--                       and lock.PGLocker)
--   inventory_outbox  — transactional outbox (ADR-0002)
--
-- The version column on stock_items is the optimistic-lock token.
-- Every UPDATE goes through lock.PGLocker which always sets
--     version = version + 1
-- and guards the WHERE clause with version = \$expected.

CREATE TABLE IF NOT EXISTS stock_items (
    sku         TEXT         PRIMARY KEY,
    available   INTEGER      NOT NULL CHECK (available >= 0),
    reserved    INTEGER      NOT NULL DEFAULT 0 CHECK (reserved >= 0),
    version     BIGINT       NOT NULL DEFAULT 1,
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS inventory_outbox (
    id              BIGSERIAL    PRIMARY KEY,
    event_id        UUID         NOT NULL UNIQUE,
    event_type      TEXT         NOT NULL,
    aggregate_id    TEXT         NOT NULL,
    aggregate_type  TEXT         NOT NULL,
    schema_version  TEXT         NOT NULL,
    topic           TEXT         NOT NULL,
    payload         JSONB        NOT NULL,
    status          TEXT         NOT NULL DEFAULT 'PENDING',
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    sent_at         TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_inventory_outbox_pending
    ON inventory_outbox (created_at)
    WHERE status = 'PENDING';