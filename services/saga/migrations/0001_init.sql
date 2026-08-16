-- orderflow Saga Service — initial schema.
-- Sub-stage 3.9.d.
--
-- One table: order_sagas, one row per order. The state column
-- drives the in-process state machine (services/saga/state.go);
-- the expires_at column + TTL index powers the watchdog sweep
-- across restarts (3.9.c follow-up). updated_at is bumped on every
-- Handle call for observability.

CREATE TABLE IF NOT EXISTS order_sagas (
    order_id    UUID         PRIMARY KEY,
    state       TEXT         NOT NULL,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    expires_at  TIMESTAMPTZ  NOT NULL
);

-- TTL index: a periodic job (or the watchdog's restart sweep)
-- selects rows WHERE expires_at < NOW() to compensate abandoned
-- sagas that died before their in-process watchdog could fire.
CREATE INDEX IF NOT EXISTS idx_order_sagas_expires
    ON order_sagas (expires_at);