-- orderflow Payment Service — initial schema.
-- Sub-stage 3.5.e.
--
-- Three tables:
--   payments              — business state (one row per charge)
--   idempotency_keys      — Redis-backed in-flight markers (mirror of
--                           the Redis store, kept here for crash
--                           recovery / observability — see spec note)
--   payment_outbox        — transactional outbox (ADR-0002)

CREATE TABLE IF NOT EXISTS payments (
    id              UUID         PRIMARY KEY,
    order_id        UUID         NOT NULL,
    amount_cents    BIGINT       NOT NULL,
    status          TEXT         NOT NULL,
    last_four       TEXT         NOT NULL,
    error_code      TEXT,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_payments_order ON payments (order_id);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    key             TEXT         PRIMARY KEY,
    status          TEXT         NOT NULL DEFAULT 'IN_FLIGHT',
    response_body   JSONB,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,
    expires_at      TIMESTAMPTZ  NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_idempotency_keys_expires
    ON idempotency_keys (expires_at);

CREATE TABLE IF NOT EXISTS payment_outbox (
    id              BIGSERIAL    PRIMARY KEY,
    event_id        UUID         NOT NULL UNIQUE,
    event_type      TEXT         NOT NULL,
    aggregate_id    UUID         NOT NULL,
    aggregate_type  TEXT         NOT NULL,
    schema_version  TEXT         NOT NULL,
    topic           TEXT         NOT NULL,
    payload         JSONB        NOT NULL,
    status          TEXT         NOT NULL DEFAULT 'PENDING',
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    sent_at         TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_payment_outbox_pending
    ON payment_outbox (created_at)
    WHERE status = 'PENDING';