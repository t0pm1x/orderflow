-- orderflow Order Service — initial schema.
-- Sub-stage 3.4.e.
--
-- Two tables:
--   orders         — business state
--   order_outbox   — transactional outbox (see ADR-0002)
--
-- Both are written in the same tx from services/order/internal/api
-- via INSERT INTO orders + outbox.Writer.Append(...).

CREATE TABLE IF NOT EXISTS orders (
    id            UUID         PRIMARY KEY,
    customer_id   UUID         NOT NULL,
    items         JSONB        NOT NULL,
    state         TEXT         NOT NULL,
    total_cents   BIGINT       NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    completed_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders (customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_state    ON orders (state);

-- Outbox table. Status lifecycle: PENDING → SENT (or PENDING → FAILED → DLQ).
-- The poller (sub-stage 3.7) reads PENDING rows by created_at ASC and
-- transitions them to SENT after Kafka confirms the publish.
CREATE TABLE IF NOT EXISTS order_outbox (
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

-- Hot path: poller selects PENDING rows ordered by created_at.
CREATE INDEX IF NOT EXISTS idx_order_outbox_pending
    ON order_outbox (created_at)
    WHERE status = 'PENDING';