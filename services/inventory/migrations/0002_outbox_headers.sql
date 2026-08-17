-- orderflow Inventory Service — outbox headers column.
-- Sub-stage 3.10.b.
--
-- Adds a JSONB column to carry per-record Kafka headers (notably
-- W3C traceparent — propagated into the Envelope and onto the
-- outgoing record by pkg/outbox.KafkaPublisher).

ALTER TABLE inventory_outbox
    ADD COLUMN headers JSONB NOT NULL DEFAULT '{}'::jsonb;