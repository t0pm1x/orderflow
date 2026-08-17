-- orderflow Test fixtures — sub-stage 3.11.a.
--
-- This file is the seed baseline that E2E, chaos, and load tests
-- apply against the harness-managed Postgres instances before driving
-- traffic through the orderflow service binaries. It is NOT applied
-- automatically by the harness itself; each test applies what it
-- needs (typically via psql -f or pgx).
--
-- Scope:
--   stock_items        — a small SKU catalog with available stock
--                        (services/inventory/migrations/0001_init.sql
--                        creates the table empty).
--   idempotency_keys   — intentionally empty; payment-service
--                        runtime creates Redis-backed IN_FLIGHT
--                        markers and the schema mirrors them.
--   orders             — empty by default; tests insert their own.
--   payments           — empty by default; tests insert their own.
--   order_sagas        — empty by default; saga-service creates rows.

INSERT INTO stock_items (sku, available, reserved) VALUES
    ('SKU-001', 100, 0),
    ('SKU-002',  50, 0),
    ('SKU-003', 200, 0)
ON CONFLICT (sku) DO NOTHING;
