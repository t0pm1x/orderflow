-- Seed stock for e2e/demo flows. Idempotent (ON CONFLICT DO NOTHING)
-- so it is safe to keep alongside production migrations: a real
-- inventory database will already have its own stock_items rows
-- populated by the warehouse operator, and this seed is a no-op
-- for any sku that already exists.
INSERT INTO stock_items (sku, available, reserved) VALUES
    ('SKU-001', 100, 0),
    ('SKU-002', 100, 0)
ON CONFLICT (sku) DO NOTHING;
