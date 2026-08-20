-- orderflow Inventory Service — SAGA-3 schema addition.
--
-- Tracks per-reservation stock holdings so a saga can release ONLY
-- the stock it actually reserved (the pre-fix release.sql keyed on
-- sku+qty only, letting a release for items[1..n] decrement a
-- different order's reservation — cross-order stock theft).
--
-- The pre-fix asymmetry was:
--   OrderCreated emits StockReserveRequested for items[0] only
--   PaymentFailed emits StockReleaseRequested for ALL items
-- So a release for items[1..n] could match the wrong order's
-- reserved counter and decrement stock the saga never reserved.
--
-- New flow:
--   ReserveStock:  INSERT INTO stock_reservations (reservation_id, sku, quantity, order_id)
--                  UPDATE stock_items SET reserved=+q, available=-q WHERE sku=$sku
--                  (same tx, atomic)
--   ReleaseStock:  DELETE FROM stock_reservations WHERE reservation_id=$rid RETURNING sku, quantity
--                  UPDATE stock_items SET reserved=-q, available=+q WHERE sku=$sku
--                  (same tx, atomic — only succeeds when the DELETE returns a row)
--
-- The reservation row is the per-reservation proof of ownership. A
-- release that doesn't find its reservation row is a no-op (rows
-- affected = 0) and surfaces to the caller as ErrNotFound so the
-- inventory consumer can ack-and-skip the orphan event.

CREATE TABLE IF NOT EXISTS stock_reservations (
    reservation_id TEXT        NOT NULL PRIMARY KEY,
    sku            TEXT        NOT NULL,
    quantity       INTEGER     NOT NULL CHECK (quantity > 0),
    order_id       TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stock_reservations_sku
    ON stock_reservations (sku);