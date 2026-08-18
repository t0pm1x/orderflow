-- orderflow Payment Service — unique constraint on payments.order_id.
-- Sub-stage v1.1.
--
-- The handler inserts with ON CONFLICT (id) DO NOTHING where id is
-- a fresh UUID per delivery. On a redelivered PaymentRequested the
-- handler generates a new paymentID, so the conflict never fires
-- and a duplicate payments row is written.
--
-- Adding a UNIQUE constraint on order_id makes ON CONFLICT (order_id)
-- DO NOTHING the dedup key — the saga only charges each order once
-- even if the event is replayed.

ALTER TABLE payments ADD CONSTRAINT payments_order_id_unique UNIQUE (order_id);