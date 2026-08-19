-- orderflow Saga Service — v1.1.5 schema additions.
--
-- Adds a last_four column to order_sagas so the saga can carry the
-- client's payment hint through to the PaymentRequested event. The
-- pre-v1.1.5 saga handlers had no way to forward the order's
-- payment info (the order service's submitRequest accepted it but
-- silently dropped it), so the payment provider fell back to
-- deriving last_four from orderID[len(orderID)-4:]. That made the
-- compensation test's "last_four=0001 forces decline" claim
-- meaningless — a fresh UUID virtually never ends in 0001, so the
-- test was passing on the happy path instead of the failure path.
--
-- The new flow: order POST → OrderCreated(last_four) → saga row
-- INSERT with last_four → PaymentRequested(last_four) →
-- provider.Charge(cardID, amount, last_four). Empty string means
-- "client did not pass payment info" — preserved as a fall-back
-- to the pre-v1.1.5 derived-from-orderID behavior in the payment
-- service.

ALTER TABLE order_sagas
    ADD COLUMN IF NOT EXISTS last_four TEXT NOT NULL DEFAULT '';