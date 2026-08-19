-- orderflow Order Service — orders.last_four column.
-- Task 9 (P0.5) of the 2026-08-19 web audit-and-polish plan.
--
-- The order service's submit handler accepts a `payment.last_four`
-- hint from the client and threads it onto the OrderCreated event
-- so the saga can forward it to the payment provider. The
-- services/order/internal/api/handler.submit path already sets
-- o.LastFour on the in-memory Order, but the column has never been
-- persisted on the orders row — the Get handler therefore always
-- returned LastFour="" to the playground, even after a successful
-- submit.
--
-- The summary card on the web /orders/{id} page re-binds
-- Order.LastFour into the response (handlers.PageOrderDetail), so
-- once Get reads the column back the playground will show the same
-- last_four the client submitted. Matches the saga's column added
-- in services/saga/migrations/0003_saga_payment_last_four.sql.
--
-- NULLABLE on purpose: an empty Payment block in the submit body
-- is allowed (preserves the v1.x behavior for callers that didn't
-- send payment info). The Get path scans into a sql.NullString so a
-- NULL column maps to "" on the wire, exactly the same as the
-- in-memory Order.LasFour omitempty default.
--
-- ADD COLUMN IF NOT EXISTS makes the migration idempotent against
-- the test harness applyMigrations helper which re-runs every
-- file in lexical order on every test.

ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS last_four TEXT;
