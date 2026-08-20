-- orderflow Inventory Service — autonomous (non-tx) UPDATE that
-- increments the per-row `attempts` column on every publish
-- failure. See services/order/internal/outbox/bumpAttempts.sql
-- for the full rationale (OBX-001).
UPDATE inventory_outbox
   SET attempts   = attempts + 1,
       last_error = COALESCE($1, last_error)
 WHERE event_id = ANY($2)
   AND status = 'PENDING'