-- orderflow Order Service — autonomous (non-tx) UPDATE that
-- increments the per-row `attempts` column on every publish
-- failure. Pairs with Source.BumpAttempts (OBX-001).
--
-- Pre-fix, the v1.1.4 "DB attempts survives restarts" claim was
-- inert: MarkFailedTx was the only writer of `attempts`, and
-- MarkFailedTx was only called at the terminal FAILED transition
-- (which excluded the row from future fetches). So every PENDING
-- row had attempts=0 in the DB.
--
-- This UPDATE runs OUTSIDE the locked run-in-tx closure so it
-- commits independently of the rollback path. The `status =
-- 'PENDING'` guard is the same one MarkFailedTx uses — if the
-- row has already been transitioned to SENT or FAILED by a
-- concurrent replica, we don't double-count.
UPDATE order_outbox
   SET attempts   = attempts + 1,
       last_error = COALESCE($1, last_error)
 WHERE event_id = ANY($2)
   AND status = 'PENDING'