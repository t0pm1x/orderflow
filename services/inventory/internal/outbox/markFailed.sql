UPDATE inventory_outbox
SET status = 'FAILED',
    attempts = attempts + 1,
    last_error = COALESCE($1, last_error)
WHERE event_id = ANY($2)
  AND status = 'PENDING'
