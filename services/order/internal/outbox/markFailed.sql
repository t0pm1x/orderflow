UPDATE order_outbox
SET status = 'FAILED',
    last_error = COALESCE($1, last_error)
WHERE event_id = ANY($2)
  AND status = 'PENDING'