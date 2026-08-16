UPDATE payment_outbox
SET status = 'FAILED'
WHERE event_id = ANY($1)
  AND status = 'PENDING'