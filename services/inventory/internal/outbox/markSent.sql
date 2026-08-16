UPDATE inventory_outbox
SET status = 'SENT', sent_at = NOW()
WHERE event_id = ANY($1)
  AND status = 'PENDING'