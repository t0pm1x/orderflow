SELECT event_id, event_type, aggregate_id, aggregate_type,
       schema_version, topic, payload
FROM payment_outbox
WHERE status = 'PENDING'
ORDER BY created_at ASC
LIMIT $1
FOR UPDATE SKIP LOCKED