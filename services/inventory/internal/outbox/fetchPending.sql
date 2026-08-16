SELECT event_id, event_type, aggregate_id, aggregate_type,
       schema_version, topic, payload
FROM inventory_outbox
WHERE status = 'PENDING'
ORDER BY created_at ASC
LIMIT $1