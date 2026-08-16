INSERT INTO order_outbox (
    event_id,
    event_type,
    aggregate_id,
    aggregate_type,
    schema_version,
    topic,
    payload,
    status,
    created_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, NOW()
)