UPDATE stock_items SET
    available = available + $2,
    reserved = reserved - $2,
    version = version + 1,
    updated_at = NOW()
WHERE sku = $1
  AND version = $3
  AND reserved >= $2
RETURNING sku, available, reserved, version, updated_at