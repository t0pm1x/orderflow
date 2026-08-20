-- SAGA-3: the saga's release flow keys on reservation_id (via
-- ReleaseStock in services/inventory/internal/repository/pg_repo.go,
-- which DELETEs from stock_reservations first and only proceeds on
-- success). This release.sql is the alternative
-- version-checked UPDATE used by lock.PGLocker.Release. The lock
-- path is currently unused by the saga's release flow (the
-- consumer calls PGRepo.ReleaseStock directly), but kept
-- reservation-aware so any future caller can't reintroduce the
-- cross-order stock-theft hole.
--
-- The CTE pattern: only run the UPDATE when the reservation row
-- exists for this reservation_id. Returns the same shape as the
-- pre-fix version, so the locker stays drop-in compatible.

WITH claimed AS (
    DELETE FROM stock_reservations
     WHERE reservation_id = $4
     RETURNING sku, quantity
)
UPDATE stock_items SET
    available = available + $2,
    reserved = reserved - $2,
    version = version + 1,
    updated_at = NOW()
FROM claimed
WHERE stock_items.sku = $1
  AND stock_items.sku = claimed.sku
  AND stock_items.version = $3
  AND stock_items.reserved >= $2
  AND claimed.quantity = $2
RETURNING stock_items.sku, stock_items.available, stock_items.reserved, stock_items.version, stock_items.updated_at