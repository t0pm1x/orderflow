SELECT version,
       CASE WHEN $2::boolean THEN available ELSE reserved END AS stock
FROM stock_items
WHERE sku = $1