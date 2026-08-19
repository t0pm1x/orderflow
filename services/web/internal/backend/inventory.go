package backend

import (
	"context"
	"fmt"
	"net/http"
)

// GetStock fetches the current Inventory row for sku from the
// Inventory service. Returns nil, err if the SKU is unknown or
// the upstream is unreachable.
func (c *HTTPClient) GetStock(ctx context.Context, sku string) (*StockItem, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("%s/v1/inventory/stock/%s", c.inventoryURL, sku), nil)
	if err != nil {
		return nil, fmt.Errorf("inventory get: %w", err)
	}
	var out StockItem
	if err := c.do(req, &out); err != nil {
		return nil, fmt.Errorf("inventory get: %w", err)
	}
	return &out, nil
}
