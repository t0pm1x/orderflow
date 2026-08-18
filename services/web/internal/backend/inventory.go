package backend

import (
	"context"
	"fmt"
	"net/http"
)

func (c *HTTPClient) ListStock(ctx context.Context) ([]StockItem, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.inventoryURL+"/v1/inventory/stock", nil)
	if err != nil {
		return nil, fmt.Errorf("inventory list: %w", err)
	}
	var out []StockItem
	if err := c.do(req, &out); err != nil {
		return nil, fmt.Errorf("inventory list: %w", err)
	}
	return out, nil
}
