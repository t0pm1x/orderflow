package backend

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

func (c *HTTPClient) FireWebhook(ctx context.Context, w PaymentWebhook) error {
	body, _ := json.Marshal(w)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.paymentURL+"/v1/payments/webhook", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("payment webhook: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if err := c.do(req, nil); err != nil {
		return fmt.Errorf("payment webhook: %w", err)
	}
	return nil
}
