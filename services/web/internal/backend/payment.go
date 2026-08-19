package backend

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// idempotencyPrefix tags the keys this client emits so the payment
// service's idempotency middleware can attribute replays to the
// web playground rather than an external provider.
const idempotencyPrefix = "orderflow-web:"

// FireWebhook posts a Payment webhook to the Payment service. The
// function is also used by the web playground's force-success /
// force-fail simulator (see /payments/sim). Sets a deterministic
// Idempotency-Key so the upstream's idempotency middleware can
// dedupe replays.
func (c *HTTPClient) FireWebhook(ctx context.Context, w PaymentWebhook) error {
	body, _ := json.Marshal(w)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.paymentURL+"/v1/payments/webhook", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("payment webhook: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	// Idempotency-Key is REQUIRED by the payment webhook when
	// REDIS_URL is configured: without it, the middleware returns
	// 400 "Idempotency-Key header required". The signature is
	// deterministic per (order_id, status) so a UI replay hits the
	// cached response instead of producing a fresh downstream
	// event; a different status (force ✓ vs force ✗) is a
	// different key by design.
	req.Header.Set("Idempotency-Key", idempotencyPrefix+w.PaymentID+":"+w.Status)
	if err := c.do(req, nil); err != nil {
		return fmt.Errorf("payment webhook: %w", err)
	}
	return nil
}
