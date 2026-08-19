package backend

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// List fetches orders from the Order service, optionally filtered
// by lifecycle state (pending/reserved/confirmed/cancelled/failed).
// limit > 0 sets the page size; 0 or negative lets the upstream
// pick its own default.
func (c *HTTPClient) List(ctx context.Context, state OrderState, limit int) (*OrderList, error) {
	u := c.orderURL + "/v1/orders"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("order list: %w", err)
	}
	if state != "" {
		q := req.URL.Query()
		q.Set("state", string(state))
		req.URL.RawQuery = q.Encode()
	}
	if limit > 0 {
		q := req.URL.Query()
		q.Set("limit", fmt.Sprintf("%d", limit))
		req.URL.RawQuery = q.Encode()
	}
	var out OrderList
	if err := c.do(req, &out); err != nil {
		return nil, fmt.Errorf("order list: %w", err)
	}
	return &out, nil
}

// Get fetches a single Order by id (UUID). Returns nil, err if
// the id is unknown (404) or the upstream is unreachable.
func (c *HTTPClient) Get(ctx context.Context, id string) (*Order, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("%s/v1/orders/%s", c.orderURL, url.PathEscape(id)), nil)
	if err != nil {
		return nil, fmt.Errorf("order get: %w", err)
	}
	var out Order
	if err := c.do(req, &out); err != nil {
		return nil, fmt.Errorf("order get: %w", err)
	}
	return &out, nil
}

// Submit posts a new Order to the Order service and returns the
// resulting row. Validation (500 / 4xx) propagates as *HTTPError
// from do so callers can branch on Status. When
// OrderSubmit.IdempotencyKey is non-empty the client also sets
// the `Idempotency-Key` header (prefixed with `orderflow-web:` —
// see payment.go for the prefix constant) so a future idempotency
// middleware in the Order service can dedupe replays of the same
// form-render token.
func (c *HTTPClient) Submit(ctx context.Context, in OrderSubmit) (*Order, error) {
	body, _ := json.Marshal(in)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.orderURL+"/v1/orders", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("order submit: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if in.IdempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyPrefix+in.IdempotencyKey)
	}
	var out Order
	if err := c.do(req, &out); err != nil {
		return nil, fmt.Errorf("order submit: %w", err)
	}
	return &out, nil
}

// Cancel is idempotent: 204 and 404 both succeed.
func (c *HTTPClient) Cancel(ctx context.Context, id string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete,
		fmt.Sprintf("%s/v1/orders/%s", c.orderURL, url.PathEscape(id)), nil)
	if err != nil {
		return fmt.Errorf("order cancel: %w", err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("order cancel: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusNotFound {
		return nil
	}
	return fmt.Errorf("order cancel: status %d", resp.StatusCode)
}

// do runs req and decodes a JSON body on 2xx; on non-2xx returns a
// *HTTPError so callers can branch on Status (4xx user errors are
// usually safe to surface as 400 to the end user; 5xx stays 502).
func (c *HTTPClient) do(req *http.Request, out any) error {
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return &HTTPError{
			Status: resp.StatusCode,
			Body:   strings.TrimSpace(string(body)),
			URL:    req.URL.Path,
		}
	}
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return fmt.Errorf("decode %s %s: %w", req.Method, req.URL.Path, err)
		}
	}
	return nil
}
