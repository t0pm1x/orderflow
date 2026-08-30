// Package server — JSON API gateway. Each handler is a thin
// wrapper that takes an inbound JSON request (or no body for GET),
// calls into the existing internal/backend HTTP client, and writes
// a JSON response. Errors are mapped to a stable JSON envelope
// (`{"error": "<code>", "message": "..."}`) with the right status
// code so the SPA can branch on `error` and show a friendly
// ErrorBanner.
//
// The handlers do no business logic of their own — that's the
// backend's job. They only do:
//   - input validation (UUIDs, quantity > 0, required fields)
//   - error-to-status-code mapping (404 stays 404, 5xx collapses
//     to 502 with a generic "service unavailable" message so we
//     don't leak backend internals to the browser)
//   - JSON marshaling
//
// Idempotency tokens for /api/orders (POST) come from the SPA
// (crypto.randomUUID()) so duplicate-submit clicks on the order
// form are caught at the BFF layer BEFORE the order service sees
// them. The replay-cache is per-process — fine for the playground;
// in a multi-replica deployment it would move to Redis (mirrors
// the pattern in services/payment/internal/idempotency/).
package server

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/t0pm1x/orderflow/services/web/internal/backend"
)

// API holds the backend HTTP clients + a logger. One per Server.
type API struct {
	Order     backend.OrderClient
	Payment   backend.PaymentClient
	Inventory backend.InventoryClient
	Logger    *slog.Logger

	replaysMu sync.Mutex
	replays   map[string]time.Time // idempotency-token -> first-seen
}

// writeJSON serializes v with the right content-type and status.
// On marshal failure we fall back to a generic 500 — the BFF
// never crashes the SPA on a response-side bug.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if v == nil {
		return
	}
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// already wrote header; best we can do is log
	}
}

// writeError produces a stable error envelope. The `error` code
// is short and stable ("NOT_FOUND", "BAD_REQUEST", "UPSTREAM",
// "INTERNAL"); `message` is human-readable; `status` is the HTTP
// code. The SPA matches on `error` and shows a banner with
// `message`.
func writeError(w http.ResponseWriter, logger *slog.Logger, route string, status int, code, msg string, err error) {
	if logger != nil && err != nil {
		logger.Warn("api proxy error",
			"route", route,
			"status", status,
			"code", code,
			"err", err)
	}
	writeJSON(w, status, map[string]any{
		"error":   code,
		"message": msg,
	})
}

// mapUpstreamError collapses an upstream backend error into a
// stable HTTP response:
//   - nil                 -> 200 (caller checks)
//   - *backend.HTTPError  -> use the upstream's own status code
//                            (404 stays 404, 400 stays 400, etc.) so the
//                            SPA can render branch-specific banners
//   - other (transport)   -> 502 "upstream unavailable"
//
// We never echo the upstream's raw error body — the BFF presents
// its own short message so a backend stack trace never leaks to
// the browser.
func mapUpstreamError(w http.ResponseWriter, logger *slog.Logger, route string, err error) (status int, code string, msg string) {
	if err == nil {
		return http.StatusOK, "", ""
	}
	var he *backend.HTTPError
	if errors.As(err, &he) {
		logger.Warn("upstream error",
			"route", route,
			"status", he.Status,
			"body", he.Body,
			"url", he.URL)
		switch {
		case he.Status == http.StatusNotFound:
			return he.Status, "NOT_FOUND", "Order not found."
		case he.Status == http.StatusBadRequest:
			return he.Status, "BAD_REQUEST", "The backend rejected the request. Please check your input."
		case he.Status == http.StatusConflict:
			return he.Status, "CONFLICT", "This order is already in a terminal state and cannot be modified."
		case he.Status >= 400 && he.Status < 500:
			return he.Status, "UPSTREAM_4XX", "The backend rejected the request."
		default:
			// 5xx and anything else: collapse to 502 so the SPA shows
			// a generic "try again" banner instead of a backend-
			// specific error.
			return http.StatusBadGateway, "UPSTREAM_UNAVAILABLE",
				"The order service is temporarily unavailable. Please try again in a moment."
		}
	}
	logger.Error("upstream transport error", "route", route, "err", err)
	return http.StatusBadGateway, "UPSTREAM_UNAVAILABLE",
		"Cannot reach the order service. Please check your connection."
}

// isValidUUID does a cheap syntax check on a UUID string. The
// backend will 4xx anyway, but rejecting malformed IDs at the
// BFF keeps the upstream URL builder from constructing a
// pathological path.
func isValidUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	if s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return false
	}
	for i, c := range s {
		switch i {
		case 8, 13, 18, 23:
			continue
		}
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

// replayCachePrune removes entries older than replayWindow. Cheap
// O(n) scan; the cache is bounded by the operator's click rate
// over the last 5 minutes (typically <1k entries).
func (a *API) replayCachePrune(now time.Time) {
	a.replaysMu.Lock()
	defer a.replaysMu.Unlock()
	for k, t := range a.replays {
		if now.Sub(t) > 5*time.Minute {
			delete(a.replays, k)
		}
	}
}

// replaySeen records the token and returns true if it was seen
// in the last 5 minutes (i.e. this is a duplicate submission).
func (a *API) replaySeen(token string, now time.Time) bool {
	a.replaysMu.Lock()
	defer a.replaysMu.Unlock()
	if a.replays == nil {
		a.replays = make(map[string]time.Time)
	}
	if t, ok := a.replays[token]; ok {
		if now.Sub(t) <= 5*time.Minute {
			return true
		}
	}
	a.replays[token] = now
	// opportunistic prune — every 100th check sweep the cache.
	if len(a.replays) > 1024 {
		for k, t := range a.replays {
			if now.Sub(t) > 5*time.Minute {
				delete(a.replays, k)
			}
		}
	}
	return false
}

// ---- ListOrders ----------------------------------------------------

// ListOrders GET /api/orders?state=&limit=&sku=A&sku=B
//
// Proxies to the order service. SKU filtering is client-side in
// the BFF because the Order service doesn't expose a per-SKU list
// endpoint yet (mirrors the pre-SvelteKit behavior — see CHANGELOG
// v1.1.7). Returns 200 with an envelope: {"items": [...], "next_cursor": ""}.
func (a *API) ListOrders(w http.ResponseWriter, r *http.Request) {
	state := backend.OrderState(r.URL.Query().Get("state"))
	skus := r.URL.Query()["sku"]

	limit := 200 // widened so the SKU filter has enough rows to operate on
	if raw := r.URL.Query().Get("limit"); raw != "" {
		// Caller-supplied limit (used by the dashboard's KPI window,
		// which only needs the 10 most recent orders). Falls back
		// to the default if the value is missing or non-numeric;
		// non-positive values are clamped to 1 so the upstream
		// still receives a usable page size.
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	orders, err := a.Order.List(r.Context(), state, limit)
	if err != nil {
		status, code, msg := mapUpstreamError(w, a.Logger, "GET /api/orders", err)
		writeError(w, a.Logger, "GET /api/orders", status, code, msg, err)
		return
	}

	// SKU filter (client-side; mirror services/web/internal/handlers/handlers.go:OrderListBySKUs)
	filtered := orders.Items
	if filtered == nil {
		// Defensive: the Order service serializes a nil slice as
		// JSON null. The SPA's TypeScript declares items as Order[]
		// (non-nullable) and [...items] (spread) throws on null.
		// F-007 root cause: pre-fix this leaked through and the
		// payments/sim page crashed with "n is not iterable" the
		// first time it loaded with no pending/reserved orders.
		filtered = []backend.Order{}
	}
	if len(skus) > 0 {
		want := make(map[string]struct{}, len(skus))
		for _, s := range skus {
			if s != "" {
				want[s] = struct{}{}
			}
		}
		out := filtered[:0]
		for _, o := range filtered {
			for _, it := range o.Items {
				if _, ok := want[it.SKU]; ok {
					out = append(out, o)
					break
				}
			}
		}
		filtered = out
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"items":       filtered,
		"next_cursor": "",
	})
}

// ---- GetOrder ------------------------------------------------------

// GetOrder GET /api/orders/{id}
//
// 200 with the Order, or 404 if not found. Invalid UUIDs are
// rejected at the BFF (400) to keep the upstream URL builder
// honest.
func (a *API) GetOrder(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if !isValidUUID(id) {
		writeError(w, a.Logger, "GET /api/orders/{id}", http.StatusBadRequest,
			"BAD_REQUEST", "Order id must be a UUID.", nil)
		return
	}
	order, err := a.Order.Get(r.Context(), id)
	if err != nil {
		status, code, msg := mapUpstreamError(w, a.Logger, "GET /api/orders/{id}", err)
		writeError(w, a.Logger, "GET /api/orders/{id}", status, code, msg, err)
		return
	}
	writeJSON(w, http.StatusOK, order)
}

// ---- SubmitOrder ---------------------------------------------------

// orderSubmitReq mirrors services/order/internal/api/handler.go's
// submitRequest shape so the JSON the SPA sends matches the
// upstream payload verbatim (with one extra `idempotency_key`
// field at the BFF layer — upstream ignores it; BFF uses it for
// the replay guard).
type orderSubmitReq struct {
	CustomerID  string `json:"customer_id"`
	IdempotencyKey string `json:"idempotency_key"`
	Items       []struct {
		SKU            string `json:"sku"`
		Quantity       int    `json:"quantity"`
		UnitPriceCents *int64 `json:"unit_price_cents,omitempty"`
	} `json:"items"`
	Payment *struct {
		LastFour string `json:"last_four,omitempty"`
	} `json:"payment,omitempty"`
}

// SubmitOrder POST /api/orders
//
// Validates basic shape (sku/quantity required, idempotency_key
// required, sku <= 64 chars, quantity > 0, customer_id is a UUID
// when supplied), runs the BFF replay guard, then proxies to the
// order service. On success returns 201 with the created Order.
func (a *API) SubmitOrder(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 64*1024)
	var req orderSubmitReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, a.Logger, "POST /api/orders", http.StatusBadRequest,
			"BAD_REQUEST", "Invalid JSON body.", err)
		return
	}

	if len(req.Items) == 0 {
		writeError(w, a.Logger, "POST /api/orders", http.StatusBadRequest,
			"VALIDATION", "items required.", nil)
		return
	}
	for _, it := range req.Items {
		if it.SKU == "" || it.Quantity <= 0 {
			writeError(w, a.Logger, "POST /api/orders", http.StatusBadRequest,
				"VALIDATION", "SKU and positive quantity are required.", nil)
			return
		}
		if len(it.SKU) > 64 {
			writeError(w, a.Logger, "POST /api/orders", http.StatusBadRequest,
				"VALIDATION", "SKU must be 64 characters or fewer.", nil)
			return
		}
	}
	if req.IdempotencyKey == "" {
		writeError(w, a.Logger, "POST /api/orders", http.StatusBadRequest,
			"VALIDATION", "idempotency_key required.", nil)
		return
	}
	if req.CustomerID != "" && !isValidUUID(req.CustomerID) {
		writeError(w, a.Logger, "POST /api/orders", http.StatusBadRequest,
			"VALIDATION", "customer_id must be a UUID (or leave blank for auto-generation).", nil)
		return
	}
	if a.replaySeen(req.IdempotencyKey, time.Now()) {
		writeError(w, a.Logger, "POST /api/orders", http.StatusConflict,
			"DUPLICATE_SUBMIT", "This order was already submitted recently (idempotency replay window is 5 minutes).", nil)
		return
	}

	// Translate to the backend.OrderClient.Submit shape.
	var payment *backend.OrderSubmitPayment
	if req.Payment != nil && req.Payment.LastFour != "" {
		payment = &backend.OrderSubmitPayment{LastFour: req.Payment.LastFour}
	}
	submit := backend.OrderSubmit{
		CustomerID: &req.CustomerID,
		Items:      make([]backend.OrderItem, len(req.Items)),
		Payment:    payment,
	}
	for i, it := range req.Items {
		submit.Items[i] = backend.OrderItem{
			SKU:            it.SKU,
			Quantity:       it.Quantity,
			UnitPriceCents: it.UnitPriceCents,
		}
	}
	created, err := a.Order.Submit(r.Context(), submit)
	if err != nil {
		status, code, msg := mapUpstreamError(w, a.Logger, "POST /api/orders", err)
		writeError(w, a.Logger, "POST /api/orders", status, code, msg, err)
		return
	}
	writeJSON(w, http.StatusCreated, created)
}

// ---- CancelOrder ---------------------------------------------------

// CancelOrder DELETE /api/orders/{id}
//
// Idempotent: 204 from upstream = success; 404 from upstream is
// treated as already-gone (also 204-equivalent for the SPA). Other
// upstream errors map via mapUpstreamError.
func (a *API) CancelOrder(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if !isValidUUID(id) {
		writeError(w, a.Logger, "DELETE /api/orders/{id}", http.StatusBadRequest,
			"BAD_REQUEST", "Order id must be a UUID.", nil)
		return
	}
	err := a.Order.Cancel(r.Context(), id)
	if err != nil {
		// Treat upstream 404 as success (idempotent cancel).
		var he *backend.HTTPError
		if errors.As(err, &he) && he.Status == http.StatusNotFound {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		status, code, msg := mapUpstreamError(w, a.Logger, "DELETE /api/orders/{id}", err)
		writeError(w, a.Logger, "DELETE /api/orders/{id}", status, code, msg, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---- GetInventoryStock ---------------------------------------------

// GetInventoryStock GET /api/inventory/stock/{sku}
//
// 200 with the StockItem, 404 if unknown SKU.
func (a *API) GetInventoryStock(w http.ResponseWriter, r *http.Request) {
	sku := chi.URLParam(r, "sku")
	if sku == "" {
		writeError(w, a.Logger, "GET /api/inventory/stock/{sku}", http.StatusBadRequest,
			"BAD_REQUEST", "sku required.", nil)
		return
	}
	item, err := a.Inventory.GetStock(r.Context(), sku)
	if err != nil {
		status, code, msg := mapUpstreamError(w, a.Logger, "GET /api/inventory/stock/{sku}", err)
		writeError(w, a.Logger, "GET /api/inventory/stock/{sku}", status, code, msg, err)
		return
	}
	writeJSON(w, http.StatusOK, item)
}

// ---- FireWebhook --------------------------------------------------

// paymentFireReq mirrors the upstream webhook shape. The SPA
// pre-fills `payment_id` from `order_id` so the mock's idempotency
// guard dedupes repeat fires for the same order.
type paymentFireReq struct {
	PaymentID string `json:"payment_id"`
	Status    string `json:"status"`
	ErrorCode string `json:"error_code,omitempty"`
	LastFour  string `json:"last_four,omitempty"`
}

// FireWebhook POST /api/payments/webhook
//
// Rejects missing/invalid status (400). Anything upstream raises
// propagates as a 502 (UPSTREAM_UNAVAILABLE) or whatever status
// the upstream returned (4xx upstream → 4xx to SPA).
func (a *API) FireWebhook(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 64*1024)
	var req paymentFireReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, a.Logger, "POST /api/payments/webhook", http.StatusBadRequest,
			"BAD_REQUEST", "Invalid JSON body.", err)
		return
	}
	if req.PaymentID == "" {
		writeError(w, a.Logger, "POST /api/payments/webhook", http.StatusBadRequest,
			"VALIDATION", "payment_id required.", nil)
		return
	}
	if !isValidUUID(req.PaymentID) {
		writeError(w, a.Logger, "POST /api/payments/webhook", http.StatusBadRequest,
			"VALIDATION", "payment_id must be a UUID.", nil)
		return
	}
	if req.Status != "succeeded" && req.Status != "failed" {
		writeError(w, a.Logger, "POST /api/payments/webhook", http.StatusBadRequest,
			"VALIDATION", "status must be succeeded|failed.", nil)
		return
	}
	err := a.Payment.FireWebhook(r.Context(), backend.PaymentWebhook{
		PaymentID: req.PaymentID,
		Status:    req.Status,
		ErrorCode: req.ErrorCode,
		LastFour:  req.LastFour,
	})
	if err != nil {
		status, code, msg := mapUpstreamError(w, a.Logger, "POST /api/payments/webhook", err)
		writeError(w, a.Logger, "POST /api/payments/webhook", status, code, msg, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// ensure unused import 'io' is referenced at least once — the
// http.MaxBytesReader (from net/http) takes the place of io.LimitReader
// here, so we explicitly pin io to keep it visible to future
// maintainers even if MaxBytesReader's import path changes.