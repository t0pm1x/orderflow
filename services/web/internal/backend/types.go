// Package backend holds typed HTTP clients that wrap the upstream
// Order, Payment, and Inventory services exposed by the orderflow
// platform. Handlers in services/web depend on the interfaces
// defined here so they can be tested against fakes.
package backend

import "time"

// OrderItem matches `OrderItem` in api/openapi.yaml:238-256.
type OrderItem struct {
	SKU            string `json:"sku"`
	Quantity       int    `json:"quantity"`
	UnitPriceCents *int64 `json:"unit_price_cents,omitempty"`
}

// OrderSubmit matches `OrderSubmit` in api/openapi.yaml:224-236.
// IdempotencyKey is NOT serialized into the request body; the
// HTTPClient forwards it as the `Idempotency-Key` header (see
// internal/backend/order.go). Keeping it on the struct (rather
// than threading it as a separate argument) lets the BFF set
// both request body and header from one call site without
// breaking the OrderClient interface signature.
type OrderSubmit struct {
	CustomerID *string     `json:"customer_id,omitempty"`
	Items      []OrderItem `json:"items"`
	IdempotencyKey string   `json:"-"`
}

// OrderState matches `OrderState` in api/openapi.yaml:216-222.
// The lifecycle is: pending → reserved → confirmed, with
// cancelled / failed reachable from any non-terminal state.
type OrderState string

const (
	// OrderStatePending — order accepted, awaiting stock reservation.
	OrderStatePending OrderState = "pending"
	// OrderStateReserved — stock reserved, awaiting payment.
	OrderStateReserved OrderState = "reserved"
	// OrderStateConfirmed — payment captured, terminal happy path.
	OrderStateConfirmed OrderState = "confirmed"
	// OrderStateCancelled — user-initiated cancel or saga compensation.
	OrderStateCancelled OrderState = "cancelled"
	// OrderStateFailed — saga timed out before reaching a clean state.
	OrderStateFailed OrderState = "failed"
)

// Order matches `Order` in api/openapi.yaml:257-288.
type Order struct {
	ID            string      `json:"id"`
	CustomerID    *string     `json:"customer_id,omitempty"`
	Items         []OrderItem `json:"items"`
	State         OrderState  `json:"state"`
	TotalCents    *int64      `json:"total_cents,omitempty"`
	CreatedAt     time.Time   `json:"created_at"`
	UpdatedAt     time.Time   `json:"updated_at"`
	CompletedAt   *time.Time  `json:"completed_at,omitempty"`
	FailureReason *string     `json:"failure_reason,omitempty"`
}

// OrderList matches `OrderList` in api/openapi.yaml:290-300.
type OrderList struct {
	Items      []Order `json:"items"`
	NextCursor *string `json:"next_cursor,omitempty"`
}

// StockItem matches `model.Stock` in
// services/inventory/internal/model/stock.go:12-18, which is what
// `GET /v1/inventory/stock/{sku}` returns. The inventory service
// only exposes single-SKU reads; deriving the SKU list for the UI
// is the handler layer's problem (Task 8).
type StockItem struct {
	SKU       string    `json:"sku"`
	Available int       `json:"available"`
	Reserved  int       `json:"reserved"`
	Version   int64     `json:"version"`
	UpdatedAt time.Time `json:"updated_at"`
}

// PaymentWebhook matches `PaymentWebhook` in api/openapi.yaml:302-316.
type PaymentWebhook struct {
	PaymentID string `json:"payment_id"`
	Status    string `json:"status"` // "succeeded" | "failed"
	ErrorCode string `json:"error_code,omitempty"`
}
