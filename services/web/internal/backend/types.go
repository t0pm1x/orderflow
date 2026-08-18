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
type OrderSubmit struct {
	CustomerID *string     `json:"customer_id,omitempty"`
	Items      []OrderItem `json:"items"`
}

// OrderState matches `OrderState` in api/openapi.yaml:216-222.
type OrderState string

const (
	OrderStatePending   OrderState = "pending"
	OrderStateReserved  OrderState = "reserved"
	OrderStateConfirmed OrderState = "confirmed"
	OrderStateCancelled OrderState = "cancelled"
	OrderStateFailed    OrderState = "failed"
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

// StockItem is a best-effort decode of GET /v1/inventory/stock.
// The OpenAPI spec doesn't define it explicitly; the existing
// handler at services/inventory/internal/api/handler.go returns
// `[]Stock{...}` with fields sku, available_qty, reserved_qty,
// version. We match that local contract.
type StockItem struct {
	SKU         string  `json:"sku"`
	Available   int64   `json:"available_qty"`
	Reserved    int64   `json:"reserved_qty"`
	Version     int64   `json:"version"`
	Description *string `json:"description,omitempty"`
}

// PaymentWebhook matches `PaymentWebhook` in api/openapi.yaml:302-316.
type PaymentWebhook struct {
	PaymentID string `json:"payment_id"`
	Status    string `json:"status"` // "succeeded" | "failed"
	ErrorCode string `json:"error_code,omitempty"`
}
