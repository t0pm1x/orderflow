// Package events contains the wire-format payload types the Saga
// Service emits to the order-events topic. The shape of every
// payload is fixed by the cross-service spec; downstream consumers
// (inventory, payment, order) deserialize against these Go types.
package events

// StockReserveRequestedPayload is emitted when a saga starts and
// asks inventory to reserve stock for the order's first item.
// Multi-item orders are out of scope for v0.5.0 — the first item
// is reserved and subsequent items are handled in a follow-up.
type StockReserveRequestedPayload struct {
	OrderID       string `json:"order_id"`
	SKU           string `json:"sku"`
	Quantity      int    `json:"quantity"`
	ReservationID string `json:"reservation_id"`
}

// PaymentRequestedPayload is emitted after stock is reserved and
// the saga asks payment to charge the order total. IdempotencyKey
// is a fresh UUID per request so the payment service can safely
// retry without double-charging.
type PaymentRequestedPayload struct {
	OrderID        string `json:"order_id"`
	AmountCents    int64  `json:"amount_cents"`
	IdempotencyKey string `json:"idempotency_key"`
}

// OrderConfirmedPayload is emitted when the saga reaches the
// terminal "completed" state. ConfirmedAt is RFC3339 so the order
// service can store it as text without timezone parsing surprises.
type OrderConfirmedPayload struct {
	OrderID     string `json:"order_id"`
	ConfirmedAt string `json:"confirmed_at"`
}

// StockReleaseRequestedPayload is emitted when the saga enters the
// compensation flow and asks inventory to release the stock it
// reserved for this order.
type StockReleaseRequestedPayload struct {
	OrderID       string `json:"order_id"`
	ReservationID string `json:"reservation_id"`
}

// OrderCancelledPayload is emitted alongside StockReleaseRequested
// when the saga compensates. Reason is "payment_failed" or
// "stock_failed" (timeout is reserved for a follow-up); Source is
// always "saga" so the order service can distinguish saga-driven
// cancellations from user-initiated ones.
type OrderCancelledPayload struct {
	OrderID string `json:"order_id"`
	Reason  string `json:"reason"`
	Source  string `json:"source"`
}
