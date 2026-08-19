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
//
// LastFour is the client-supplied payment hint copied from the
// originating OrderCreated event (and persisted on the saga row by
// the v1.1.5 migration). The payment service passes it to
// provider.Charge so the mock can pick a deterministic
// success/decline branch on cards ending in 0001. Empty when the
// submit body did not include a payment block; the payment handler
// falls back to deriving from orderID in that case so pre-v1.1.5
// clients keep their old behavior.
type PaymentRequestedPayload struct {
	OrderID        string `json:"order_id"`
	AmountCents    int64  `json:"amount_cents"`
	IdempotencyKey string `json:"idempotency_key"`
	LastFour       string `json:"last_four,omitempty"`
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
//
// SKU and Quantity were added in v1.1 — without them, the inventory
// service couldn't decrement the stock_items.reserved counter, and
// every cancelled order leaked a phantom reservation. Multi-item
// orders emit one StockReleaseRequested per item.
type StockReleaseRequestedPayload struct {
	OrderID       string `json:"order_id"`
	ReservationID string `json:"reservation_id"`
	SKU           string `json:"sku"`
	Quantity      int    `json:"quantity"`
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
