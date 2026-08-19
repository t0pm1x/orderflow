package domain

import (
	"time"

	"github.com/t0pm1x/orderflow/platform/types"
)

// OrderItem is a single line item on an Order: SKU, quantity, and
// unit price in cents. Total cost is derived from these fields when
// the order is constructed.
type OrderItem struct {
	SKU            string `json:"sku"`
	Quantity       int    `json:"quantity"`
	UnitPriceCents int64  `json:"unit_price_cents"`
}

// Order is the Order aggregate root. Its lifecycle is driven by
// the saga via the events consumed in services/order/internal/consumer.
type Order struct {
	ID         types.OrderID    `json:"id"`
	CustomerID types.CustomerID `json:"customer_id"`
	Items      []OrderItem      `json:"items"`
	State      OrderState       `json:"state"`
	TotalCents types.Money      `json:"total_cents"`
	// LastFour is the last four digits of the payment card the
	// client submitted at submit time. The order service treats it
	// as opaque — it's threaded into the OrderCreated event so the
	// saga can include it on the downstream PaymentRequested
	// payload, where the payment provider uses it to decide
	// success/decline (services/payment/internal/provider/provider.go).
	// Not persisted on the orders row: the value is only meaningful
	// until the saga emits OrderConfirmed/OrderCancelled; storage
	// belongs on the saga row (services/saga/migrations/0003_*.sql).
	LastFour    string     `json:"last_four,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
}

// NewOrder creates a new Order in StatePending.
func NewOrder(customerID types.CustomerID, items []OrderItem) *Order {
	var total int64
	for _, it := range items {
		total += int64(it.Quantity) * it.UnitPriceCents
	}
	now := time.Now().UTC()
	return &Order{
		ID:         types.NewOrderID(),
		CustomerID: customerID,
		Items:      items,
		State:      StatePending,
		TotalCents: types.NewMoneyFromCents(total),
		CreatedAt:  now,
		UpdatedAt:  now,
	}
}

// Transition changes the order state. Returns error if invalid.
func (o *Order) Transition(to OrderState) error {
	if !CanTransition(o.State, to) {
		return &InvalidTransitionError{From: o.State, To: to}
	}
	o.State = to
	o.UpdatedAt = time.Now().UTC()
	if to == StateConfirmed || to == StateCancelled || to == StateFailed {
		t := o.UpdatedAt
		o.CompletedAt = &t
	}
	return nil
}
