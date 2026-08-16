package domain

import (
	"time"

	"github.com/t0pm1x/orderflow/platform/types"
)

type OrderItem struct {
	SKU            string `json:"sku"`
	Quantity       int    `json:"quantity"`
	UnitPriceCents int64  `json:"unit_price_cents"`
}

type Order struct {
	ID          types.OrderID    `json:"id"`
	CustomerID  types.CustomerID `json:"customer_id"`
	Items       []OrderItem      `json:"items"`
	State       OrderState       `json:"state"`
	TotalCents  types.Money      `json:"total_cents"`
	CreatedAt   time.Time        `json:"created_at"`
	UpdatedAt   time.Time        `json:"updated_at"`
	CompletedAt *time.Time       `json:"completed_at,omitempty"`
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
