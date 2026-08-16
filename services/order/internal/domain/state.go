package domain

// OrderState represents the lifecycle state of an Order.
type OrderState string

const (
	StatePending   OrderState = "pending"   // created, awaiting saga
	StateReserved  OrderState = "reserved"  // stock reserved, awaiting payment
	StateConfirmed OrderState = "confirmed" // payment completed
	StateCancelled OrderState = "cancelled" // stock or payment failed, OR user cancelled
	StateFailed    OrderState = "failed"    // saga timeout
)

// CanTransition returns true if `from → to` is a valid state transition.
func CanTransition(from, to OrderState) bool {
	transitions := map[OrderState][]OrderState{
		StatePending:   {StateReserved, StateCancelled, StateFailed},
		StateReserved:  {StateConfirmed, StateCancelled, StateFailed},
		StateConfirmed: {}, // terminal
		StateCancelled: {}, // terminal
		StateFailed:    {}, // terminal
	}
	for _, allowed := range transitions[from] {
		if allowed == to {
			return true
		}
	}
	return false
}