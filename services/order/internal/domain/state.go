package domain

// OrderState represents the lifecycle state of an Order.
type OrderState string

const (
	// StatePending is the initial order state after creation, awaiting saga start.
	StatePending OrderState = "pending"
	// StateReserved means stock has been reserved and the order is awaiting payment.
	StateReserved OrderState = "reserved"
	// StateConfirmed means payment completed and the order is fulfilled.
	StateConfirmed OrderState = "confirmed"
	// StateCancelled means stock or payment failed, or the user cancelled the order.
	StateCancelled OrderState = "cancelled"
	// StateFailed means the saga timed out before any terminal state was reached.
	StateFailed OrderState = "failed"
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
