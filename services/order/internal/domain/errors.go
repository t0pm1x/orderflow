package domain

import "fmt"

// InvalidTransitionError is returned when an order tries to make an
// illegal state transition.
type InvalidTransitionError struct {
	From OrderState
	To   OrderState
}

func (e *InvalidTransitionError) Error() string {
	return fmt.Sprintf("invalid transition: %s → %s", e.From, e.To)
}