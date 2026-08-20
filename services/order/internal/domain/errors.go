package domain

import (
	"errors"
	"fmt"
)

// InvalidTransitionError is returned when an order tries to make an
// illegal state transition.
type InvalidTransitionError struct {
	From OrderState
	To   OrderState
}

func (e *InvalidTransitionError) Error() string {
	return fmt.Sprintf("invalid transition: %s → %s", e.From, e.To)
}

// ErrOrderNotFound is the sentinel for "this order id doesn't
// exist" returned by Repository.Get / Cancel. The api handler maps
// it to HTTP 404. Lives in domain (not repository) so both the
// repository (lower-level) and the api (higher-level) can reference
// it without an import cycle.
var ErrOrderNotFound = errors.New("order not found")

// ErrOrderAlreadyTerminal is the sentinel for "the order exists
// but is in a terminal state (confirmed/cancelled/failed)" returned
// by Repository.Cancel. The api handler maps it to HTTP 409
// (audit WEB-3-CANCEL-409 fix) so the web UI can render a distinct
// "already in this state" message instead of folding it into a
// silent 404 "Not found".
var ErrOrderAlreadyTerminal = errors.New("order already in terminal state")
