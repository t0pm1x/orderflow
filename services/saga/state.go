// Package saga is the saga orchestrator service. It listens for
// order-events (OrderCreated, PaymentCompleted, PaymentFailed,
// StockReserved, StockReleased, StockReservationFailed) and runs
// the per-order state machine:
//
//	initiated
//	  ↓ StockReserved
//	stock_reserved
//	  ↓ PaymentRequested → PaymentCompleted
//	payment_completed
//	  ↓ (terminal)
//	completed
//
//	OR, on any failure:
//	  → compensated (terminal)
//
// Sub-stages:
//
//	3.9.a state machine (this file)
//	3.9.b compensation actions (compensate.go)
//	3.9.c timeout watchdog (timeout.go)
package saga

import (
	"errors"
	"fmt"
)

// State is the saga's per-order state.
type State string

// State is the per-order saga state. The string values are the
// wire form written to order_sagas.state; the names below are the
// transitions the handlers in services/saga/internal/consumer
// drive. See transitionTable for the allowed (state, event) pairs.
//
// StatePaymentPending and StatePaymentComplete were reserved for
// a future in-flight payment sub-state but the actual flow
// (initiated → stock_reserved → completed) skips them — the saga
// only emits PaymentRequested once and waits for the result.
const (
	StateInitiated     State = "initiated"
	StateStockReserved State = "stock_reserved"
	StateCompleted     State = "completed"
	StateCompensated   State = "compensated"
)

// IsTerminal reports whether the state is final (no further
// transitions allowed).
func (s State) IsTerminal() bool {
	return s == StateCompleted || s == StateCompensated
}

// transitionTable maps current state → event → next state.
// Any (state, event) pair not in the table returns ErrInvalidTransition.
var transitionTable = map[State]map[string]State{
	StateInitiated: {
		"StockReserved":          StateStockReserved,
		"StockReservationFailed": StateCompensated,
	},
	StateStockReserved: {
		"StockReleased":    StateInitiated, // back to start (rare)
		"PaymentCompleted": StateCompleted, // happy path
		"PaymentFailed":    StateCompensated,
	},
}

// ErrInvalidTransition is returned by Handle when the (state,
// event) pair is not allowed.
var ErrInvalidTransition = errors.New("saga: invalid transition")

// Saga is one order's in-memory state. Construct via New(id).
type Saga struct {
	OrderID string
	State   State
}

// New creates a Saga in StateInitiated.
func New(orderID string) *Saga {
	return &Saga{OrderID: orderID, State: StateInitiated}
}

// Handle applies an event. Returns the new state on success or
// ErrInvalidTransition if the (state, event) pair is not allowed.
func (s *Saga) Handle(eventType string) (State, error) {
	if s.State.IsTerminal() {
		return s.State, fmt.Errorf("%w: %s from terminal %s", ErrInvalidTransition, eventType, s.State)
	}
	transitions, ok := transitionTable[s.State]
	if !ok {
		return s.State, fmt.Errorf("%w: no transitions for %s", ErrInvalidTransition, s.State)
	}
	next, ok := transitions[eventType]
	if !ok {
		return s.State, fmt.Errorf("%w: %s → %s not allowed", ErrInvalidTransition, s.State, eventType)
	}
	s.State = next
	return s.State, nil
}

// CanTransition reports whether the (state, event) pair would be
// accepted by Handle without mutating the saga.
func (s *Saga) CanTransition(eventType string) bool {
	if s.State.IsTerminal() {
		return false
	}
	transitions, ok := transitionTable[s.State]
	if !ok {
		return false
	}
	_, ok = transitions[eventType]
	return ok
}
