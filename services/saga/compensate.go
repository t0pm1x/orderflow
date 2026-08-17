package saga

import (
	"context"
	"errors"
	"fmt"
)

// Compensator is the contract for running one saga compensation
// step. Each Compensator is idempotent: if called twice with the
// same arguments, the second call is a no-op (returns nil).
type Compensator func(ctx context.Context, s *Saga) error

// ErrAlreadyCompensated is returned by a Compensator when it
// detects a duplicate call (e.g. the saga row is already in
// StateCompensated).
var ErrAlreadyCompensated = errors.New("saga: already compensated")

// Compensate runs every Compensator registered for the saga and
// returns the first non-nil error (subsequent compensators are
// still attempted so a partial failure doesn't leave the saga in
// an inconsistent state).
func Compensate(ctx context.Context, s *Saga, compensators []Compensator) error {
	if s.State == StateCompensated {
		return ErrAlreadyCompensated
	}
	var firstErr error
	for _, c := range compensators {
		if err := c(ctx, s); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	s.State = StateCompensated
	return firstErr
}

// ReleaseStockCompensator returns a Compensator that calls release
// on the saga's reserved stock. The actual call is delegated to
// the supplied function so this package doesn't depend on
// services/inventory directly (which would create a cycle).
func ReleaseStockCompensator(release func(ctx context.Context, orderID string) error) Compensator {
	return func(ctx context.Context, s *Saga) error {
		if release == nil {
			return fmt.Errorf("saga: nil release func for %s", s.OrderID)
		}
		return release(ctx, s.OrderID)
	}
}

// RefundPaymentCompensator returns a Compensator that refunds the
// saga's payment. Like ReleaseStock, the actual call is delegated
// to the supplied function.
func RefundPaymentCompensator(refund func(ctx context.Context, orderID string) error) Compensator {
	return func(ctx context.Context, s *Saga) error {
		if refund == nil {
			return fmt.Errorf("saga: nil refund func for %s", s.OrderID)
		}
		return refund(ctx, s.OrderID)
	}
}
