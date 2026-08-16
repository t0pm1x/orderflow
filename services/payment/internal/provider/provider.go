// Package provider implements a mock payment provider for tests.
// Card behavior is deterministic based on the last 4 digits:
//
//	...0000 → success
//	...0001 → declined
//	...0002 → insufficient funds
//	...0003 → timeout (returns error after delay)
//	anything else → success
package provider

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Result is the outcome of a charge attempt.
type Result struct {
	PaymentID string
	Status    string // "succeeded" | "failed"
	ErrorCode string // empty on success
}

// ErrProviderTimeout is returned when the mock simulates a timeout.
var ErrProviderTimeout = errors.New("provider: timeout")

// Charge attempts to charge `amountCents` to the card identified by
// lastFour. Returns Result (with Status="failed" + ErrorCode on soft
// declines) or an error (network/timeout).
func Charge(ctx context.Context, paymentID string, amountCents int64, lastFour string) (*Result, error) {
	if len(lastFour) < 4 {
		return nil, fmt.Errorf("provider: invalid card number (need last 4)")
	}
	suffix := lastFour[len(lastFour)-4:]

	switch suffix {
	case "0001":
		return &Result{PaymentID: paymentID, Status: "failed", ErrorCode: "card_declined"}, nil
	case "0002":
		return &Result{PaymentID: paymentID, Status: "failed", ErrorCode: "insufficient_funds"}, nil
	case "0003":
		// Simulate timeout
		select {
		case <-time.After(30 * time.Second):
			return nil, ErrProviderTimeout
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	default:
		return &Result{PaymentID: paymentID, Status: "succeeded"}, nil
	}
}

// Refund reverses a charge. Always succeeds in the mock.
func Refund(ctx context.Context, paymentID string, amountCents int64) (*Result, error) {
	return &Result{PaymentID: paymentID, Status: "succeeded"}, nil
}
