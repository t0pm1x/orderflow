// Package types contains shared types used across orderflow services.
package types

import (
	"fmt"
	"math"
)

// Money is an integer count of cents (avoids float precision issues).
type Money int64

// NewMoneyFromCents creates Money from a cent count.
func NewMoneyFromCents(cents int64) Money { return Money(cents) }

// NewMoneyFromMajor creates Money from major units (e.g., dollars).
// Uses bankers' rounding (math.Round rounds half away from zero,
// which matches the spec'd test cases — 19.99 → 1999 cents).
func NewMoneyFromMajor(major float64) Money {
	return Money(math.Round(major * 100))
}

// Cents returns the money as int64 cents.
func (m Money) Cents() int64 { return int64(m) }

// String formats as "$X.YY".
func (m Money) String() string {
	cents := int64(m)
	major := cents / 100
	frac := cents % 100
	if frac < 0 {
		frac = -frac
	}
	return fmt.Sprintf("$%d.%02d", major, frac)
}
