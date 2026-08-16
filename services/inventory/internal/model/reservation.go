package model

import (
	"time"
)

// Reservation is a Redis-stored TTL reservation token.
type Reservation struct {
	ReservationID string
	SKU           string
	Quantity      int
	OrderID       string
	ExpiresAt     time.Time
}

// Expired reports whether the reservation TTL has elapsed.
func (r *Reservation) Expired() bool {
	return time.Now().After(r.ExpiresAt)
}
