package model

import (
	"time"
)

type Stock struct {
	SKU       string    `json:"sku"`
	Available int       `json:"available"`
	Reserved  int       `json:"reserved"`
	Version   int64     `json:"version"` // optimistic-lock token
	UpdatedAt time.Time `json:"updated_at"`
}

// CanReserve returns true if we have enough available stock.
func (s *Stock) CanReserve(qty int) bool {
	return s.Available >= qty
}

// Reserve moves qty from Available to Reserved and bumps Version, or
// returns ErrInsufficientStock.
// Caller must atomically UPDATE WHERE version = s.Version in the same tx.
func (s *Stock) Reserve(qty int) error {
	if !s.CanReserve(qty) {
		return ErrInsufficientStock
	}
	s.Available -= qty
	s.Reserved += qty
	s.Version++
	s.UpdatedAt = time.Now().UTC()
	return nil
}

// Release returns qty back to available pool (for cancellations).
func (s *Stock) Release(qty int) {
	s.Available += qty
	if s.Reserved >= qty {
		s.Reserved -= qty
	}
	s.Version++
	s.UpdatedAt = time.Now().UTC()
}
