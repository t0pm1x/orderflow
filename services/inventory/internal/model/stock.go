package model

import (
	"time"
)

// Stock is the inventory aggregate for a single SKU. Available and
// Reserved are kept in lockstep: Available counts units that can be
// reserved, Reserved counts units committed to a saga that haven't
// been released yet. Version is the optimistic-lock token that
// callers must echo in their UPDATE WHERE clause.
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
