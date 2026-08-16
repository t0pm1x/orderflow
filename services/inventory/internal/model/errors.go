package model

import "errors"

// ErrInsufficientStock means we don't have enough available to reserve.
var ErrInsufficientStock = errors.New("model: insufficient stock")

// ErrStaleVersion is returned by lock.Upsert when the version changed
// between read and write (concurrent modification).
var ErrStaleVersion = errors.New("model: stale version (concurrent update)")
