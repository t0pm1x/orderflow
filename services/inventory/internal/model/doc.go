// Package model contains the Stock aggregate and its persistence types.
//
// Stock is the canonical source of truth for available quantity. Each row
// carries a `version` column used for optimistic locking by the lock
// package: updates are issued as `UPDATE ... SET version = version + 1
// WHERE sku = $1 AND version = $2` so concurrent reservers cannot
// double-decrement.
//
// Implemented in sub-stage 3.6.b.
package model
