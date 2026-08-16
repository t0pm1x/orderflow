// Package lock provides optimistic locking primitives for Stock rows.
//
// The contract is simple: callers pass the currently observed version and
// the SQL `UPDATE ... WHERE version = $1` either succeeds (and bumps the
// version) or returns zero rows affected, which the caller must treat as
// a retryable conflict.
//
// Implemented in sub-stage 3.6.b.
package lock
