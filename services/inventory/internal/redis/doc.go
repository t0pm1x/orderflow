// Package redis wraps the Redis-backed reservation store.
//
// Reservations are written with a TTL (typically 5 minutes — the order
// saga wait window for payment) so they self-expire if a saga stalls.
// The store also provides idempotent `release` / `confirm` operations
// keyed by reservation id.
//
// Implemented in sub-stage 3.6.c.
package redis
