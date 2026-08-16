// Package idempotency provides Redis-backed dedupe of webhook events
// by Idempotency-Key header. Returns the same response for duplicate keys.
package idempotency