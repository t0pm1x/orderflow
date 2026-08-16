// Package webhook exposes the POST /v1/payments/webhook endpoint.
// Validates HMAC signature from the (mock) provider, then enqueues
// a PaymentCompleted or PaymentFailed event via the outbox.
package webhook