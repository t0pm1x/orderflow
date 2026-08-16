// Package consumer handles PaymentRequested events: looks up payment,
// calls mock provider, stores result, emits event via outbox.
package consumer