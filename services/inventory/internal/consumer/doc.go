// Package consumer handles Kafka events relevant to the Inventory Service:
//
//	- OrderCancelled     → release any pending reservation
//	- PaymentFailed      → release any pending reservation
//	- OrderConfirmed     → confirm reservation (decrement Stock row permanently)
//
// Implemented in sub-stage 3.6.e.
package consumer
