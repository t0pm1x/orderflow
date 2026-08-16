// Package consumer handles Kafka events relevant to the Order Service:
//   - StockReserved    → state transition pending → reserved
//   - PaymentCompleted → state transition reserved → confirmed
//   - StockReservationFailed / PaymentFailed → state transition → cancelled
//
// Implemented in sub-stage 3.8.d.
package consumer
