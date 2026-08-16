// Package outbox contains the transactional outbox writer and poller.
//
// The writer is called within the same DB tx as the Stock row update.
// The poller periodically publishes pending events to Kafka with EOS.
// Inventory publishes StockReserved, StockReservationFailed,
// StockReleased, StockConfirmed.
//
// Implemented in sub-stage 3.6.f (writer) and 3.7 (poller/publisher).
package outbox
