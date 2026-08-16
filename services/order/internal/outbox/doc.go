// Package outbox contains the transactional outbox writer and poller.
//
// The writer is called within the same DB tx as the business state change.
// The poller periodically publishes pending events to Kafka with EOS.
//
// Implemented in sub-stage 3.4.d (writer) and 3.7 (poller/publisher).
package outbox
