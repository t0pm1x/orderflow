// Package events hosts an in-process publish/subscribe bus used by
// the SSE endpoint to relay Kafka events to connected browsers.
// Task 10 replaces this stub with the full implementation.
package events

// BusEvent is the value type passed through the bus. The Envelope
// field is `any` in the stub; Task 10 narrows it to
// pkg/platform/events.Envelope.
type BusEvent struct {
	Envelope any
}

// Bus is a fan-out broadcast hub. This stub lets handlers compile
// before Task 10 lands. Replace with full Subscribe/Publish/Close.
type Bus struct{}

// NewBus returns a fresh stub bus.
func NewBus() *Bus { return &Bus{} }
