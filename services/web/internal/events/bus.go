// Package events hosts an in-process publish/subscribe bus used by
// the SSE endpoint to relay Kafka events to connected browsers.
package events

import (
	"sync"

	pkgEvents "github.com/t0pm1x/orderflow/platform/events"
)

// BusEvent is the value type passed through the bus. The Envelope is
// re-used from pkg/platform/events so consumers can unmarshal the
// exact Kafka record body without translating types.
type BusEvent struct {
	Envelope pkgEvents.Envelope
}

// Bus fans events out to subscribers. Slow consumers drop oldest
// first, never blocking the publisher.
type Bus struct {
	mu   sync.Mutex
	subs map[chan BusEvent]struct{}
	done chan struct{}
}

// NewBus constructs a fresh bus.
func NewBus() *Bus {
	return &Bus{subs: map[chan BusEvent]struct{}{}, done: make(chan struct{})}
}

// Subscribe returns a buffered channel that receives every event
// from now on, plus an unsubscribe function.
func (b *Bus) Subscribe() (chan BusEvent, func()) {
	ch := make(chan BusEvent, 64)
	b.mu.Lock()
	if b.closed() {
		b.mu.Unlock()
		close(ch)
		return ch, func() {}
	}
	b.subs[ch] = struct{}{}
	b.mu.Unlock()
	return ch, func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if _, ok := b.subs[ch]; ok {
			delete(b.subs, ch)
			close(ch)
		}
	}
}

// Publish sends e to every current subscriber. If a subscriber's
// buffer is full, the OLDEST queued event on that channel is
// dropped to make room (subscriber is too slow; keep them current).
func (b *Bus) Publish(e BusEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed() {
		return
	}
	for ch := range b.subs {
		select {
		case ch <- e:
		default:
			// Drop oldest, push newest. Non-blocking.
			select {
			case <-ch:
			default:
			}
			select {
			case ch <- e:
			default:
			}
		}
	}
}

// Close marks the bus as closed. Subsequent Publish is a no-op;
// all subscriber channels are closed.
func (b *Bus) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	select {
	case <-b.done:
		return
	default:
		close(b.done)
	}
	for ch := range b.subs {
		close(ch)
		delete(b.subs, ch)
	}
}

func (b *Bus) closed() bool {
	select {
	case <-b.done:
		return true
	default:
		return false
	}
}