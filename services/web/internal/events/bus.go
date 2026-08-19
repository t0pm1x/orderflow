// Package events hosts an in-process publish/subscribe bus used by
// the SSE endpoint to relay Kafka events to connected browsers.
package events

import (
	"sync"

	pkgEvents "github.com/t0pm1x/orderflow/platform/events"
)

// ringCap is the upper bound on the per-process event ring buffer.
// When the ring is full, the oldest 10% are dropped in one slice
// trim to amortize the cost.
const ringCap = 200

// RingCap reports the maximum number of events the ring buffer
// retains. Exported so external tests can assert overflow behavior
// without copying the magic number.
func RingCap() int { return ringCap }

// ringEntry is a single (aggregate, envelope) pair held in the
// bounded ring. The aggregate ID is stored alongside the envelope
// so History can filter without re-marshalling.
type ringEntry struct {
	aggregateID string
	env         pkgEvents.Envelope
}

// BusEvent is the value type passed through the bus. The Envelope is
// re-used from pkg/platform/events so consumers can unmarshal the
// exact Kafka record body without translating types.
type BusEvent struct {
	Envelope pkgEvents.Envelope
}

// Bus fans events out to subscribers. Slow consumers drop oldest
// first, never blocking the publisher. A bounded ring buffer mirrors
// the most recent events so late-arriving HTTP clients can fetch
// historical events per-aggregate via History.
//
// The drop-oldest path is non-blocking on every channel send: if the
// per-subscriber buffer is still full after draining one, the new
// event is dropped for that subscriber rather than blocking the
// publisher. This is the only safe behavior — a blocking send here
// would wedge every publisher while waiting on the slowest consumer
// and prevent Close() from acquiring the write lock.
type Bus struct {
	mu   sync.RWMutex
	subs map[chan BusEvent]struct{}
	ring []ringEntry
	done chan struct{}
}

// NewBus constructs a fresh bus.
func NewBus() *Bus {
	return &Bus{subs: map[chan BusEvent]struct{}{}, ring: make([]ringEntry, 0, ringCap), done: make(chan struct{})}
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
// The event is also appended to the bounded ring buffer so it can
// be retrieved via History; when the ring overflows, the oldest
// 10% entries are dropped in one slice trim.
//
// Concurrency: subscribers are snapshotted under the write lock so
// the fan-out runs lock-free from other publishers. Each per-channel
// send holds a read lock — Close() / unsubscribe() block on the
// write lock until all in-flight RLocks release, so a snapshotted
// channel cannot be closed concurrently with the send (Unlock(Write)
// synchronizes-with the next RLock). The per-channel defer recover
// is therefore load-bearing in one specific case: Close() / an
// unsub() can acquire the write lock AFTER the publisher snapshots
// the channel but BEFORE the publisher's RLock — in that window
// `close(ch)` runs without any RLock synchronizing-with the pending
// send, and recover is the only thing preventing a "send on closed
// channel" panic from crashing the process. The send itself is
// non-blocking throughout: drop-oldest uses a drain (`<-ch`) plus a
// second non-blocking send, and both sends have `default` branches.
// A blocking send anywhere in this path would deadlock Close()
// (which waits on the write lock) once any single subscriber's
// buffer stayed full.
func (b *Bus) Publish(e BusEvent) {
	b.mu.Lock()
	if b.closed() {
		b.mu.Unlock()
		return
	}
	snapshot := make([]chan BusEvent, 0, len(b.subs))
	for ch := range b.subs {
		snapshot = append(snapshot, ch)
	}
	b.ring = append(b.ring, ringEntry{aggregateID: e.Envelope.AggregateID, env: e.Envelope})
	if len(b.ring) > ringCap {
		drop := ringCap / 10
		b.ring = b.ring[drop:]
	}
	b.mu.Unlock()

	for _, ch := range snapshot {
		ch := ch
		func() {
			defer func() {
				if r := recover(); r != nil {
					_ = r // channel closed mid-fan-out — drop this event silently
				}
			}()
			b.mu.RLock()
			defer b.mu.RUnlock()
			select {
			case ch <- e:
				return
			default:
			}
			select {
			case <-ch:
			default:
			}
			select {
			case ch <- e:
			default:
				// give up — drop this event for this slow subscriber
			}
		}()
	}
}

// History returns the most recent events for aggregateID from the
// bounded ring buffer, in occurrence order (oldest first). Returns
// an empty slice when the aggregate has no entries. The ring is
// trimmed on overflow so the returned slice is bounded by ringCap.
func (b *Bus) History(aggregateID string) []pkgEvents.Envelope {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]pkgEvents.Envelope, 0)
	for _, e := range b.ring {
		if e.aggregateID == aggregateID {
			out = append(out, e.env)
		}
	}
	return out
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
