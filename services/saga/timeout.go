package saga

import (
	"context"
	"sync"
	"time"
)

// Watchdog expires sagas that have been in a non-terminal state
// longer than Timeout. The compensation actions then run (see
// compensate.go).
//
// The watchdog is intentionally in-process (no DB dependency) for
// now — the per-saga TTL row in services/saga/migrations is the
// source of truth across restarts (sub-stage 3.9.d follow-up).
type Watchdog struct {
	Timeout time.Duration

	mu      sync.Mutex
	sagas  map[string]time.Time // orderID → deadline
	stopped chan struct{}
}

// NewWatchdog constructs a Watchdog with the given timeout.
func NewWatchdog(timeout time.Duration) *Watchdog {
	return &Watchdog{
		Timeout: timeout,
		sagas:   map[string]time.Time{},
		stopped: make(chan struct{}),
	}
}

// Register adds orderID to the watch list with a deadline of
// now+Timeout. If the saga is already registered, the deadline is
// reset (e.g. when an event extends the saga's effective lifetime).
func (w *Watchdog) Register(orderID string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.sagas[orderID] = time.Now().Add(w.Timeout)
	w.reschedule()
}

// Deregister removes orderID from the watch list (used when the
// saga reaches a terminal state).
func (w *Watchdog) Deregister(orderID string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.sagas, orderID)
	w.reschedule()
}

// Stop signals Run to exit.
func (w *Watchdog) Stop() {
	close(w.stopped)
}

// Run loops until Stop is called or ctx is cancelled, firing
// callbacks for expired sagas.
func (w *Watchdog) Run(ctx context.Context, onExpire func(orderID string)) {
	for {
		w.mu.Lock()
		if len(w.sagas) == 0 {
			w.mu.Unlock()
			select {
			case <-ctx.Done():
				return
			case <-w.stopped:
				return
			case <-w.after(0): // wake up periodically
				continue
			}
		}
		// Find the earliest deadline.
		var nextDeadline time.Time
		for _, t := range w.sagas {
			if nextDeadline.IsZero() || t.Before(nextDeadline) {
				nextDeadline = t
			}
		}
		w.mu.Unlock()

		wait := time.Until(nextDeadline)
		if wait < 0 {
			wait = 0
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-w.stopped:
			timer.Stop()
			return
		case <-timer.C:
		}

		// Reap expired sagas.
		w.mu.Lock()
		now := time.Now()
		var expired []string
		for id, t := range w.sagas {
			if !t.After(now) {
				expired = append(expired, id)
			}
		}
		for _, id := range expired {
			delete(w.sagas, id)
		}
		w.mu.Unlock()
		for _, id := range expired {
			onExpire(id)
		}
	}
}

// reschedule wakes up Run's loop so it can recompute the next
// deadline after a Register/Deregister. Called with w.mu held.
func (w *Watchdog) reschedule() {
	// Trick: send a no-op signal via after(0).
	w.after(0)
}

// after returns a channel that fires after d. Centralized so
// reschedule() can poke the loop without a separate timer.
func (w *Watchdog) after(d time.Duration) <-chan time.Time {
	if d <= 0 {
		ch := make(chan time.Time, 1)
		ch <- time.Now()
		return ch
	}
	return time.After(d)
}
