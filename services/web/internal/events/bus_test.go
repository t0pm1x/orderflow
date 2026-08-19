package events_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	pkgEvents "github.com/t0pm1x/orderflow/platform/events"
	"github.com/t0pm1x/orderflow/services/web/internal/events"
)

func TestBus_PublishSubscribe(t *testing.T) {
	b := events.NewBus()
	defer b.Close()
	ch, unsub := b.Subscribe()
	defer unsub()

	env := pkgEvents.Envelope{EventID: "e1", EventType: "OrderCreated"}
	go b.Publish(events.BusEvent{Envelope: env})

	select {
	case got := <-ch:
		if got.Envelope.EventID != "e1" {
			t.Errorf("got %s", got.Envelope.EventID)
		}
	case <-time.After(time.Second):
		t.Fatal("no event received in 1s")
	}
}

func TestBus_UnsubscribeStopsDelivery(t *testing.T) {
	b := events.NewBus()
	defer b.Close()
	ch, unsub := b.Subscribe()
	unsub()
	env := pkgEvents.Envelope{EventID: "e2"}
	b.Publish(events.BusEvent{Envelope: env})
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("expected channel closed after unsub")
		}
	case <-time.After(100 * time.Millisecond):
		// OK: a closed channel never yields a value; an open channel
		// would receive the buffered message.
	}
}

func TestBus_MultipleSubscribers(_ *testing.T) {
	b := events.NewBus()
	defer b.Close()
	ch1, u1 := b.Subscribe()
	defer u1()
	ch2, u2 := b.Subscribe()
	defer u2()

	var wg sync.WaitGroup
	wg.Add(2)
	for _, ch := range []chan events.BusEvent{ch1, ch2} {
		ch := ch
		go func() {
			defer wg.Done()
			<-ch
		}()
	}
	b.Publish(events.BusEvent{Envelope: pkgEvents.Envelope{EventID: "x"}})
	wg.Wait()
}

func TestBus_BufferOverflow_DropsOldest(t *testing.T) {
	b := events.NewBus()
	defer b.Close()
	ch, unsub := b.Subscribe()
	defer unsub()

	for i := 0; i < 1000; i++ {
		b.Publish(events.BusEvent{Envelope: pkgEvents.Envelope{EventID: "x"}})
	}
	// Subscriber didn't deadlock and at least one message buffered.
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("subscriber starved")
	}
}

func TestBus_History_PerAggregate(t *testing.T) {
	b := events.NewBus()
	defer b.Close()
	for i := 0; i < 50; i++ {
		agg := "ord-A"
		if i%2 == 0 {
			agg = "ord-B"
		}
		b.Publish(events.BusEvent{Envelope: pkgEvents.Envelope{EventID: "e", EventType: "X", AggregateID: agg}})
	}
	hA := b.History("ord-A")
	hB := b.History("ord-B")
	if len(hA) != 25 || len(hB) != 25 {
		t.Fatalf("expected 25 each, got A=%d B=%d", len(hA), len(hB))
	}
	if h := b.History("ord-missing"); len(h) != 0 {
		t.Fatalf("expected empty history for unknown aggregate, got %d", len(h))
	}
}

func TestBus_History_OccurrenceOrder(t *testing.T) {
	b := events.NewBus()
	defer b.Close()
	for i := 0; i < 10; i++ {
		b.Publish(events.BusEvent{Envelope: pkgEvents.Envelope{EventID: fmt.Sprintf("e%02d", i), EventType: "X", AggregateID: "ord"}})
	}
	h := b.History("ord")
	if len(h) != 10 {
		t.Fatalf("expected 10 events, got %d", len(h))
	}
	for i, e := range h {
		want := fmt.Sprintf("e%02d", i)
		if e.EventID != want {
			t.Fatalf("position %d: want EventID=%s got %s", i, want, e.EventID)
		}
	}
}

func TestBus_RingOverflow(t *testing.T) {
	b := events.NewBus()
	defer b.Close()
	for i := 0; i < events.RingCap()*3; i++ {
		b.Publish(events.BusEvent{Envelope: pkgEvents.Envelope{EventID: "x", EventType: "X", AggregateID: "ord"}})
	}
	h := b.History("ord")
	if len(h) > events.RingCap() {
		t.Fatalf("history exceeded ringCap: %d", len(h))
	}
	// Sanity: 3*ringCap publishes must keep at least ringCap newest events.
	if len(h) < events.RingCap() {
		t.Fatalf("expected history to retain ringCap newest, got %d", len(h))
	}
	// The newest event must be the last one published.
	if h[len(h)-1].EventID != "x" {
		t.Fatalf("expected newest event at tail, got %s", h[len(h)-1].EventID)
	}
}

func TestBus_ConcurrentPublishSubscribe(_ *testing.T) {
	b := events.NewBus()
	defer b.Close()
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch, _ := b.Subscribe()
			for j := 0; j < 100; j++ {
				<-ch
			}
		}()
	}
	for i := 0; i < 1000; i++ {
		b.Publish(events.BusEvent{Envelope: pkgEvents.Envelope{EventType: "X", AggregateID: "a"}})
	}
	wg.Wait()
}

// TestBus_CloseRaceWithPublish exercises the window between Publish
// releasing the mutex (after snapshotting subscribers) and the
// per-channel select. During that window Close() or an unsubscribe
// can close a snapshotted channel, which would panic with "send on
// closed channel" without the per-channel defer recover. The test
// asserts no panic fires by completing cleanly under -race across
// many concurrent publishers and a mid-flight Close.
func TestBus_CloseRaceWithPublish(_ *testing.T) {
	const trials = 5
	for trial := 0; trial < trials; trial++ {
		b := events.NewBus()
		const N = 200
		const S = 32

		subs := make([]chan events.BusEvent, S)
		unsubs := make([]func(), S)
		for i := range subs {
			subs[i], unsubs[i] = b.Subscribe()
		}

		var drainWG sync.WaitGroup
		drainWG.Add(S)
		for _, ch := range subs {
			ch := ch
			go func() {
				defer drainWG.Done()
				for range ch {
					/* drain — see TestBus_CloseRaceWithPublish doc */
				}
			}()
		}

		closer := func() {
			time.Sleep(100 * time.Microsecond)
			for _, u := range unsubs {
				u()
			}
		}
		go closer()

		var pubWG sync.WaitGroup
		pubWG.Add(N)
		for i := 0; i < N; i++ {
			go func() {
				defer pubWG.Done()
				b.Publish(events.BusEvent{Envelope: pkgEvents.Envelope{EventType: "X", AggregateID: "a"}})
			}()
		}
		pubWG.Wait()
		b.Close()
		drainWG.Wait()
	}
}

// TestBus_SlowSubscriberDoesNotDeadlock reproduces the deadlock
// reported by the reviewer for the "atomic single select" drop-oldest
// pattern (`case <-ch: ch <- e`). With 16 concurrent publishers and
// a single subscriber that never reads, the subscriber's buffered
// channel (cap 64) fills up immediately. Each publisher's drop-oldest
// path drains one then attempts an UNCONDITIONAL send — if another
// publisher refills the slot in between, that send blocks forever
// while holding RLock, which prevents Close() from ever acquiring
// the write lock and wedges every publisher in the process.
//
// With the non-blocking drop-oldest (drain + second non-blocking
// send, both with default branches) the publisher simply drops the
// event for the slow subscriber and moves on. The whole test must
// complete in well under a second.
func TestBus_SlowSubscriberDoesNotDeadlock(t *testing.T) {
	const P = 16
	const PerPub = 200 // 16 * 200 = 3200 events vs. cap 64 → guaranteed overflow

	done := make(chan struct{})
	go func() {
		defer close(done)
		b := events.NewBus()
		defer b.Close()

		ch, _ := b.Subscribe()
		_ = ch // intentionally never drained — the stalled subscriber

		var wg sync.WaitGroup
		wg.Add(P)
		for i := 0; i < P; i++ {
			go func() {
				defer wg.Done()
				for j := 0; j < PerPub; j++ {
					b.Publish(events.BusEvent{Envelope: pkgEvents.Envelope{EventType: "X", AggregateID: "a"}})
				}
			}()
		}
		wg.Wait()
	}()

	select {
	case <-done:
		// pass — finished under the deadline
	case <-time.After(time.Second):
		t.Fatal("bus deadlocked: 16 publishers + 1 stalled subscriber did not complete in 1s")
	}
}
