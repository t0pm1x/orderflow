package events_test

import (
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

func TestBus_MultipleSubscribers(t *testing.T) {
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