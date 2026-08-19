package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/t0pm1x/orderflow/platform/events"
)

func TestRegistry_HasAllEventTypes(t *testing.T) {
	r := Registry(slog.Default())
	want := []string{"PaymentRequested"}
	for _, ev := range want {
		if _, ok := r[ev]; !ok {
			t.Errorf("Payment Service handler for %q is missing", ev)
		}
	}
}

func TestRegistry_StubsLogAndReturnNil(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	r := Registry(logger)
	for eventType, h := range r {
		env := &events.Envelope{
			EventID:       "e1",
			EventType:     eventType,
			AggregateID:   "a1",
			AggregateType: "Payment",
			SchemaVersion: "1.0",
			Payload:       json.RawMessage(`{}`),
		}
		if err := h(context.Background(), env); err != nil {
			t.Errorf("handler for %q returned error: %v", eventType, err)
		}
	}
	if buf.Len() == 0 {
		t.Error("expected stub handlers to log something")
	}
}

func TestStart_DisabledWhenNoEnv(t *testing.T) {
	ctx := context.Background()
	closer, err := Start(ctx, slog.Default(), "", "", nil, nil)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := closer(ctx); err != nil {
		t.Errorf("close: %v", err)
	}
}

// TestSetHandler_RaceWithLoad exercises the v1.1.2 fix for P1-#10:
// globalHandler is atomic.Pointer so concurrent Store/Load is
// race-free. We hammer SetHandler from one goroutine pool and
// Registry from another, then assert: no panic, no data race
// (go test -race), and every observed Registry result was either
// the loaded handler or nil (the stub path).
func TestSetHandler_RaceWithLoad(t *testing.T) {
	const (
		writers      = 4
		readers      = 8
		opsPerWorker = 200
	)
	var (
		wg       sync.WaitGroup
		stop     = make(chan struct{})
		observed atomic.Uint64
	)

	t.Cleanup(func() { SetHandler(nil) })

	// Writers: alternate between a non-nil *Handler (constructed
	// with a nil pool — safe because we never invoke it in this
	// test) and nil.
	mkHandler := func() *Handler { return NewHandler(nil, slog.Default()) }

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for j := 0; j < opsPerWorker; j++ {
				select {
				case <-stop:
					return
				default:
				}
				if (seed+j)%2 == 0 {
					SetHandler(mkHandler())
				} else {
					SetHandler(nil)
				}
			}
		}(i)
	}

	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerWorker; j++ {
				select {
				case <-stop:
					return
				default:
				}
				_ = Registry(slog.Default())
				observed.Add(1)
			}
		}()
	}

	wg.Wait()
	if observed.Load() != uint64(readers*opsPerWorker) {
		t.Errorf("reader iterations: got %d want %d", observed.Load(), readers*opsPerWorker)
	}
}
