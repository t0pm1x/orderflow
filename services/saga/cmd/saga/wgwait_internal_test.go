package saga

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"
)

// TestWgWait_Behavior pins the contract that motivated the
// post-review fix: registered goroutines that exit must be awaited
// before wgWait returns, and a stuck goroutine must not prevent
// wgWait from returning once the supplied shutdown context expires.
// It also asserts nil-safety for the close fns, pool, and httpSrv
// parameters, which is required so the listen-error path (no
// httpSrv yet) and the disabled runtime (no consumer/outbox) can
// share the same helper. No DB or Kafka required.
func TestWgWait_Behavior(t *testing.T) {
	exited := make(chan struct{})
	var cleanWG sync.WaitGroup
	cleanWG.Add(1)
	go func() {
		defer cleanWG.Done()
		<-exited
	}()

	go func() {
		time.Sleep(20 * time.Millisecond)
		close(exited)
	}()

	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	wgWait(waitCtx, &cleanWG, nil, nil, nil, nil)

	select {
	case <-exited:
	default:
		t.Fatal("wgWait returned before the tracked goroutine exited")
	}

	var stuck sync.WaitGroup
	stuck.Add(1)
	deadline, deadlineCancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer deadlineCancel()
	started := time.Now()
	wgWait(deadline, &stuck, nil, nil, nil, &http.Server{})
	if elapsed := time.Since(started); elapsed < 20*time.Millisecond {
		t.Fatalf("wgWait returned before shutdown context expired: %s", elapsed)
	}
	stuck.Done()
}
