package idempotency

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newMiniredisStore spins up an in-process Redis and returns a Store
// backed by it. Tests using this can run with `-short` (no external
// Redis required).
func newMiniredisStore(t *testing.T) (*Store, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() {
		_ = client.Close()
	})
	return NewStore(client), mr
}

// okHandler always returns 200 with body "ok".
func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
}

func TestMiddleware_MissingHeader(t *testing.T) {
	s, _ := newMiniredisStore(t)
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(""))
	rec := httptest.NewRecorder()

	Middleware(s)(okHandler()).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestMiddleware_FirstCall_CachesResponse(t *testing.T) {
	s, _ := newMiniredisStore(t)
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(""))
	req.Header.Set(HeaderIDKey, "key-A")
	rec := httptest.NewRecorder()

	Middleware(s)(okHandler()).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if rec.Body.String() != "ok" {
		t.Fatalf("expected body 'ok', got %q", rec.Body.String())
	}
	if rec.Header().Get(HeaderReplayed) != "" {
		t.Fatalf("first call must not have %s header", HeaderReplayed)
	}
}

func TestMiddleware_DuplicateReplaysCachedBody(t *testing.T) {
	s, _ := newMiniredisStore(t)
	mw := Middleware(s)

	// First call: cache "ok".
	req1 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(""))
	req1.Header.Set(HeaderIDKey, "key-dup")
	rec1 := httptest.NewRecorder()
	mw(okHandler()).ServeHTTP(rec1, req1)

	// Second call: should replay without invoking the handler.
	called := false
	spy := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusTeapot)
	})
	req2 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(""))
	req2.Header.Set(HeaderIDKey, "key-dup")
	rec2 := httptest.NewRecorder()
	mw(spy).ServeHTTP(rec2, req2)

	if called {
		t.Fatal("handler must not be called on replay")
	}
	if rec2.Code != http.StatusOK {
		t.Fatalf("replay expected 200, got %d", rec2.Code)
	}
	if rec2.Body.String() != "ok" {
		t.Fatalf("replay expected body 'ok', got %q", rec2.Body.String())
	}
	if rec2.Header().Get(HeaderReplayed) != "true" {
		t.Fatalf("replay must set %s=true", HeaderReplayed)
	}
}

func TestMiddleware_ReleaseOn5xx(t *testing.T) {
	s, _ := newMiniredisStore(t)
	mw := Middleware(s)

	failingHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("boom"))
	})

	// First call: handler fails → reservation released.
	req1 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(""))
	req1.Header.Set(HeaderIDKey, "key-5xx")
	rec1 := httptest.NewRecorder()
	mw(failingHandler).ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rec1.Code)
	}

	// Second call: should hit the handler again, not replay.
	called := false
	spy := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("recovered"))
	})
	req2 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(""))
	req2.Header.Set(HeaderIDKey, "key-5xx")
	rec2 := httptest.NewRecorder()
	mw(spy).ServeHTTP(rec2, req2)

	if !called {
		t.Fatal("handler should be called after release")
	}
	if rec2.Body.String() != "recovered" {
		t.Fatalf("expected 'recovered', got %q", rec2.Body.String())
	}
}

func TestMiddleware_4xxIsCached(t *testing.T) {
	s, _ := newMiniredisStore(t)
	mw := Middleware(s)

	clientErrHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"err":"bad card"}`))
	})

	req1 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(""))
	req1.Header.Set(HeaderIDKey, "key-400")
	rec1 := httptest.NewRecorder()
	mw(clientErrHandler).ServeHTTP(rec1, req1)

	req2 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(""))
	req2.Header.Set(HeaderIDKey, "key-400")
	rec2 := httptest.NewRecorder()
	mw(clientErrHandler).ServeHTTP(rec2, req2)

	// 4xx is a deterministic outcome → must be cached & replayed.
	if rec2.Code != http.StatusOK {
		t.Fatalf("replay expected 200, got %d", rec2.Code)
	}
	if rec2.Body.String() != `{"err":"bad card"}` {
		t.Fatalf("replay expected cached 4xx body, got %q", rec2.Body.String())
	}
}

// TestMiddleware_EmptyBodyReleases is the regression guard for
// P1-#7 from the v1.1.1 audit: a handler that returns without
// writing anything (e.g. panics that are recovered, or a
// request whose context is cancelled mid-flight) must NOT cache
// an empty body. Pre-fix, the middleware called Complete with
// the empty body, so the next retry got HTTP 200 with body="".
// The downstream consumer (the saga) saw the 200 and treated the
// request as completed without the handler ever having done
// any work.
func TestMiddleware_EmptyBodyReleases(t *testing.T) {
	s, _ := newMiniredisStore(t)
	mw := Middleware(s)

	noWriteHandler := http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		// Intentionally do nothing — simulate a panic recovered
		// upstream or a context-cancelled write.
	})

	req1 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(""))
	req1.Header.Set(HeaderIDKey, "key-empty")
	rec1 := httptest.NewRecorder()
	mw(noWriteHandler).ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("first call: expected 200 (default before any write), got %d", rec1.Code)
	}

	// Second call: must hit the handler (reservation was released),
	// NOT replay an empty body.
	called := false
	spy := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("real-handler"))
	})
	req2 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(""))
	req2.Header.Set(HeaderIDKey, "key-empty")
	rec2 := httptest.NewRecorder()
	mw(spy).ServeHTTP(rec2, req2)
	if !called {
		t.Fatal("handler should be called after empty-body release; got replay instead")
	}
	if rec2.Body.String() != "real-handler" {
		t.Errorf("body: got %q want %q", rec2.Body.String(), "real-handler")
	}
}

// TestMiddleware_HandlerPanicRecovers: a handler that panics
// must not crash the middleware. Pre-fix, the panic propagated
// out of next.ServeHTTP and crashed the goroutine. With the
// recover, the middleware releases the reservation so a retry
// can succeed.
func TestMiddleware_HandlerPanicRecovers(t *testing.T) {
	s, _ := newMiniredisStore(t)
	mw := Middleware(s)

	panicHandler := http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		panic("handler bug")
	})

	req1 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(""))
	req1.Header.Set(HeaderIDKey, "key-panic")
	rec1 := httptest.NewRecorder()
	// Must not panic out of this call.
	mw(panicHandler).ServeHTTP(rec1, req1)

	// Second call: handler must run (reservation was released).
	called := false
	spy := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("recovered"))
	})
	req2 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(""))
	req2.Header.Set(HeaderIDKey, "key-panic")
	rec2 := httptest.NewRecorder()
	mw(spy).ServeHTTP(rec2, req2)
	if !called {
		t.Fatal("handler should be called after panic-release")
	}
}

// TestMiddleware_ConcurrentSameKey_ExactlyOneHandlerCall verifies
// that N goroutines firing the same Idempotency-Key see exactly
// one handler invocation. The rest get 200 cached (replay) or
// 409 (concurrent in-flight). Pre-v1.1.1 (before ErrInFlight
// mapped to 409), duplicates saw 200 with body "in-flight",
// and the saga wrongly treated the no-op as a success.
func TestMiddleware_ConcurrentSameKey_ExactlyOneHandlerCall(t *testing.T) {
	const N = 32
	s, _ := newMiniredisStore(t)

	var calls atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		// Sleep to widen the race window: other goroutines should
		// observe ErrInFlight during this period.
		time.Sleep(50 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("first"))
	})

	var (
		wg      sync.WaitGroup
		statuses [N]int
		bodies  [N]string
	)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(""))
			req.Header.Set(HeaderIDKey, "concurrent-key")
			rec := httptest.NewRecorder()
			Middleware(s)(handler).ServeHTTP(rec, req)
			statuses[i] = rec.Code
			bodies[i] = rec.Body.String()
		}(i)
	}
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Errorf("handler invocations: got %d want 1 (pre-v1.1.1: 200 with 'in-flight' body)", got)
	}
	// All responses must be either 200 (replay or success) or
	// 409 (in-flight). No 5xx, no silent cache of empty body.
	for i, st := range statuses {
		if st != http.StatusOK && st != http.StatusConflict {
			t.Errorf("goroutine %d status: got %d want 200 or 409; body=%q", i, st, bodies[i])
		}
	}
	// Exactly one body must be "first" (the original); the rest
	// are either "first" (replay) or empty (in-flight 409).
	var firstCount int
	for _, b := range bodies {
		if b == "first" {
			firstCount++
		}
	}
	if firstCount < 1 {
		t.Errorf("expected at least one body=\"first\" (winner or replay), got 0; bodies=%v", bodies)
	}
}
