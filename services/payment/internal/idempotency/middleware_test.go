package idempotency

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
