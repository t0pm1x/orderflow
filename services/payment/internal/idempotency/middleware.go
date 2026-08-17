// Package idempotency provides an HTTP middleware that uses the Store
// to dedupe requests by Idempotency-Key header. On first sight, it
// reserves the key, runs the handler, and caches the response. On
// duplicate it replays the cached body. On handler error (>=500) it
// releases the reservation so a retry can succeed.
package idempotency

import (
	"bytes"
	"errors"
	"net/http"
)

// HeaderIDKey is the HTTP header used for idempotency. Matches the
// convention used by Stripe, PayPal, and the orderflow events spec.
const HeaderIDKey = "Idempotency-Key"

// HeaderReplayed is set on responses that are cached replays.
const HeaderReplayed = "Idempotent-Replayed"

// Middleware returns a chi-compatible middleware that enforces
// idempotent handling for requests carrying an Idempotency-Key.
//
// Flow:
//
//  1. If the header is missing, respond 400 — the webhook contract
//     requires a key.
//  2. Store.Begin(key):
//     - on *ErrDuplicate, write the cached body with status 200 and
//     HeaderReplayed=true; do not call the handler.
//     - on other error, respond 500.
//     - on success, capture the handler's response.
//  3. After the handler runs:
//     - if status >= 500, Store.Release so a retry can succeed.
//     - otherwise, Store.Complete with the captured body bytes.
//
// The captured body is the raw bytes the handler wrote; status and
// headers besides HeaderReplayed are not preserved on replay
// (callers that depend on response headers other than Content-Type
// should embed them in the body).
func Middleware(s *Store) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := r.Header.Get(HeaderIDKey)
			if key == "" {
				http.Error(w, "Idempotency-Key header required", http.StatusBadRequest)
				return
			}

			res, err := s.Begin(r.Context(), key)
			if err != nil {
				var dup *ErrDuplicate
				if errors.As(err, &dup) {
					w.Header().Set(HeaderReplayed, "true")
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write(dup.CachedResponse)
					return
				}
				http.Error(w, "idempotency backend error", http.StatusInternalServerError)
				return
			}

			buf := &responseBuffer{ResponseWriter: w}
			next.ServeHTTP(buf, r)

			if buf.status >= 500 {
				_ = s.Release(r.Context(), res)
				return
			}
			_ = s.Complete(r.Context(), res, buf.body.Bytes())
		})
	}
}

// responseBuffer wraps http.ResponseWriter to capture status and
// body so the middleware can persist them in the idempotency store.
type responseBuffer struct {
	http.ResponseWriter
	status int
	body   bytes.Buffer
}

func (r *responseBuffer) WriteHeader(s int) {
	if r.status == 0 {
		r.status = s
	}
	r.ResponseWriter.WriteHeader(s)
}

func (r *responseBuffer) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	r.body.Write(b)
	return r.ResponseWriter.Write(b)
}
