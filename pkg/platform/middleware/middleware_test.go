package middleware

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestStack_PassesThrough(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})

	chain := Stack("test", slog.New(slog.NewJSONHandler(io.Discard, nil)))
	var final http.Handler = h
	for i := len(chain) - 1; i >= 0; i-- {
		final = chain[i](final)
	}

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()
	final.ServeHTTP(w, req)

	if w.Code != http.StatusTeapot {
		t.Errorf("expected 418, got %d", w.Code)
	}
}