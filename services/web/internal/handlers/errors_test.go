package handlers

import (
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/t0pm1x/orderflow/services/web/internal/backend"
)

// TestMapUpstreamError_Nil covers the no-error fast path: the helper
// must return 200 + "" so the caller can `if err != nil { map(...) }`
// without pessimizing the happy path. Without this contract, callers
// would either always pay the parse cost or skip the helper for
// success, which is exactly the bug TDD is supposed to prevent.
func TestMapUpstreamError_Nil(t *testing.T) {
	msg, status := mapUpstreamError(slog.Default(), "test", nil)
	if status != http.StatusOK {
		t.Errorf("status: got %d want 200", status)
	}
	if msg != "" {
		t.Errorf("msg: got %q want \"\"", msg)
	}
}

// TestMapUpstreamError_4xx_BadRequest covers the user-input branch:
// upstream 400 is a validation error, so the BFF should surface 400
// with a "check your input" hint. The raw upstream body MUST NOT
// appear in the message — this is the P1.1 contract.
func TestMapUpstreamError_4xx_BadRequest(t *testing.T) {
	err := &backend.HTTPError{Status: 400, Body: "internal debug: stack trace here", URL: "http://order/v1/orders"}
	msg, status := mapUpstreamError(slog.Default(), "POST /v1/orders", err)
	if status != http.StatusBadRequest {
		t.Errorf("status: got %d want 400", status)
	}
	if strings.Contains(msg, "stack trace") {
		t.Errorf("msg echoes upstream body: %q", msg)
	}
	if strings.Contains(msg, "internal debug") {
		t.Errorf("msg echoes upstream body: %q", msg)
	}
	if msg == "" {
		t.Errorf("msg must not be empty")
	}
}

// TestMapUpstreamError_4xx_NotFound covers the "no such order"
// branch: upstream 404 collapses to 404 (and stays as a Not found
// banner) so the page-detail handler can re-render the layout.
func TestMapUpstreamError_4xx_NotFound(t *testing.T) {
	err := &backend.HTTPError{Status: 404, Body: "order not found", URL: "http://order/v1/orders/x"}
	msg, status := mapUpstreamError(slog.Default(), "GET /orders/{id}", err)
	if status != http.StatusNotFound {
		t.Errorf("status: got %d want 404", status)
	}
	if strings.Contains(msg, "not found") && strings.Contains(msg, "order") {
		// OK: "Not found" is generic; the rendered message must not carry
		// the upstream's "order not found" payload verbatim.
		if strings.Contains(msg, "order not found") {
			t.Errorf("msg echoes upstream body verbatim: %q", msg)
		}
	}
}

// TestMapUpstreamError_4xx_Conflict covers order-state conflicts:
// upstream 409 (already-cancelled, already-reserved) maps to 409
// with a "state" hint. Caller decides whether to surface 409 or
// fold to 400; helper preserves the status so the choice stays
// with the route.
func TestMapUpstreamError_4xx_Conflict(t *testing.T) {
	err := &backend.HTTPError{Status: 409, Body: "already cancelled", URL: "http://order/v1/orders/x"}
	_, status := mapUpstreamError(slog.Default(), "POST /v1/orders/{id}", err)
	if status != http.StatusConflict {
		t.Errorf("status: got %d want 409", status)
	}
}

// TestMapUpstreamError_4xx_Unprocessable maps to 400 (validation),
// per audit rubric: 422 is "I understood the request but rejected
// it" — a user-fixable error pattern, not a BFF-protocol error.
func TestMapUpstreamError_4xx_Unprocessable(t *testing.T) {
	err := &backend.HTTPError{Status: 422, Body: "validation: qty must be > 0", URL: "http://order/v1/orders"}
	_, status := mapUpstreamError(slog.Default(), "POST /v1/orders", err)
	if status != http.StatusBadRequest {
		t.Errorf("status: got %d want 400 (422 folds to 400 user-fixable)", status)
	}
}

// TestMapUpstreamError_5xx covers the upstream-failure branch: 5xx
// is NEVER user-fixable, so the BFF returns 502 (Bad Gateway) with
// a "try again" hint. The upstream body must be discarded.
func TestMapUpstreamError_5xx(t *testing.T) {
	err := &backend.HTTPError{Status: 503, Body: "service unavailable: db down", URL: "http://order/v1/orders"}
	msg, status := mapUpstreamError(slog.Default(), "POST /v1/orders", err)
	if status != http.StatusBadGateway {
		t.Errorf("status: got %d want 502", status)
	}
	if strings.Contains(msg, "db down") {
		t.Errorf("msg echoes upstream body: %q", msg)
	}
	if strings.Contains(msg, "service unavailable") {
		t.Errorf("msg echoes upstream body: %q", msg)
	}
}

// TestMapUpstreamError_Transport covers the network-error branch:
// a *url.Error wrapped in something non-HTTPError is the dial-fail
// case. Must map to 502 + "cannot reach" hint with the original
// error preserved in the server-side log (not the user message).
func TestMapUpstreamError_Transport(t *testing.T) {
	_, err := url.Parse("http://nope.invalid/")
	if err != nil {
		t.Fatal(err)
	}
	transport := &url.Error{Op: "Post", URL: "http://nope.invalid/", Err: errors.New("dial tcp: no such host")}
	msg, status := mapUpstreamError(slog.Default(), "POST /v1/orders", transport)
	if status != http.StatusBadGateway {
		t.Errorf("status: got %d want 502", status)
	}
	if strings.Contains(msg, "dial tcp") {
		t.Errorf("msg echoes transport error: %q", msg)
	}
	if strings.Contains(msg, "nope.invalid") {
		t.Errorf("msg echoes transport URL: %q", msg)
	}
}

// TestMapUpstreamError_WrappedHTTPError covers the path where the
// HTTPError is wrapped in fmt.Errorf("...: %w", ...) down in the
// backend client. errors.As must still peel it off.
func TestMapUpstreamError_WrappedHTTPError(t *testing.T) {
	inner := &backend.HTTPError{Status: 400, Body: "stack trace payload", URL: "http://order/v1/orders"}
	wrapped := errors.New("backend wrapped: " + inner.Error())
	wrapped = wrapErr(wrapped, inner)
	msg, status := mapUpstreamError(slog.Default(), "POST /v1/orders", wrapped)
	if status != http.StatusBadRequest {
		t.Errorf("status: got %d want 400", status)
	}
	if strings.Contains(msg, "stack trace") {
		t.Errorf("wrapped error leaked body: %q", msg)
	}
}

// wrapErr is a tiny %w wrapper used by the wrapped-error test so
// the test file doesn't pull fmt in just for one call site.
func wrapErr(outer, inner error) error {
	return &wrappedErr{outer: outer, inner: inner}
}

type wrappedErr struct {
	outer error
	inner error
}

func (w *wrappedErr) Error() string { return w.outer.Error() + ": " + w.inner.Error() }
func (w *wrappedErr) Unwrap() error { return w.inner }
