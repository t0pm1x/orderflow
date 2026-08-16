package errors

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
)

func TestHTTPStatus(t *testing.T) {
	if HTTPStatus(ErrNotFound) != http.StatusNotFound {
		t.Error("not found should be 404")
	}
	if HTTPStatus(errors.New("plain")) != http.StatusInternalServerError {
		t.Error("plain error should be 500")
	}
}

func TestCode(t *testing.T) {
	if Code(ErrNotFound) != "NOT_FOUND" {
		t.Error("expected NOT_FOUND")
	}
}

func TestWrap(t *testing.T) {
	cause := errors.New("underlying")
	wrapped := Wrap(http.StatusBadRequest, "TEST", "test msg", cause)
	if wrapped.Status != http.StatusBadRequest {
		t.Error("status mismatch")
	}
	if !errors.Is(wrapped, cause) {
		t.Error("Unwrap should expose cause")
	}
	msg := wrapped.Error()
	if msg == "" {
		t.Error("Error() should produce message")
	}
	fmt.Println(msg)
}
