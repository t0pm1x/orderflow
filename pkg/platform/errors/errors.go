// Package errors provides typed errors that map to HTTP responses.
package errors

import (
	"errors"
	"fmt"
	"net/http"
)

// APIError is a typed error that maps to a specific HTTP status code.
type APIError struct {
	Status  int
	Code    string
	Message string
	Cause   error
}

func (e *APIError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *APIError) Unwrap() error { return e.Cause }

// Common errors with sensible defaults.
var (
	ErrNotFound       = &APIError{Status: http.StatusNotFound, Code: "NOT_FOUND", Message: "resource not found"}
	ErrConflict       = &APIError{Status: http.StatusConflict, Code: "CONFLICT", Message: "conflict"}
	ErrInvalidPayload = &APIError{Status: http.StatusBadRequest, Code: "INVALID_PAYLOAD", Message: "invalid request body"}
	ErrUnauthorized   = &APIError{Status: http.StatusUnauthorized, Code: "UNAUTHORIZED", Message: "missing or invalid credentials"}
	ErrRateLimited    = &APIError{Status: http.StatusTooManyRequests, Code: "RATE_LIMITED", Message: "too many requests"}
	ErrInternal       = &APIError{Status: http.StatusInternalServerError, Code: "INTERNAL", Message: "internal server error"}
)

// Wrap creates an APIError that wraps a cause.
func Wrap(status int, code, msg string, cause error) *APIError {
	return &APIError{Status: status, Code: code, Message: msg, Cause: cause}
}

// HTTPStatus extracts status from any error (default 500).
func HTTPStatus(err error) int {
	var api *APIError
	if errors.As(err, &api) {
		return api.Status
	}
	return http.StatusInternalServerError
}

// Code extracts code from any error (default "INTERNAL").
func Code(err error) string {
	var api *APIError
	if errors.As(err, &api) {
		return api.Code
	}
	return "INTERNAL"
}
