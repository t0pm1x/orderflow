package handlers

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/t0pm1x/orderflow/services/web/internal/backend"
)

// mapUpstreamError turns a backend error into a user-safe message
// and the HTTP status the BFF should return. The helper logs the
// original error server-side with full detail (status, body, URL)
// so operators can diagnose without exposing internal payloads to
// end users. The returned userMsg is suitable for layout-banner
// rendering; the caller is responsible for writing the status code
// and rendering the page.
//
// Mapping rules (P1.1 audit rubric):
//   - nil  -> 200, "" (caller skips the helper on success)
//   - 4xx  -> 4xx (or 400 for 422), user-fixable message
//   - 5xx  -> 502, "try again" hint
//   - transport (non-HTTPError) -> 502, "cannot reach" hint
//
// The original upstream body is NEVER echoed in userMsg.
func mapUpstreamError(logger *slog.Logger, route string, err error) (userMsg string, status int) {
	if err == nil {
		return "", http.StatusOK
	}
	var he *backend.HTTPError
	if errors.As(err, &he) {
		logger.Warn("upstream error",
			"route", route,
			"status", he.Status,
			"body", he.Body,
			"url", he.URL)
		switch {
		case he.Status >= http.StatusBadRequest && he.Status < 500:
			switch he.Status {
			case http.StatusBadRequest:
				return "The order service rejected the request. Please check your input.", http.StatusBadRequest
			case http.StatusNotFound:
				return "Not found.", http.StatusNotFound
			case http.StatusConflict:
				return "Conflict \u2014 the order may already be in this state.", http.StatusConflict
			case http.StatusUnprocessableEntity:
				return "The request was understood but rejected. Please check your input.", http.StatusBadRequest
			default:
				return "The order service rejected the request.", http.StatusBadRequest
			}
		default:
			return "The order service is temporarily unavailable. Please try again in a moment.", http.StatusBadGateway
		}
	}
	logger.Error("upstream transport error", "route", route, "err", err)
	return "Cannot reach the order service. Please check your connection.", http.StatusBadGateway
}
