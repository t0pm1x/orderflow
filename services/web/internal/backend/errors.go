package backend

import "fmt"

// HTTPError represents a non-2xx upstream response. Status carries
// the HTTP status code so callers can distinguish 4xx (user error)
// from 5xx (upstream failure) from network errors.
//
// Network/transport errors are NOT wrapped in HTTPError — they
// remain raw *url.Error / *net.OpError values so callers can
// errors.As(&netErr) if they want, or simply treat them as 502.
type HTTPError struct {
	Status int
	Body   string
	URL    string
}

// Error renders the upstream status + truncated body so logs and
// UI banners stay readable. The full body is preserved in .Body.
func (e *HTTPError) Error() string {
	return fmt.Sprintf("upstream status %d: %s", e.Status, e.Body)
}

// Status exposes the upstream HTTP status. Handy for callers that
// want to do `var he *HTTPError; if errors.As(err, &he) { he.Status() }`.
func (e *HTTPError) StatusCode() int { return e.Status }