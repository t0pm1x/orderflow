package middleware

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// Check is a single dependency probe the /readyz handler runs in
// parallel. A nil return means the dependency is healthy; a non-nil
// error is reported in the /readyz response and counted as a failure.
// Implementations should respect ctx (the /readyz handler caps it at
// 2 seconds) and be cheap — they are called on every Kubernetes
// readiness probe (typically every 5-10s).
type Check func(ctx context.Context) error

// ReadyHandler returns an http.HandlerFunc that runs every supplied
// Check in parallel under a 2-second timeout and writes:
//
//   - 200 + {"status":"ok"} when no Check returned an error
//   - 503 + {"status":"down","failed":[...names...]} when at least
//     one Check failed
//
// `names` pairs 1:1 with `checks` and is the human-readable label
// included in the failure response so an operator can pinpoint which
// dependency is unhealthy. The handler is intentionally tolerant of
// an empty check list (returns 200 with {"status":"ok"}) so disabled
// services — DATABASE_URL unset, KAFKA_BROKERS unset — can mount /readyz
// with zero checks.
//
// The 2-second timeout matches the value services/web/internal/server
// uses for its upstream probes; Kubernetes typically waits 5s for the
// readiness probe HTTP request, so 2s of probe budget plus 1s of HTTP
// overhead leaves 2s for the kubelet's next decision.
func ReadyHandler(names []string, checks []Check) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if len(checks) == 0 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		results := make(chan checkResult, len(checks))
		var wg sync.WaitGroup
		wg.Add(len(checks))
		for i, c := range checks {
			i, c := i, c
			go func() {
				defer wg.Done()
				err := c(ctx)
				results <- checkResult{name: names[i], err: err}
			}()
		}
		wg.Wait()
		close(results)

		var failed []string
		for res := range results {
			if res.err != nil {
				failed = append(failed, res.name)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		if len(failed) > 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(struct {
				Status string   `json:"status"`
				Failed []string `json:"failed"`
			}{Status: "down", Failed: failed})
			return
		}
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}
}

type checkResult struct {
	name string
	err  error
}
