package server

import (
	"context"
	"net/http"
	"sync"
	"time"
)

// pingUpstreams does parallel GET /healthz probes against each upstream
// URL and returns the list of URLs that failed (any non-2xx or
// transport error counts as a failure). Each probe uses a 2s timeout
// so a single dead upstream doesn't block the whole readiness check.
// Designed for /readyz: callers decide what to do with the failed
// list (return 503 + JSON, log, etc.).
func pingUpstreams(ctx context.Context, urls []string) []string {
	if len(urls) == 0 {
		return nil
	}
	type result struct {
		url  string
		fail bool
	}
	results := make(chan result, len(urls))
	var wg sync.WaitGroup
	for _, u := range urls {
		u := u
		wg.Add(1)
		go func() {
			defer wg.Done()
			pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(pctx, http.MethodGet, u, nil)
			if err != nil {
				results <- result{url: u, fail: true}
				return
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				results <- result{url: u, fail: true}
				return
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				results <- result{url: u, fail: true}
				return
			}
			results <- result{url: u, fail: false}
		}()
	}
	wg.Wait()
	close(results)
	var failed []string
	for r := range results {
		if r.fail {
			failed = append(failed, r.url)
		}
	}
	return failed
}
