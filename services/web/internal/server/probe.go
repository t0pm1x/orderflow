package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

type ServiceHealth struct {
	Status    string `json:"status"`
	LatencyMS int64  `json:"latency_ms"`
	TakenAt   string `json:"taken_at"`
	Detail    string `json:"detail,omitempty"`
}

type HealthSnapshot struct {
	Order      ServiceHealth `json:"order"`
	Payment    ServiceHealth `json:"payment"`
	Inventory  ServiceHealth `json:"inventory"`
	Saga       ServiceHealth `json:"saga"`
	Kafka      ServiceHealth `json:"kafka"`
	SnapshotAt string        `json:"snapshot_at"`
}

// probeOne GETs u's /healthz with a 2s timeout and classifies
// the result per the rules in the dashboard spec:
//   - down      if transport error / timeout / HTTP 5xx / body says "down"
//   - degraded  if HTTP 200 AND (latency >= 1s OR body says "degraded")
//   - ok        otherwise (HTTP 200, latency < 1s, body absent or "ok")
//
// Always returns a non-zero ServiceHealth — the caller never
// has to nil-check.
func probeOne(parent context.Context, u string) ServiceHealth {
	start := time.Now()
	pctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(pctx, http.MethodGet, u+"/healthz", nil)
	if err != nil {
		return ServiceHealth{
			Status:  "down",
			TakenAt: time.Now().UTC().Format(time.RFC3339Nano),
			Detail:  err.Error(),
		}
	}
	resp, err := http.DefaultClient.Do(req)
	elapsed := time.Since(start)
	if err != nil {
		return ServiceHealth{
			Status:    "down",
			LatencyMS: elapsed.Milliseconds(),
			TakenAt:   time.Now().UTC().Format(time.RFC3339Nano),
			Detail:    err.Error(),
		}
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	bodyStr := strings.TrimSpace(string(body))
	bodyStatus := parseBodyStatus(bodyStr)
	switch {
	case resp.StatusCode >= 500:
		return ServiceHealth{
			Status:    "down",
			LatencyMS: elapsed.Milliseconds(),
			TakenAt:   time.Now().UTC().Format(time.RFC3339Nano),
			Detail:    fmt.Sprintf("upstream returned %d", resp.StatusCode),
		}
	case bodyStatus == "down":
		return ServiceHealth{
			Status:    "down",
			LatencyMS: elapsed.Milliseconds(),
			TakenAt:   time.Now().UTC().Format(time.RFC3339Nano),
			Detail:    bodyStr,
		}
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		if elapsed >= time.Second || bodyStatus == "degraded" {
			return ServiceHealth{
				Status:    "degraded",
				LatencyMS: elapsed.Milliseconds(),
				TakenAt:   time.Now().UTC().Format(time.RFC3339Nano),
				Detail:    detailForDegraded(elapsed, bodyStr),
			}
		}
		return ServiceHealth{
			Status:    "ok",
			LatencyMS: elapsed.Milliseconds(),
			TakenAt:   time.Now().UTC().Format(time.RFC3339Nano),
		}
	default:
		return ServiceHealth{
			Status:    "down",
			LatencyMS: elapsed.Milliseconds(),
			TakenAt:   time.Now().UTC().Format(time.RFC3339Nano),
			Detail:    fmt.Sprintf("upstream returned %d", resp.StatusCode),
		}
	}
}

// parseBodyStatus extracts a "status":"<x>" field from the body.
// Returns "" if the body is empty, not JSON, or has no status.
func parseBodyStatus(body string) string {
	if body == "" {
		return ""
	}
	var parsed struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal([]byte(body), &parsed); err != nil {
		return ""
	}
	return parsed.Status
}

func detailForDegraded(elapsed time.Duration, body string) string {
	if body != "" && parseBodyStatus(body) == "degraded" {
		return body
	}
	return fmt.Sprintf("latency %dms exceeds 1000ms threshold", elapsed.Milliseconds())
}

// HealthAll GET /api/health/all — probes every upstream /healthz
// in parallel (2s per probe, 1s snapshot cache) and reports the
// in-process Kafka tail state. Always returns HTTP 200; degraded
// and down are valid payload contents. Cache key is the wall
// clock — collision-free in practice for a single-process
// playground.
func (s *Server) HealthAll(w http.ResponseWriter, r *http.Request) {
	type cacheEntry struct {
		taken    time.Time
		snapshot HealthSnapshot
	}
	s.healthCacheMu.Lock()
	cached, ok := s.healthCache.(cacheEntry)
	if ok && time.Since(cached.taken) < time.Second {
		s.healthCacheMu.Unlock()
		writeJSON(w, http.StatusOK, cached.snapshot)
		return
	}
	s.healthCacheMu.Unlock()

	urls := s.opt.Urls
	var wg sync.WaitGroup
	snap := HealthSnapshot{SnapshotAt: time.Now().UTC().Format(time.RFC3339Nano)}
	for _, target := range []struct {
		name string
		url  string
		dest *ServiceHealth
	}{
		{"order", urls.Order, &snap.Order},
		{"payment", urls.Payment, &snap.Payment},
		{"inventory", urls.Inventory, &snap.Inventory},
		{"saga", urls.Saga, &snap.Saga},
	} {
		target := target
		wg.Add(1)
		go func() {
			defer wg.Done()
			if target.url == "" {
				*target.dest = ServiceHealth{
					Status:  "down",
					TakenAt: time.Now().UTC().Format(time.RFC3339Nano),
					Detail:  "upstream URL not configured",
				}
				return
			}
			*target.dest = probeOne(r.Context(), target.url)
		}()
	}
	// Kafka has no /healthz — read the closure supplied via Options.
	if s.opt.KafkaHealth != nil && s.opt.KafkaHealth() {
		snap.Kafka = ServiceHealth{
			Status:    "ok",
			LatencyMS: 0,
			TakenAt:   time.Now().UTC().Format(time.RFC3339Nano),
		}
	} else {
		snap.Kafka = ServiceHealth{
			Status:  "down",
			TakenAt: time.Now().UTC().Format(time.RFC3339Nano),
			Detail:  "Kafka tail not running (KAFKA_BROKERS unset or consumer error)",
		}
	}
	wg.Wait()

	s.healthCacheMu.Lock()
	s.healthCache = cacheEntry{taken: time.Now(), snapshot: snap}
	s.healthCacheMu.Unlock()

	writeJSON(w, http.StatusOK, snap)
}
