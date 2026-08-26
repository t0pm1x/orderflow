// Package server — SSE endpoint. Streams the in-process event
// bus (populated by the Kafka tail goroutine) to any subscribed
// browser via text/event-stream.
//
// Wire format (one event per message, framed by a blank line):
//
//   id: <event_id>
//   data: {"event_id":"...","event_type":"...","aggregate_id":"...",
//
//	"occurred_at":"...","payload":{...}}\n
//
//   <blank line>
//
// Last-Event-ID replay: when the client reconnects with
// `Last-Event-ID: <id>` we replay every ring-buffer event with
// id > Last-Event-ID before subscribing. The ring buffer is
// bounded at 200 entries (events.NewBus); events that fell off
// the ring are simply not replayed.
package server

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	pkgevents "github.com/t0pm1x/orderflow/platform/events"
	webevents "github.com/t0pm1x/orderflow/services/web/internal/events"
)

// sseHandler produces the GET /events/stream handler. If
// eventsEnabled is false (no Kafka broker configured), returns
// 503 + JSON so the SPA can render "Live events: disconnected"
// in the sidebar instead of an open stream that only emits
// heartbeats.
func sseHandler(bus *webevents.Bus, logger *slog.Logger, eventsEnabled bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !eventsEnabled {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = writeJSONErr(w, `{"error":"events_unavailable"}`)
			return
		}

		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		// Replay from the ring buffer using Last-Event-ID. The
		// replay runs synchronously BEFORE Subscribe so the
		// browser sees any events that fired while disconnected
		// before any new ones.
		if lastID := r.Header.Get("Last-Event-ID"); lastID != "" {
			for _, env := range bus.HistoryAll() {
				if env.EventID <= lastID {
					continue
				}
				if !writeSSE(w, flusher, &env, logger) {
					return
				}
			}
		}

		ch, unsub := bus.Subscribe()
		defer unsub()

		heartbeat := time.NewTicker(15 * time.Second)
		defer heartbeat.Stop()

		if _, err := fmt.Fprintf(w, ": connected\n\n"); err != nil {
			return
		}
		flusher.Flush()

		for {
			select {
			case <-r.Context().Done():
				return
			case <-heartbeat.C:
				if _, err := fmt.Fprintf(w, ": ping\n\n"); err != nil {
					return
				}
				flusher.Flush()
			case ev, ok := <-ch:
				if !ok {
					return
				}
				if !writeSSE(w, flusher, &ev.Envelope, logger) {
					return
				}
			}
		}
	}
}

// writeSSE serializes one envelope as `id:` + `data:` lines and
// flushes. Returns false if the write or flush failed (client
// disconnected); the caller should return immediately.
//
// Format matches the browser's EventSource parser: each event is
// terminated by a blank line. We emit unnamed messages (no `event:`
// line) so the browser dispatches them with the default event
// type `"message"`, which is what the SPA's EventSource listener
// subscribes to. The event_type is still in the JSON payload so
// the SPA can colour-code it.
func writeSSE(w http.ResponseWriter, flusher http.Flusher, env *pkgevents.Envelope, logger *slog.Logger) bool {
	data, err := json.Marshal(env)
	if err != nil {
		logger.Warn("SSE marshal envelope failed",
			"event_id", env.EventID,
			"event_type", env.EventType,
			"err", err)
		return true // marshal failure on one event shouldn't kill the stream
	}
	if _, err := fmt.Fprintf(w, "id: %s\ndata: %s\n\n", env.EventID, data); err != nil {
		return false
	}
	flusher.Flush()
	return true
}

// writeJSONErr is a tiny helper that mirrors writeJSON's semantics
// for the 503 unavailable case (no extra fields, just an error
// envelope).
func writeJSONErr(w http.ResponseWriter, body string) error {
	_, err := w.Write([]byte(body))
	return err
}
