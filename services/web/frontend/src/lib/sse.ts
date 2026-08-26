// Live-event SSE bridge.
//
// The BFF serves /events/stream as a server-sent-events stream
// (text/event-stream with one `id:` + `data:` line per event,
// terminated by a blank line). The browser's EventSource
// dispatches each message as the JS event type "message" because
// we don't emit an `event:` line (matches the SPA contract —
// see services/web/internal/server/sse.go).
//
// We wrap the EventSource in a tiny reconnecting client + a Svelte
// writable store so any component can subscribe via:
//
//   import { liveEvents } from '$lib/sse';
//
// The store is updated from the EventSource `message` listener; the
// EventSource is created lazily on first connectSSE() call. We
// throttle reconnects (exponential backoff capped at 30s) so a
// transient BFF restart doesn't hammer the server.

import { writable } from 'svelte/store';
import type { SseEvent } from './types';

const MAX_EVENTS = 50;
const RECONNECT_BASE_MS = 1_000;
const RECONNECT_MAX_MS = 30_000;

export const liveEvents = writable<SseEvent[]>([]);
export const sseConnected = writable<boolean>(false);

let source: EventSource | null = null;
let attempt = 0;
let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
let connectedOnce = false;

function scheduleReconnect(): void {
  if (reconnectTimer != null) return;
  const delay = Math.min(
    RECONNECT_BASE_MS * 2 ** attempt,
    RECONNECT_MAX_MS
  );
  attempt++;
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connectSSE();
  }, delay);
}

export function connectSSE(): void {
  if (typeof window === 'undefined') return;
  if (source && source.readyState !== EventSource.CLOSED) return;

  try {
    source = new EventSource('/events/stream', { withCredentials: false });
  } catch {
    scheduleReconnect();
    return;
  }

  source.onopen = () => {
    attempt = 0;
    connectedOnce = true;
    sseConnected.set(true);
  };

  source.onmessage = (e) => {
    try {
      const ev = JSON.parse(e.data) as SseEvent;
      liveEvents.update((list) => [ev, ...list].slice(0, MAX_EVENTS));
    } catch (_) {
      // ignore malformed payloads — the stream stays open
    }
  };

  source.onerror = () => {
    sseConnected.set(false);
    if (connectedOnce) {
      // first successful connect: just retry
      scheduleReconnect();
    } else {
      // never connected: probably BFF hasn't started or KAFKA is
      // disabled → keep retrying with backoff so a later start
      // eventually wires up the live-event sidebar.
      scheduleReconnect();
    }
  };
}

export function disconnectSSE(): void {
  if (reconnectTimer != null) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }
  source?.close();
  source = null;
  sseConnected.set(false);
}
