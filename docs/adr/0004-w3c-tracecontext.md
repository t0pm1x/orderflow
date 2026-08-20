# ADR-0004: W3C Tracecontext propagation through Kafka

- **Status:** Accepted (revised 2026-08-20 — OBS-5 fix)
- **Date:** 2026-08-17
- **Deciders:** orderflow architecture team

## Context

v0.1.0-MVP shipped without cross-service trace correlation. The outbox publisher wrote zero `TraceID`/`SpanID` into the Envelope (explicit TODO at `pkg/outbox/kafka.go:71`), the consumer did not create per-message spans (`pkg/consumer/consumer.go:171`), and `pkg/platform/otel.go` set the global propagator to `propagation.TraceContext{}` but no caller populated or extracted headers.

As a result, the Tempo service map was broken across Kafka topic boundaries: every service appeared as an isolated island.

## Decision

Adopt W3C tracecontext (`traceparent`/`tracestate`) as the cross-service correlation standard.

1. **The outbox publisher propagates trace context into both the Envelope and the Kafka record.** It populates `Envelope.TraceID` and `Envelope.SpanID` in JSON from the active span at publish time (legacy path) AND injects the W3C `traceparent` header into the outgoing `kgo.Record.Headers` (OBS-5). The header path is the source of truth for cross-process propagation; the Envelope IDs are the fallback for legacy consumers and for downstream tooling (Redis dedup keys, SQL trail rows) that don't read Kafka headers.
2. **The consumer creates per-message child spans.** `dispatch` extracts the W3C `traceparent` from the record headers via `kafkaprop.Extract` BEFORE unmarshalling the envelope, then opens the per-message span via `kafkaprop.SpanFromEnvelope`. `SpanFromEnvelope` prefers an already-valid context (from the header extract) and falls back to the Envelope IDs when the header is absent — the two paths converge on the same trace.
3. **Every service initializes OpenTelemetry with service identity.** Every service binary calls `pkg/platform.InitTracing(ctx, serviceName, version)` at startup, setting the `service.name` and `service.version` resource attributes.

## Alternatives Considered

### A. OpenTelemetry-only (no W3C) (rejected)

This would lock us out of cross-language consumers, so it was rejected.

### B. Custom `x-orderflow-trace-id` header (rejected)

This non-standard header would break OpenTelemetry tooling auto-correlation, so it was rejected.

### C. Kafka transaction headers only (no Envelope fields) (rejected — initially proposed, withdrawn)

Envelopes are how the consumer reconstructs a span context today; removing the IDs would break the consumer path for every existing producer that hasn't been recompiled. The OBS-5 fix instead uses BOTH paths: Kafka headers for the cross-process wire format (cross-language safe), Envelope IDs as a fallback. Pre-OBS-5 the Envelope-only path was inert for non-Go consumers; the OBS-5 header path is now live for everyone.

## Consequences

- Every consumer must be OTel-aware. Go services are; legacy consumers without OTel must ignore the new JSON fields and Kafka headers, both of which are additive and therefore backward-compatible.
- The Envelope JSON shape changes additively: `TraceID` and `SpanID` are nullable.
- The `outbox.Record.Headers` column is added as JSONB with a default of `{}`; there is no schema migration risk beyond the additive column.
- Kafka record headers now carry `traceparent` (and any business headers the producer passes). Operators inspecting raw Kafka traffic with `kcat -H` will see the new header on every record emitted by an OBS-5 producer.

## References

- W3C Trace Context spec: https://www.w3.org/TR/trace-context/
- `pkg/platform/instrumentation/kafkaprop/kafkaprop.go` (new module from 3.10.a; OBS-5 added `RecordHeaderCarrier`)
- `pkg/platform/events/events.go` (`PublishRaw` now injects traceparent; OBS-5)
- `pkg/outbox/kafka.go` (`recordToEnvelope` change in 3.10.b; OBS-5 still emits Envelope IDs as fallback)
- `pkg/consumer/consumer.go` (`dispatch` reads headers via `kafkaprop.Extract` before envelope-body fallback; OBS-5)
- `pkg/platform/otel.go` (`InitTracing` signature change in 3.10.e)
