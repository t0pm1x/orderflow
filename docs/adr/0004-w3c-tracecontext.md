# ADR-0004: W3C Tracecontext propagation through Kafka

- **Status:** Accepted
- **Date:** 2026-08-17
- **Deciders:** orderflow architecture team

## Context

v0.1.0-MVP shipped without cross-service trace correlation. The outbox publisher wrote zero `TraceID`/`SpanID` into the Envelope (explicit TODO at `pkg/outbox/kafka.go:71`), the consumer did not create per-message spans (`pkg/consumer/consumer.go:171`), and `pkg/platform/otel.go` set the global propagator to `propagation.TraceContext{}` but no caller populated or extracted headers.

As a result, the Tempo service map was broken across Kafka topic boundaries: every service appeared as an isolated island.

## Decision

Adopt W3C tracecontext (`traceparent`/`tracestate`) as the cross-service correlation standard.

1. **The outbox publisher propagates trace context into the envelope and Kafka record.** It populates `Envelope.TraceID` and `Envelope.SpanID` in JSON from the active span at publish time. The new `outbox.Record.Headers` carries the W3C `traceparent` header for non-Go consumers.
2. **The consumer creates per-message child spans.** `dispatch` creates a child span via `kafkaprop.SpanFromEnvelope`, so each `consumer.<EventType>` span links to the original producer trace.
3. **Every service initializes OpenTelemetry with service identity.** Every service binary calls `pkg/platform.InitTracing(ctx, serviceName, version)` at startup, setting the `service.name` and `service.version` resource attributes.

## Alternatives Considered

### A. OpenTelemetry-only (no W3C) (rejected)

This would lock us out of cross-language consumers, so it was rejected.

### B. Custom `x-orderflow-trace-id` header (rejected)

This non-standard header would break OpenTelemetry tooling auto-correlation, so it was rejected.

### C. Kafka transaction headers only (no Envelope fields) (rejected)

Redis and SQL fallback paths, including the saga timeout sweep, would lose correlation, so it was rejected.

## Consequences

- Every consumer must be OTel-aware. Go services are; legacy consumers without OTel must ignore the new JSON fields, which are additive and therefore backward-compatible.
- The Envelope JSON shape changes additively: `TraceID` and `SpanID` are nullable.
- The `outbox.Record.Headers` column is added as JSONB with a default of `{}`; there is no schema migration risk beyond the additive column.

## References

- W3C Trace Context spec: https://www.w3.org/TR/trace-context/
- `pkg/platform/instrumentation/kafkaprop/kafkaprop.go` (new module from 3.10.a)
- `pkg/outbox/kafka.go` (`recordToEnvelope` change in 3.10.b)
- `pkg/consumer/consumer.go` (`dispatch` change in 3.10.c)
- `pkg/platform/otel.go` (`InitTracing` signature change in 3.10.e)
