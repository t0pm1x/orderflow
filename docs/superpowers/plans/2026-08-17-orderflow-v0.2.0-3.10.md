# Stage 3.10 — W3C tracecontext through Kafka + Tempo service map

**Why:** `pkg/outbox/kafka.go:73` `recordToEnvelope` currently writes zero `TraceID`/`SpanID` (explicitly TODO'd). `pkg/consumer/consumer.go:171` `dispatch` creates no span per message. Together this breaks Tempo's service map across topic boundaries. Goal: publish traceparent via Envelope, restore it on consume, instrument franz-go client with otelfranzgo, and make chi middleware the trace source for inbound HTTP.

### Task 3.10.a — Add otelfranzgo to all modules that touch kgo (SEQ)

**Files:**
- Modify: `pkg/outbox/go.mod`
- Modify: `pkg/consumer/go.mod`
- Modify: `pkg/platform/go.mod`
- Modify: `pkg/outbox/go.sum`
- Modify: `pkg/consumer/go.sum`
- Modify: `pkg/platform/go.sum`

**Interfaces:**
- Adds import path `go.opentelemetry.io/contrib/instrumentation/github.com/twmb/franz-go/kgootelmw` (verify exact path; if missing, fall back to wrapping manually — see Task 3.10.a-step-3 fallback).

- [ ] **Step 1: Decide the instrumentation path**

Run in PowerShell:
```powershell
cd C:\Users\t0p_m\projects\orderflow
$out = & go list -m -versions go.opentelemetry.io/contrib/instrumentation/github.com/twmb/franz-go/kgootelmw 2>&1
$out | Select-Object -Last 3
```
Expected: at least one version returned. If "no matching versions" or empty: see Step 3 fallback.

- [ ] **Step 2: Add the module to the three consumers**

For each of `pkg/outbox/go.mod`, `pkg/consumer/go.mod`, `pkg/platform/go.mod`:
```powershell
cd C:\Users\t0p_m\projects\orderflow\pkg\<outbox|consumer|platform>
go get go.opentelemetry.io/contrib/instrumentation/github.com/twmb/franz-go/kgootelmw@latest
go mod tidy
```

- [ ] **Step 3: Fallback if Step 2 fails (manual wrapper)**

Create `pkg/platform/instrumentation/kafkaprop/kafkaprop.go` exporting:
```go
package kafkaprop

import (
    "context"
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/propagation"
    "go.opentelemetry.io/otel/trace"
)

// Inject writes W3C traceparent into the supplied header carrier.
func Inject(ctx context.Context, carrier propagation.TextMapCarrier) {
    otel.GetTextMapPropagator().Inject(ctx, carrier)
}

// Extract reads traceparent from the carrier into ctx.
func Extract(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
    return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

// SpanFromEnvelope returns the span for a deserialized Envelope (creates a child if trace_id present).
func SpanFromEnvelope(ctx context.Context, traceID, spanID, name string) (context.Context, trace.Span) {
    sc := trace.NewSpanContext(trace.SpanContextConfig{
        TraceID:    trace.TraceIDFromHex(traceID),
        SpanID:     trace.SpanIDFromHex(spanID),
        TraceFlags: trace.FlagsSampled,
        Remote:     true,
    })
    if !sc.IsValid() {
        return otel.Tracer("orderflow").Start(ctx, name)
    }
    return otel.Tracer("orderflow").Start(
        trace.ContextWithSpanContext(ctx, sc),
        name,
    )
}
```
Add a unit test `kafkaprop_test.go` that writes a fake `propagation.TraceContext` header via `Inject` and reads it back via `Extract`, asserting the resulting `SpanContext().TraceID()` matches. Skips itself when `t.Setenv("SKIP_OTEL_TESTS","1") != ""`.

- [ ] **Step 4: Update go.work**

If `pkg/platform/instrumentation/kafkaprop` was created, run from repo root:
```powershell
cd C:\Users\t0p_m\projects\orderflow
go work use ./pkg/platform/instrumentation/kafkaprop
go work sync
```

- [ ] **Step 5: Commit**

```powershell
cd C:\Users\t0p_m\projects\orderflow
git add pkg/outbox/go.mod pkg/outbox/go.sum pkg/consumer/go.mod pkg/consumer/go.sum pkg/platform/go.mod pkg/platform/go.sum go.work go.work.sum pkg/platform/instrumentation
git commit -m "orderflow/3.10.a: wire W3C tracecontext propagation dependency"
```

### Task 3.10.b — Populate TraceID/SpanID in outbox publisher (PAR with 3.10.c, 3.10.d, 3.10.e)

**Files:**
- Modify: `pkg/outbox/kafka.go` (lines 71-101, `recordToEnvelope` and `PublishRaw` invocation)
- Modify: `pkg/outbox/kafka_test.go` (add test for envelope trace fields)
- Modify: `pkg/outbox/types.go` (add `Headers` field)
- Create: `services/order/migrations/0002_outbox_headers.sql`
- Create: `services/payment/migrations/0002_outbox_headers.sql`
- Create: `services/inventory/migrations/0002_outbox_headers.sql`

**Interfaces:**
- Consumes: `outbox.Record` carrying a `Headers map[string]string` populated by the poller (new field, see Step 1).
- Produces: `Envelope.TraceID` and `Envelope.SpanID` JSON fields populated when present.

- [ ] **Step 1: Extend `outbox.Record` with `Headers map[string]string`**

In `pkg/outbox/types.go`, add field:
```go
Headers map[string]string `json:"headers,omitempty"`
```
Write migrations `services/<svc>/migrations/0002_outbox_headers.sql`:
```sql
ALTER TABLE <svc>_outbox ADD COLUMN headers JSONB NOT NULL DEFAULT '{}'::jsonb;
```
Replace `<svc>` with `order`, `payment`, `inventory` respectively.

- [ ] **Step 2: Thread ctx into `recordToEnvelope`**

Change `pkg/outbox/kafka.go:73` signature to:
```go
func recordToEnvelope(ctx context.Context, r outbox.Record) (events.Envelope, error)
```
and inside it, after computing the Envelope struct, inject the active span:
```go
carrier := propagation.MapCarrier{}
otel.GetTextMapPropagator().Inject(ctx, carrier)
sc := trace.SpanFromContext(ctx).SpanContext()
if sc.IsValid() {
    env.TraceID = sc.TraceID().String()
    env.SpanID  = sc.SpanID().String()
}
```

- [ ] **Step 3: Update `KafkaPublisher.PublishRaw` to accept ctx and attach headers**

Signature change in `pkg/outbox/kafka.go`:
```go
type KafkaClient interface {
    PublishRaw(ctx context.Context, topic string, key, body []byte, headers map[string]string) error
}
```
In the franz-go `kgo.Produce` call, convert the headers map to `[]kgo.RecordHeader`.

- [ ] **Step 4: Failing test for Step 2**

Add to `pkg/outbox/kafka_test.go`:
```go
func TestRecordToEnvelope_PropagatesActiveSpan(t *testing.T) {
    _, span := otel.Tracer("test").Start(context.Background(), "op")
    defer span.End()
    env, err := recordToEnvelope(context.Background(), outbox.Record{})
    if err != nil { t.Fatal(err) }
    sc := span.SpanContext()
    if env.TraceID != sc.TraceID().String() { t.Errorf("trace_id mismatch: %q", env.TraceID) }
    if env.SpanID  != sc.SpanID().String()  { t.Errorf("span_id mismatch: %q", env.SpanID)  }
}
```

- [ ] **Step 5: Run new test; expect PASS**

```powershell
cd C:\Users\t0p_m\projects\orderflow\pkg\outbox
go test ./... -run TestRecordToEnvelope_PropagatesActiveSpan -v
```

- [ ] **Step 6: Update poller to attach ctx to Publish**

In `pkg/outbox/poller.go` `Run` (line 92), wrap each `pub.Publish(ctx, recs)` call so `ctx` already has a span `outbox.publish` started. The publisher will inherit it via `recordToEnvelope`.

- [ ] **Step 7: Commit**

```powershell
cd C:\Users\t0p_m\projects\orderflow
git add pkg/outbox services/order/migrations services/payment/migrations services/inventory/migrations
git commit -m "orderflow/3.10.b: propagate W3C trace_id/span_id into Envelope on publish"
```

### Task 3.10.c — Restore traceparent in consumer and create per-message span (PAR)

**Files:**
- Modify: `pkg/consumer/consumer.go` (function `dispatch`, lines 171-219)
- Modify: `pkg/consumer/consumer_test.go`

**Interfaces:**
- Consumes: envelope's JSON `trace_id`/`span_id` fields.
- Produces: ctx passed to handler that carries a span linked to the original trace.

- [ ] **Step 1: Failing test**

Append to `pkg/consumer/consumer_test.go`:
```go
func TestDispatch_RestoresSpanFromEnvelope(t *testing.T) {
    _, origSpan := otel.Tracer("test").Start(context.Background(), "producer.op")
    defer origSpan.End()
    env := events.Envelope{
        EventID: "id-1", EventType: "OrderCreated",
        AggregateID: "agg-1", AggregateType: "Order",
        TraceID: origSpan.SpanContext().TraceID().String(),
        SpanID:  origSpan.SpanContext().SpanID().String(),
        SchemaVersion: 1, Payload: json.RawMessage(`{}`),
    }
    var seenParent trace.SpanContext
    reg := HandlerRegistry{"OrderCreated": func(ctx context.Context, _ *events.Envelope) error {
        seenParent = trace.SpanFromContext(ctx).SpanContext()
        return nil
    }}
    c := &Consumer{registry: reg, maxAttempts: 1}
    rec := &kgo.Record{Value: mustEncode(env), Key: []byte("agg-1")}
    c.dispatch(context.Background(), rec)
    if seenParent.TraceID() != origSpan.SpanContext().TraceID() {
        t.Fatalf("trace_id lost across consumer boundary")
    }
}
```

- [ ] **Step 2: Implement**

In `pkg/consumer/consumer.go` `dispatch`, after deserializing the envelope into `&env`:
```go
ctx = kafkaprop.SpanFromEnvelope(ctx, env.TraceID, env.SpanID, "consumer."+env.EventType)
span := trace.SpanFromContext(ctx)
defer span.End()
span.SetAttributes(
    attribute.String("messaging.system", "kafka"),
    attribute.String("messaging.destination", env.EventType),
    attribute.String("messaging.kafka.message.key", string(rec.Key)),
)
```
Imports: `go.opentelemetry.io/otel/attribute`, `go.opentelemetry.io/otel/trace`, helper package from 3.10.a.

- [ ] **Step 3: Run consumer tests**

```powershell
cd C:\Users\t0p_m\projects\orderflow\pkg\consumer
go test ./... -v
```
Expected: existing tests still pass; new test passes.

- [ ] **Step 4: Commit**

```powershell
cd C:\Users\t0p_m\projects\orderflow
git add pkg/consumer
git commit -m "orderflow/3.10.c: restore W3C tracecontext in consumer dispatch"
```

### Task 3.10.d — Replace http.ServeMux with chi+middleware in all 4 service binaries (PAR)

**Files:**
- Modify: `services/order/cmd/order/main.go` (lines 110-135, the barebones mux block)
- Modify: `services/payment/cmd/payment/main.go` (same shape)
- Modify: `services/inventory/cmd/inventory/main.go` (same shape)
- Modify: `services/saga/cmd/saga/main.go` (currently a stub; replace stub body with the chi wiring)

**Interfaces:**
- Consumes: `platform.Middleware.Stack(serviceName, logger)` already used by REST APIs.
- Produces: every binary's `/metrics` and `/healthz` go through chi so OTel HTTP server middleware captures the request span.

- [ ] **Step 1: Failing smoke test in each cmd's main_test.go**

In each `services/<svc>/cmd/<svc>/main_test.go`, replace `TestRun_DisabledWhenNoEnv` with `TestRun_ServesHealthzAndMetrics`:
```go
func TestRun_ServesHealthzAndMetrics(t *testing.T) {
    t.Setenv("DATABASE_URL", "")
    t.Setenv("KAFKA_BROKER", "")
    t.Setenv("HTTP_ADDR", "127.0.0.1:0")
    // start Run in goroutine; resolve :0 to actual port; GET /healthz and /metrics
    // assert both 200; assert response from /healthz contains `"status":"ok"`
}
```
Use `net.Listen("tcp", addr)` + `http.Server.Serve(listener)` to capture the dynamic port; do not use `httptest`.

- [ ] **Step 2: Implement chi-based server**

In each main.go, replace the barebones `http.NewServeMux()` block with:
```go
r := chi.NewRouter()
r.Use(platform.Middleware.Stack(TableName, logger)...)
r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
    w.Header().Set("Content-Type", "application/json")
    _, _ = w.Write([]byte(`{"status":"ok"}`))
})
r.Handle("/metrics", promhttp.Handler())
```
Verify `platform.Middleware.Stack` returns chi middlewares (it does; see `pkg/platform/middleware/middleware.go:14` `Stack` method).

- [ ] **Step 3: Run all four `main_test.go` suites**

```powershell
cd C:\Users\t0p_m\projects\orderflow
go test ./services/.../cmd/... -v
```

- [ ] **Step 4: Commit per service (4 commits, one per binary)**

Pattern:
```powershell
git add services/order/cmd/order
git commit -m "orderflow/3.10.d.order: chi+middleware on /healthz and /metrics"
# repeat for payment / inventory / saga
```

### Task 3.10.e — Add service.version resource attribute (PAR)

**Files:**
- Modify: `pkg/platform/otel.go` (`InitTracing` lines 22-60)

**Interfaces:**
- Consumes: `version string` argument added to `InitTracing`.
- Produces: `service.version` resource attribute visible in Tempo.

- [ ] **Step 1: Signature change**

```go
func InitTracing(ctx context.Context, serviceName, version string) (func(context.Context) error, error)
```
Update every caller in `services/<svc>/cmd/<svc>/main.go` `startOutbox`/`Main` to pass `Version`.

- [ ] **Step 2: Resource attribute**

Add to the `resource.New` call:
```go
attrs = append(attrs, semconv.ServiceVersion(version))
```

- [ ] **Step 3: Test**

Add to `pkg/platform/otel_test.go`:
```go
func TestInitTracing_ServiceVersionPropagated(t *testing.T) {
    _, shutdown, err := platform.InitTracingForTest(context.Background(), "svc", "1.2.3")
    // assert resource attr service.version == "1.2.3"
}
```
Add `InitTracingForTest` helper that doesn't pull OTLP exporter:
```go
func InitTracingForTest(ctx context.Context, name, ver string) (context.Context, func(context.Context) error, error)
```

- [ ] **Step 4: Commit**

```powershell
git add pkg/platform services/order/cmd/order services/payment/cmd/payment services/inventory/cmd/inventory services/saga/cmd/saga
git commit -m "orderflow/3.10.e: set service.version resource attribute on every service"
```

### Task 3.10.f — Tempo service map integration check (SEQ; depends on b/c/d/e)

**Files:**
- Modify: `deploy/docker-compose.yml` (add OTLP->Tempo wiring assertion via env `OTEL_EXPORTER_OTLP_ENDPOINT`)
- Create: `tests/manual/3.10-tracecheck.md` (runbook for manual Tempo verification)

**Interfaces:**
- Consumes: services running with `OTEL_EXPORTER=otlp` and `OTEL_EXPORTER_OTLP_ENDPOINT=otel-collector:4317`.
- Produces: a Tempo service map that shows edges `order -> payment`, `payment -> inventory`, etc.

- [ ] **Step 1: Add OTLP env defaults**

Append to each `services/<svc>/cmd/<svc>/main.go` `envOrDefault` block:
```go
"OtelExporter":       envOrDefault("OTEL_EXPORTER", "otlp"),
"OtelExporterOTLPEndpoint": envOrDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317"),
```
Pass into `platform.InitTracing`.

- [ ] **Step 2: Smoke runbook**

Write `tests/manual/3.10-tracecheck.md` with these steps:
1. `docker compose -f deploy/docker-compose.yml up -d`
2. `make run-order`, `make run-payment`, `make run-inventory`, `make run-saga` (each in its own terminal)
3. `curl -X POST localhost:8081/v1/orders -d @examples/order.json`
4. Open Grafana -> Explore -> Tempo -> search by `service.name=order` -> click trace -> confirm spans for order, payment, inventory appear under one trace_id.
5. Take screenshot, save under `docs/demo/screenshots/tempo-service-map.png`.

- [ ] **Step 3: Commit**

```powershell
git add services/order/cmd/order services/payment/cmd/payment services/inventory/cmd/inventory services/saga/cmd/saga deploy/docker-compose.yml tests/manual/3.10-tracecheck.md docs/demo/screenshots
git commit -m "orderflow/3.10.f: Tempo wire-up runbook + OTLP env defaults"
```