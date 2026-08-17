// Package platform — OpenTelemetry tracing setup.
package platform

import (
	"context"
	"fmt"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	oteltracer "go.opentelemetry.io/otel/trace"
)

// testResourceCtxKey is the private context key used to attach the
// resource built by InitTracingForTest to the returned context so
// tests can introspect service.name/service.version without the
// OTLP exporter.
type testResourceCtxKey struct{}

// InitTracing sets up an OTLP gRPC exporter and returns a shutdown
// func. The version is attached as the service.version resource
// attribute.
//
// Exporter selection via env vars (read by createExporter):
//
//	OTEL_EXPORTER            controls which exporter to use. Accepts
//	                         "stdout" (dev/local) or "otlp" (prod);
//	                         any non-"stdout" value selects OTLP gRPC.
//	OTEL_EXPORTER_OTLP_ENDPOINT
//	                         OTLP gRPC endpoint. Falls back to
//	                         "localhost:4317" when unset.
//
// The service binaries under services/<svc>/cmd/<svc>/main.go set
// process-level defaults ("otlp", "otel-collector:4317") before
// calling InitTracing so that the docker-compose-based deployment
// (which exposes the collector at otel-collector:4317) works out of
// the box; set OTEL_EXPORTER=stdout in your shell to redirect spans
// to stdout for local dev without docker-compose.
func InitTracing(ctx context.Context, serviceName, version string) (func(context.Context) error, error) {
	exporter, err := createExporter(ctx)
	if err != nil {
		return nil, fmt.Errorf("otel: create exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(version),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("otel: resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tp.Shutdown, nil
}

// InitTracingForTest sets up an SDK TracerProvider with the given
// service name and version resource attributes, installs it
// globally, and attaches the resource to the returned context so
// tests can introspect it. It does NOT wire the OTLP exporter (so
// unit tests don't need network access).
func InitTracingForTest(ctx context.Context, name, ver string) (context.Context, func(context.Context) error, error) {
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(name),
			semconv.ServiceVersion(ver),
		),
	)
	if err != nil {
		return ctx, nil, fmt.Errorf("otel: resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return context.WithValue(ctx, testResourceCtxKey{}, res), tp.Shutdown, nil
}

func createExporter(ctx context.Context) (sdktrace.SpanExporter, error) {
	if os.Getenv("OTEL_EXPORTER") == "stdout" {
		return stdouttrace.New(stdouttrace.WithPrettyPrint())
	}
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		endpoint = "localhost:4317"
	}
	return otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
}

// Tracer returns the package-level tracer.
func Tracer(name string) oteltracer.Tracer {
	return otel.Tracer(name)
}
