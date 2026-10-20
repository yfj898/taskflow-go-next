package telemetry

import (
	"context"
	"log"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type logExporter struct{}

func (logExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	for _, span := range spans {
		log.Printf(
			"otel trace_id=%s span_id=%s name=%q duration=%s status=%s",
			span.SpanContext().TraceID(),
			span.SpanContext().SpanID(),
			span.Name(),
			span.EndTime().Sub(span.StartTime()).Round(time.Microsecond),
			span.Status().Code.String(),
		)
	}
	return nil
}

func (logExporter) Shutdown(context.Context) error { return nil }

func Init(ctx context.Context, serviceName string, logSpans bool) (func(context.Context) error, error) {
	res, err := resource.New(ctx, resource.WithAttributes(attribute.String("service.name", serviceName)))
	if err != nil {
		return nil, err
	}

	// Do not pay the allocation/recording cost for spans when no exporter is
	// configured. Trace context propagation still works, while enabling the log
	// exporter restores full recording for local debugging.
	sampler := sdktrace.ParentBased(sdktrace.NeverSample())
	if logSpans {
		sampler = sdktrace.ParentBased(sdktrace.AlwaysSample())
	}

	opts := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	}
	if logSpans {
		opts = append(opts, sdktrace.WithBatcher(logExporter{}))
	}

	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	return tp.Shutdown, nil
}
