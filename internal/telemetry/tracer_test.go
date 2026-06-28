package telemetry

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
)

func TestTracerShutdownHang(t *testing.T) {
	ctx := context.Background()
	// Use a silent unroutable IP address to force a network connection timeout
	shutdown, err := InitTracer(ctx, "test-service", "http://192.0.2.1:4318")
	if err != nil {
		t.Fatalf("failed to init tracer: %v", err)
	}

	// Generate a span so that the batch exporter has spans to upload
	tr := otel.Tracer("test-tracer")
	_, span := tr.Start(ctx, "test-span")
	span.End()

	start := time.Now()
	shutdown()
	duration := time.Since(start)

	t.Logf("Tracer shutdown took: %v", duration)
	// If it takes longer than 250ms, it indicates it is blocking on the full default 30s connection timeout
	if duration > 250*time.Millisecond {
		t.Errorf("FRICTION: Tracer shutdown blocked for too long: %v (expected < 250ms)", duration)
	}
}
