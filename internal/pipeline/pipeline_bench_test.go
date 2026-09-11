package pipeline

import (
	"context"
	"runtime"
	"testing"
	"time"

	"datahighway/internal/model"
	"datahighway/internal/store"
)

// BenchmarkPipeline_Throughput measures end-to-end Submit -> validate ->
// enrich -> store.Save throughput with a realistic worker pool.
//
// Run: go test ./internal/pipeline/... -bench=Throughput -benchmem
func BenchmarkPipeline_Throughput(b *testing.B) {
	st := store.NewMemoryStore(0)
	p := New(64, 10000, st, nil)
	p.Start(context.Background())
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.Shutdown(ctx)
	}()

	raw := model.RawEvent{
		Source:  "bench-sensor",
		Type:    "metric",
		Payload: map[string]interface{}{"value": 1.23, "unit": "ms"},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for p.Submit(raw) != nil {
			runtime.Gosched() // queue momentarily full; let workers catch up
		}
	}
}

// BenchmarkValidateAndEnrich isolates the CPU-bound part of processing
// (no channels, no store) to show the per-event cost of the hot path.
//
// Run: go test ./internal/pipeline/... -bench=ValidateAndEnrich -benchmem
func BenchmarkValidateAndEnrich(b *testing.B) {
	raw := model.RawEvent{
		Source:  "bench-sensor",
		Type:    "metric",
		Payload: map[string]interface{}{"value": 1.23, "unit": "ms"},
	}
	now := time.Now()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := validate(raw); err != nil {
			b.Fatal(err)
		}
		_ = enrich(raw, now)
	}
}
