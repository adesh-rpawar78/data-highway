package pipeline

import (
	"context"
	"testing"
	"time"

	"datahighway/internal/model"
	"datahighway/internal/store"
)

func TestValidate(t *testing.T) {
	cases := []struct {
		name    string
		raw     model.RawEvent
		wantErr error
	}{
		{
			name: "valid event",
			raw: model.RawEvent{
				Source:  "sensor-1",
				Type:    "temperature",
				Payload: map[string]interface{}{"value": 21.5},
			},
			wantErr: nil,
		},
		{
			name:    "missing source",
			raw:     model.RawEvent{Type: "temperature", Payload: map[string]interface{}{"value": 1}},
			wantErr: ErrInvalidSource,
		},
		{
			name:    "blank source",
			raw:     model.RawEvent{Source: "   ", Type: "temperature", Payload: map[string]interface{}{"value": 1}},
			wantErr: ErrInvalidSource,
		},
		{
			name:    "missing type",
			raw:     model.RawEvent{Source: "sensor-1", Payload: map[string]interface{}{"value": 1}},
			wantErr: ErrInvalidType,
		},
		{
			name:    "empty payload",
			raw:     model.RawEvent{Source: "sensor-1", Type: "temperature", Payload: map[string]interface{}{}},
			wantErr: ErrEmptyPayload,
		},
		{
			name:    "nil payload",
			raw:     model.RawEvent{Source: "sensor-1", Type: "temperature"},
			wantErr: ErrEmptyPayload,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if err := validate(tc.raw); err != tc.wantErr {
				t.Fatalf("validate(%+v) = %v, want %v", tc.raw, err, tc.wantErr)
			}
		})
	}
}

func TestEnrich(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	t.Run("uses provided timestamp when valid", func(t *testing.T) {
		raw := model.RawEvent{
			Source:    "  sensor-1  ",
			Type:      "temperature",
			Timestamp: "2025-01-01T00:00:00Z",
			Payload:   map[string]interface{}{" value ": " 21.5 "},
		}
		evt := enrich(raw, now)

		want, _ := time.Parse(time.RFC3339, "2025-01-01T00:00:00Z")
		if !evt.OccurredAt.Equal(want) {
			t.Errorf("OccurredAt = %v, want %v", evt.OccurredAt, want)
		}
		if evt.Source != "sensor-1" {
			t.Errorf("Source = %q, want trimmed %q", evt.Source, "sensor-1")
		}
		if evt.Payload["value"] != "21.5" {
			t.Errorf("Payload[value] = %v, want trimmed string %q", evt.Payload["value"], "21.5")
		}
		if evt.ID == "" {
			t.Error("expected non-empty ID")
		}
	})

	t.Run("falls back to received time on bad timestamp", func(t *testing.T) {
		raw := model.RawEvent{
			Source:    "sensor-1",
			Type:      "temperature",
			Timestamp: "not-a-timestamp",
			Payload:   map[string]interface{}{"value": 1},
		}
		evt := enrich(raw, now)
		if !evt.OccurredAt.Equal(now) {
			t.Errorf("OccurredAt = %v, want fallback %v", evt.OccurredAt, now)
		}
	})

	t.Run("drops blank payload keys", func(t *testing.T) {
		raw := model.RawEvent{
			Source: "sensor-1",
			Type:   "temperature",
			Payload: map[string]interface{}{
				"":      "should be dropped",
				"value": 1,
			},
		}
		evt := enrich(raw, now)
		if _, ok := evt.Payload[""]; ok {
			t.Error("expected blank key to be dropped")
		}
		if len(evt.Payload) != 1 {
			t.Errorf("Payload len = %d, want 1", len(evt.Payload))
		}
	})

	t.Run("two events in the same instant get distinct IDs", func(t *testing.T) {
		raw := model.RawEvent{Source: "s", Type: "t", Payload: map[string]interface{}{"a": 1}}
		e1 := enrich(raw, now)
		e2 := enrich(raw, now)
		if e1.ID == e2.ID {
			t.Errorf("expected distinct IDs, got %q twice", e1.ID)
		}
	})
}

func TestPipeline_EndToEnd(t *testing.T) {
	st := store.NewMemoryStore(0)
	p := New(4, 300, st, nil)
	p.Start(context.Background())

	const n = 200
	for i := 0; i < n; i++ {
		raw := model.RawEvent{
			Source:  "sensor-1",
			Type:    "temperature",
			Payload: map[string]interface{}{"value": i},
		}
		if err := p.Submit(raw); err != nil {
			t.Fatalf("Submit() failed at i=%d: %v", i, err)
		}
	}

	// One malformed event mixed in should be rejected without affecting
	// the rest of the batch or crashing a worker.
	_ = p.Submit(model.RawEvent{Type: "x", Payload: map[string]interface{}{"a": 1}})

	deadline := time.After(2 * time.Second)
	for st.Count() < n {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for events to be processed, got %d/%d", st.Count(), n)
		case <-time.After(10 * time.Millisecond):
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := p.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown() error: %v", err)
	}

	stats := p.Stats()
	if stats.Processed != n {
		t.Errorf("Processed = %d, want %d", stats.Processed, n)
	}
	if stats.Failed != 1 {
		t.Errorf("Failed = %d, want 1", stats.Failed)
	}
	if stats.Received != n+1 {
		t.Errorf("Received = %d, want %d", stats.Received, n+1)
	}
}

func TestPipeline_BackpressureWhenQueueFull(t *testing.T) {
	st := store.NewMemoryStore(0)
	// Deliberately not calling Start(): nothing drains the queue, so it
	// fills up deterministically after exactly queueSize submissions.
	p := New(1, 2, st, nil)

	raw := model.RawEvent{Source: "s", Type: "t", Payload: map[string]interface{}{"a": 1}}
	for i := 0; i < 2; i++ {
		if err := p.Submit(raw); err != nil {
			t.Fatalf("Submit() unexpected error filling queue (i=%d): %v", i, err)
		}
	}
	if err := p.Submit(raw); err != ErrQueueFull {
		t.Fatalf("Submit() on full queue = %v, want ErrQueueFull", err)
	}
}

func TestPipeline_ShutdownIsIdempotentSafe(t *testing.T) {
	st := store.NewMemoryStore(0)
	p := New(2, 10, st, nil)
	p.Start(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("first Shutdown() error: %v", err)
	}
	// Calling Shutdown again must not panic (close of closed channel).
	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("second Shutdown() error: %v", err)
	}
}
