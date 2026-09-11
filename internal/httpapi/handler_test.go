package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"datahighway/internal/pipeline"
	"datahighway/internal/store"
)

func newTestServer(apiKey string) *Server {
	st := store.NewMemoryStore(0)
	p := pipeline.New(4, 100, st, nil)
	p.Start(context.Background())
	return NewServer(p, apiKey, nil)
}

func TestHandleEvents(t *testing.T) {
	cases := []struct {
		name       string
		apiKey     string
		headerKey  string
		body       string
		wantStatus int
	}{
		{
			name:       "valid single event",
			body:       `{"source":"sensor-1","type":"temperature","payload":{"value":21.5}}`,
			wantStatus: http.StatusAccepted,
		},
		{
			name:       "valid batch",
			body:       `[{"source":"a","type":"t","payload":{"v":1}},{"source":"b","type":"t","payload":{"v":2}}]`,
			wantStatus: http.StatusAccepted,
		},
		{
			name:       "malformed json",
			body:       `{not json`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing required field is still queued (validated async)",
			body:       `{"type":"temperature","payload":{"value":1}}`,
			wantStatus: http.StatusAccepted,
		},
		{
			name:       "wrong api key",
			apiKey:     "secret",
			headerKey:  "wrong",
			body:       `{"source":"a","type":"t","payload":{"v":1}}`,
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "missing api key header",
			apiKey:     "secret",
			body:       `{"source":"a","type":"t","payload":{"v":1}}`,
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "correct api key",
			apiKey:     "secret",
			headerKey:  "secret",
			body:       `{"source":"a","type":"t","payload":{"v":1}}`,
			wantStatus: http.StatusAccepted,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			srv := newTestServer(tc.apiKey)

			req := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewBufferString(tc.body))
			if tc.headerKey != "" {
				req.Header.Set("X-API-Key", tc.headerKey)
			}
			rec := httptest.NewRecorder()

			srv.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", rec.Code, tc.wantStatus, rec.Body.String())
			}
		})
	}
}

func TestHandleEvents_QueueFullReturns429(t *testing.T) {
	st := store.NewMemoryStore(0)
	// Tiny queue, no Start(): every submission after the buffer fills
	// should be rejected, deterministically.
	p := pipeline.New(1, 1, st, nil)
	srv := NewServer(p, "", nil)

	body := `{"source":"a","type":"t","payload":{"v":1}}`

	// First request fills the single-slot queue.
	req := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("first request status = %d, want 202", rec.Code)
	}

	// Second request should find the queue full.
	req = httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewBufferString(body))
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("second request status = %d, want 429 (body: %s)", rec.Code, rec.Body.String())
	}
}

func TestHandleHealthAndStats(t *testing.T) {
	srv := newTestServer("")

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("/healthz status = %d, want 200", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/v1/stats", nil)
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("/v1/stats status = %d, want 200", rec.Code)
	}
	var stats pipeline.Stats
	if err := json.NewDecoder(rec.Body).Decode(&stats); err != nil {
		t.Fatalf("decode stats: %v", err)
	}
}

func TestHandleEvents_MethodNotAllowed(t *testing.T) {
	srv := newTestServer("")
	req := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}
