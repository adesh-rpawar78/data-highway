// Package httpapi is the "Connector" surface: a secure REST endpoint
// that accepts JSON event payloads and hands them to the pipeline.
package httpapi

import (
	"encoding/json"
	"io"
	"log"
	"net/http"

	"datahighway/internal/model"
	"datahighway/internal/pipeline"
)

// maxBodyBytes caps request size to protect against unbounded memory use
// from a single request.
const maxBodyBytes = 1 << 20 // 1 MiB

// Server wires the HTTP routes to a Pipeline. It implements
// http.Handler, so it can be passed straight to http.Server.
type Server struct {
	pipeline *pipeline.Pipeline
	logger   *log.Logger
	mux      *http.ServeMux
}

// NewServer builds the router. apiKey secures POST /v1/events; pass ""
// to disable auth (e.g. local development).
func NewServer(p *pipeline.Pipeline, apiKey string, logger *log.Logger) *Server {
	if logger == nil {
		logger = log.Default()
	}
	s := &Server{pipeline: p, logger: logger, mux: http.NewServeMux()}

	s.mux.HandleFunc("/healthz", s.handleHealth)
	s.mux.HandleFunc("/v1/stats", s.handleStats)
	s.mux.Handle("/v1/events", requireAPIKey(apiKey, http.HandlerFunc(s.handleEvents)))

	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleStats(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.pipeline.Stats())
}

// handleEvents accepts either a single JSON event object or a JSON
// array of events (for batch-sending Connectors). Accepted events are
// handed to the pipeline asynchronously — a 202 here means "queued",
// not "persisted"; check /v1/stats or your store for confirmation.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST only"})
		return
	}

	body := http.MaxBytesReader(w, r.Body, maxBodyBytes)
	defer body.Close()

	raw, err := io.ReadAll(body)
	if err != nil {
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "request body too large"})
		return
	}

	events, err := decodeEvents(raw)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}

	accepted, rejected := 0, 0
	for _, e := range events {
		if err := s.pipeline.Submit(e); err != nil {
			rejected++
			continue
		}
		accepted++
	}

	status := http.StatusAccepted
	if accepted == 0 && rejected > 0 {
		status = http.StatusTooManyRequests
	}
	writeJSON(w, status, map[string]int{"accepted": accepted, "rejected": rejected})
}

func decodeEvents(raw []byte) ([]model.RawEvent, error) {
	if isJSONArray(raw) {
		var events []model.RawEvent
		if err := json.Unmarshal(raw, &events); err != nil {
			return nil, err
		}
		return events, nil
	}

	var single model.RawEvent
	if err := json.Unmarshal(raw, &single); err != nil {
		return nil, err
	}
	return []model.RawEvent{single}, nil
}

func isJSONArray(b []byte) bool {
	for _, c := range b {
		switch c {
		case ' ', '\t', '\n', '\r':
			continue
		case '[':
			return true
		default:
			return false
		}
	}
	return false
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
