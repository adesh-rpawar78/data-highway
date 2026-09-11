// Package model defines the data shapes that flow through the Data
// Highway: RawEvent (as received from a Connector) and Event (after
// validation and enrichment).
package model

import "time"

// RawEvent is the untrusted JSON payload accepted from a Connector.
// Timestamp is optional and, if present, must be RFC3339
// (e.g. "2026-01-02T15:04:05Z"); an invalid or missing value falls back
// to the server's receive time during enrichment.
type RawEvent struct {
	Source    string                 `json:"source"`
	Type      string                 `json:"type"`
	Timestamp string                 `json:"timestamp,omitempty"`
	Payload   map[string]interface{} `json:"payload"`
}

// Event is a validated, enriched RawEvent ready to be persisted.
type Event struct {
	ID          string                 `json:"id"`
	Source      string                 `json:"source"`
	Type        string                 `json:"type"`
	Payload     map[string]interface{} `json:"payload"`
	OccurredAt  time.Time              `json:"occurred_at"`  // when the event happened (from the Connector, or fallback)
	ReceivedAt  time.Time              `json:"received_at"`  // when the Data Highway accepted it
	ProcessedAt time.Time              `json:"processed_at"` // when a worker finished enriching it
}
