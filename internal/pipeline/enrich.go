package pipeline

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"datahighway/internal/model"
)

var (
	ErrInvalidSource = errors.New("source is required")
	ErrInvalidType   = errors.New("type is required")
	ErrEmptyPayload  = errors.New("payload must not be empty")
)

var idCounter uint64

// nextID returns a unique-enough ID for demo/production-lite use:
// nanosecond timestamp plus a monotonically increasing counter, so two
// events generated in the same nanosecond still get distinct IDs.
func nextID() string {
	n := atomic.AddUint64(&idCounter, 1)
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), n)
}

// validate checks the required fields on a raw event.
func validate(raw model.RawEvent) error {
	if strings.TrimSpace(raw.Source) == "" {
		return ErrInvalidSource
	}
	if strings.TrimSpace(raw.Type) == "" {
		return ErrInvalidType
	}
	if len(raw.Payload) == 0 {
		return ErrEmptyPayload
	}
	return nil
}

// enrich turns a validated RawEvent into a fully-formed Event: it
// assigns an ID, resolves the event's occurrence time (falling back to
// receivedAt if the source didn't send one or sent a bad one), and
// cleans the payload (trims keys/string values, drops blank keys).
func enrich(raw model.RawEvent, receivedAt time.Time) model.Event {
	occurred := receivedAt
	if raw.Timestamp != "" {
		if t, err := time.Parse(time.RFC3339, raw.Timestamp); err == nil {
			occurred = t
		}
	}

	cleaned := make(map[string]interface{}, len(raw.Payload))
	for k, v := range raw.Payload {
		key := strings.TrimSpace(k)
		if key == "" {
			continue
		}
		if s, ok := v.(string); ok {
			v = strings.TrimSpace(s)
		}
		cleaned[key] = v
	}

	return model.Event{
		ID:         nextID(),
		Source:     strings.TrimSpace(raw.Source),
		Type:       strings.TrimSpace(raw.Type),
		Payload:    cleaned,
		OccurredAt: occurred,
		ReceivedAt: receivedAt,
	}
}
