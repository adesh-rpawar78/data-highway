// Package store defines the persistence boundary for processed events.
// MemoryStore (this file) is the default, dependency-free implementation
// used for local dev, tests, and demos. See postgres.go for an optional
// Postgres-backed implementation you can opt into for real persistence.
package store

import (
	"context"
	"sync"

	"datahighway/internal/model"
)

// Store persists processed events. Implementations must be safe for
// concurrent use — the pipeline calls Save from many worker goroutines
// at once.
type Store interface {
	Save(ctx context.Context, e model.Event) error
	Count() int
	// All returns a copy of stored events. Intended for tests and the
	// debug/demo path — a real production store would offer query
	// methods instead of a full dump.
	All() []model.Event
}

// MemoryStore is a mutex-protected, in-memory Store with an optional cap
// on retained events so long-running demos don't grow without bound.
type MemoryStore struct {
	mu     sync.Mutex
	events []model.Event
	maxLen int
}

// NewMemoryStore creates a MemoryStore. maxLen caps how many of the most
// recent events are retained; pass 0 for unbounded (fine for tests and
// short-lived demos, not for long-running production use).
func NewMemoryStore(maxLen int) *MemoryStore {
	return &MemoryStore{maxLen: maxLen}
}

func (m *MemoryStore) Save(ctx context.Context, e model.Event) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.events = append(m.events, e)
	if m.maxLen > 0 && len(m.events) > m.maxLen {
		overflow := len(m.events) - m.maxLen
		m.events = m.events[overflow:] // drop oldest to bound memory
	}
	return nil
}

func (m *MemoryStore) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.events)
}

func (m *MemoryStore) All() []model.Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]model.Event, len(m.events))
	copy(out, m.events)
	return out
}
