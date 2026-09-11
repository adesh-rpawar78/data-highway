// Package pipeline is the "Data Highway": a bounded-channel worker pool
// that takes raw events off an ingestion queue, validates and enriches
// them, and writes them to a Store — concurrently, with explicit
// backpressure instead of unbounded memory growth.
package pipeline

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"datahighway/internal/model"
	"datahighway/internal/store"
)

// ErrQueueFull is returned by Submit when the ingestion buffer is
// saturated. Treat it as backpressure — e.g. an HTTP handler should
// return 429 Too Many Requests rather than blocking the caller.
var ErrQueueFull = errors.New("ingestion queue is full")

// Stats is a point-in-time snapshot of pipeline counters.
type Stats struct {
	Received  uint64 `json:"received"`
	Processed uint64 `json:"processed"`
	Failed    uint64 `json:"failed"`
}

// Pipeline is a concurrent worker pool. Create one with New, launch
// workers with Start, feed it with Submit, and drain it with Shutdown.
type Pipeline struct {
	jobs    chan model.RawEvent
	store   store.Store
	workers int
	wg      sync.WaitGroup
	logger  *log.Logger

	received  uint64
	processed uint64
	failed    uint64

	closeOnce sync.Once
}

// New creates a Pipeline with the given worker count and queue depth.
// workers controls how many goroutines process events concurrently;
// queueSize bounds how many events can wait in the channel before
// Submit starts returning ErrQueueFull.
func New(workers, queueSize int, s store.Store, logger *log.Logger) *Pipeline {
	if workers < 1 {
		workers = 1
	}
	if queueSize < 1 {
		queueSize = 1
	}
	if logger == nil {
		logger = log.Default()
	}
	return &Pipeline{
		jobs:    make(chan model.RawEvent, queueSize),
		store:   s,
		workers: workers,
		logger:  logger,
	}
}

// Start launches the worker goroutines. Safe to call once per Pipeline.
func (p *Pipeline) Start(ctx context.Context) {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.worker(ctx)
	}
}

// Submit enqueues a raw event without blocking the caller. It returns
// ErrQueueFull immediately if the buffer is full — callers get explicit
// backpressure instead of the queue growing without bound.
func (p *Pipeline) Submit(raw model.RawEvent) error {
	atomic.AddUint64(&p.received, 1)
	select {
	case p.jobs <- raw:
		return nil
	default:
		atomic.AddUint64(&p.failed, 1)
		return ErrQueueFull
	}
}

func (p *Pipeline) worker(ctx context.Context) {
	defer p.wg.Done()
	for {
		select {
		case raw, ok := <-p.jobs:
			if !ok {
				return // channel closed and drained: Shutdown was called
			}
			p.process(ctx, raw)
		case <-ctx.Done():
			p.drain(ctx)
			return
		}
	}
}

// drain flushes any events already buffered in the channel before the
// worker exits, so a cancelled context doesn't silently drop accepted
// work.
func (p *Pipeline) drain(ctx context.Context) {
	for {
		select {
		case raw, ok := <-p.jobs:
			if !ok {
				return
			}
			p.process(ctx, raw)
		default:
			return
		}
	}
}

func (p *Pipeline) process(ctx context.Context, raw model.RawEvent) {
	receivedAt := time.Now().UTC()

	if err := validate(raw); err != nil {
		atomic.AddUint64(&p.failed, 1)
		p.logger.Printf("dropped invalid event from %q: %v", raw.Source, err)
		return
	}

	evt := enrich(raw, receivedAt)
	evt.ProcessedAt = time.Now().UTC()

	storeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	if err := p.store.Save(storeCtx, evt); err != nil {
		atomic.AddUint64(&p.failed, 1)
		p.logger.Printf("failed to persist event %s: %v", evt.ID, err)
		return
	}
	atomic.AddUint64(&p.processed, 1)
}

// Stats returns a snapshot of the running counters. Safe to call
// concurrently with Submit and while workers are running.
func (p *Pipeline) Stats() Stats {
	return Stats{
		Received:  atomic.LoadUint64(&p.received),
		Processed: atomic.LoadUint64(&p.processed),
		Failed:    atomic.LoadUint64(&p.failed),
	}
}

// Shutdown closes the ingestion queue and waits for buffered and
// in-flight work to finish, or until ctx is done, whichever comes
// first. Call it once.
func (p *Pipeline) Shutdown(ctx context.Context) error {
	p.closeOnce.Do(func() {
		close(p.jobs)
	})

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
