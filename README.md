# Data Highway

A lightweight, concurrent event-ingestion pipeline in Go. A secure REST
**Connector** endpoint accepts JSON events; the **Data Highway** (a
channel-backed worker pool) validates, enriches, and persists them
concurrently with explicit backpressure instead of unbounded memory
growth.

```
Connector (REST)  --Submit-->  bounded channel  --workers-->  validate/enrich  --Save-->  Store
   handleEvents()                (queue)          (goroutines)                 (mutex-protected)
```

## Project layout

```
cmd/server/main.go          entry point, config, graceful shutdown
internal/model/event.go     RawEvent (inbound) / Event (enriched) types
internal/pipeline/          worker pool: channels, WaitGroup, atomics
  pipeline.go                Pipeline: New/Start/Submit/Shutdown/Stats
  enrich.go                  validate() + enrich() (pure functions, easy to test)
  pipeline_test.go           table-driven unit tests
  pipeline_bench_test.go     throughput + hot-path benchmarks
internal/store/             persistence
  store.go                   Store interface + MemoryStore (mutex, default)
  postgres.go                 optional Postgres store (build tag "postgres")
internal/httpapi/           the Connector: REST endpoint + middleware
  handler.go                  routes, JSON decode (single or batch), 202/429/400
  middleware.go                constant-time API key check
  handler_test.go              table-driven HTTP tests
```

No external dependencies are required for the default build — everything
except the optional Postgres store uses only the Go standard library.

## Requirements

- Go 1.22+

## Run it

```bash
go run ./cmd/server
```

Environment variables (all optional, shown with defaults):

| Var                | Default | Meaning                                      |
|--------------------|---------|-----------------------------------------------|
| `PORT`             | 8080    | HTTP listen port                              |
| `WORKERS`          | 64      | Concurrent worker goroutines                  |
| `QUEUE_SIZE`       | 10000   | Buffered channel depth before backpressure    |
| `API_KEY`          | (empty) | If set, required via `X-API-Key` header       |
| `MEMORY_STORE_CAP` | 100000  | Max events kept in memory (0 = unbounded)     |

```bash
PORT=9090 WORKERS=128 API_KEY=changeme go run ./cmd/server
```

## Try the API

```bash
# health check
curl -s localhost:8080/healthz

# submit a single event
curl -s -X POST localhost:8080/v1/events \
  -H "Content-Type: application/json" \
  -H "X-API-Key: changeme" \
  -d '{"source":"sensor-1","type":"temperature","payload":{"value":21.5}}'

# submit a batch
curl -s -X POST localhost:8080/v1/events \
  -H "Content-Type: application/json" \
  -H "X-API-Key: changeme" \
  -d '[{"source":"a","type":"metric","payload":{"v":1}},
       {"source":"b","type":"metric","payload":{"v":2}}]'

# check counters
curl -s localhost:8080/v1/stats
```

A `202 Accepted` means the event was queued, not yet persisted — check
`/v1/stats` (`processed` count) for confirmation. A `429 Too Many
Requests` means the ingestion queue is momentarily full; retry with
backoff.

## Run the tests

```bash
go test ./...            # all unit tests
go test ./... -v -race   # verbose, with the race detector
make test                # same as above, via Makefile
```

## Run the benchmarks

```bash
go test ./internal/pipeline/... -bench=. -benchmem -run=^$
# or:
make bench
```

- `BenchmarkPipeline_Throughput` — full path: Submit → validate → enrich → Save.
- `BenchmarkValidateAndEnrich` — isolates the CPU-bound hot path (no channels/store).

## Proving 10,000+ req/s

The pipeline itself is benchmarked above. To load-test the HTTP layer
end to end, use a tool like [`hey`](https://github.com/rakyll/hey) or
`wrk`:

```bash
go run ./cmd/server &   # or: make run

hey -z 15s -c 200 -m POST \
  -H "Content-Type: application/json" \
  -H "X-API-Key: changeme" \
  -d '{"source":"loadtest","type":"metric","payload":{"v":1}}' \
  http://localhost:8080/v1/events

curl -s localhost:8080/v1/stats
```

Tune `WORKERS` and `QUEUE_SIZE` for your hardware — more workers if
processing is CPU-light and I/O-bound (e.g. writing to a real
database), a bigger queue to absorb short traffic bursts.

## Swapping in Postgres

`MemoryStore` is the default so the project runs with zero setup. To
persist to Postgres instead:

```bash
go get github.com/jackc/pgx/v5/pgxpool
go build -tags postgres ./...
```

Then in `main.go`, replace `store.NewMemoryStore(memCap)` with:

```go
st, err := store.NewPostgresStore(context.Background(), os.Getenv("DATABASE_URL"))
if err != nil {
    logger.Fatalf("connect to postgres: %v", err)
}
defer st.Close()
```

The table schema is documented at the top of `internal/store/postgres.go`.
A `docker-compose.yml` Postgres profile is included:

```bash
docker compose --profile pg up -d postgres
```

## Design notes

- **Backpressure, not unbounded growth.** `Submit` is a non-blocking
  channel send (`select`/`default`); once the buffered channel is full
  it returns `ErrQueueFull` immediately (surfaced as HTTP 429) instead
  of letting memory grow without limit.
- **Graceful shutdown.** On `SIGINT`/`SIGTERM`, the HTTP server stops
  accepting new connections, the job channel is closed, and all
  buffered + in-flight events are drained before the process exits.
- **Concurrency-safety.** Counters use `sync/atomic` (cheap, lock-free);
  `MemoryStore` uses a `sync.Mutex` to protect its slice, since a real
  store's client library (a DB pool, a Redis client) would need similar
  protection or connection pooling.
- **Async accept, sync confirmation via stats.** The HTTP handler
  returns as soon as an event is queued, keeping the request path fast;
  `/v1/stats` exposes `received`/`processed`/`failed` counters if a
  caller needs to confirm processing.
