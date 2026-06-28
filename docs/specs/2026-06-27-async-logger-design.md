# Async batch logger + transport politeness

Status: approved (design)
Date: 2026-06-27

Add a buffered, fire-and-forget logging path for the MLflow training hot path
(thousands of tiny metric/param/tag writes), and close the one good-neighbor gap
in the request path that async would otherwise amplify.

## Part 1 — transport politeness (prereq)

`doRequest` (client.go) retries 5xx and transport errors but returns 429
immediately and ignores `Retry-After`. A polite client must not do that, and
async multiplies the request rate, so this lands first.

Changes, all inside the existing retry loop in `doRequest`:

- Retry on **429** and **503** in addition to 5xx and transport errors. (429 is
  not 5xx today, so it currently falls through to immediate return.)
- When the retryable response carries a `Retry-After` header, sleep for that
  duration instead of the computed exponential backoff. Support both forms:
  - delta-seconds (e.g. `Retry-After: 5`)
  - HTTP-date (e.g. `Retry-After: Wed, 21 Oct 2025 07:28:00 GMT`); sleep until
    that instant, clamped to >= 0.
- The wait still races `ctx.Done()` exactly as the current backoff does.
- `cfg.maxRetries` budget unchanged; 4xx other than 429 still returns immediately.

No API surface change. Existing retry tests extend to cover 429 + Retry-After
(both header forms) and 503.

## Part 2 — AsyncLogger

Logging-only wrapper over `*Client`. Covers the buffered write path only; every
other call stays on `Client`.

### Surface

```go
func (c *Client) NewAsyncLogger(opts ...AsyncOption) *AsyncLogger

type AsyncOption func(*asyncConfig)

WithBufferSize(n int)           // channel capacity, default 8192
WithBatchSize(n int)            // max records per flush, default 1000 (API max); clamped to [1,1000]
WithErrorHandler(func(error))   // invoked per failed flush; errors also aggregated for Close

func (a *AsyncLogger) LogMetric(ctx context.Context, runID, key string, value float64, tsMs, step int64) error
func (a *AsyncLogger) LogParam(ctx context.Context, runID, key, value string) error
func (a *AsyncLogger) SetTag(ctx context.Context, runID, key, value string) error
func (a *AsyncLogger) Flush(ctx context.Context) error  // force-flush all buckets, wait for completion
func (a *AsyncLogger) Close() error                     // flush remainder, stop worker, return aggregate error
```

`Log*` returns an error only when the buffer is full and `ctx` is canceled while
blocked, or when called after `Close`. Flush failures never surface through
`Log*`; they go to the error handler and the Close aggregate.

### Contract

- **Backpressure:** bounded. A full buffer blocks the caller (racing `ctx.Done()`
  and a closed signal). Buffer is sized, and the worker drains greedily, so under
  normal load the channel stays near-empty and callers rarely block. Block is the
  safety valve, not the steady state. No data is dropped.
- **Error surfacing:** per-failed-flush callback via `WithErrorHandler`, plus an
  aggregate error returned by `Close`.
- **Batching:** keyed per run_id, since `runs/log-batch` is per-run. Real batch
  size tracks traffic via the greedy drain; `WithBatchSize` caps it.

### Internals

One buffered channel `chan record`; one worker goroutine. The worker never waits
on a clock. It is either blocked receiving the first record (nothing to do) or
actively draining and flushing.

```
for {
    rec, ok := <-records          // block for the first record
    if !ok { flushAll(); return } // channel closed by Close
    add(rec)
    for {                         // greedy non-blocking drain of whatever is queued
        select {
        case rec, ok := <-records:
            if !ok { flushAll(); return }
            add(rec)
            if bucketAtBatchSize { flush(bucket) }   // size trigger
        default:
            goto caughtUp
        }
    }
caughtUp:
    flushAll()                    // caught up, channel empty: send what we have
}
```

Two flush triggers, no timer:
1. a per-run bucket reaches `WithBatchSize` (default = API limit), or
2. the worker drains the channel empty.

Under sustained load the channel backs up, the drain coalesces into large
batches, and trigger (1) dominates. Idle, trigger (2) flushes immediately. No
empty ticks.

- `Log*` builds a `record{runID, kind, metric|param|tag}` and sends it on the
  channel via `select { case ch <- rec; case <-ctx.Done(); case <-closed }`.
- Flushing calls existing `Client.LogBatch(ctx, runID, metrics, params, tags)`,
  which already chunks to the API limits. The worker uses a logger-lifetime
  context (created at `New`, canceled after the final flush in `Close`), not the
  caller's per-call ctx.
- A failed `LogBatch` calls the error handler and appends to the aggregate.
- `Flush(ctx)`: signal the worker to flush all buckets now and wait for ack.
- `Close()`: close the channel, the worker drains and flushes the remainder,
  then exits; `Close` returns the aggregate of all flush errors. Idempotent.

### Deliberately skipped (mark with `ponytail:` in code)

- Multiple flush workers. One worker plus 1000-record batches handles high
  throughput. Add `WithFlushWorkers` only if a real run saturates one. Note: per-
  run metric ordering is not load-bearing (each metric carries its own
  timestamp+step), so parallel flushes are safe when that day comes.
- Shutdown-timeout knob on `Close`.
- Drop-on-full policy. Chosen contract is block, never drop.

## Testing

- Part 1: extend the transport retry tests. 429 then 200 succeeds; 429 with
  `Retry-After: <seconds>` waits ~that long; `Retry-After: <http-date>` waits to
  the instant; 503 retried; ctx cancel during a Retry-After wait returns
  `ctx.Err()`; non-429 4xx still returns immediately.
- Part 2 (fake transport, no server):
  - many `LogMetric` calls flush as `LogBatch` chunks that respect API limits.
  - greedy drain coalesces a burst into fewer, larger batches than one-per-call.
  - per-run bucketing: interleaved run_ids produce one batch per run.
  - full buffer blocks, then unblocks as the worker drains; canceled ctx while
    blocked returns `ctx.Err()`.
  - a flush error reaches the error handler and is included in the `Close`
    aggregate.
  - `Close` flushes the remainder and is safe to call twice.
  - `Log*` after `Close` returns an error.
