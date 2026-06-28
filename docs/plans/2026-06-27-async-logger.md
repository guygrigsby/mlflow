# Async Batch Logger + Transport Politeness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a buffered, fire-and-forget `AsyncLogger` for the MLflow training write path, and make the request retry path honor 429/503 + `Retry-After`.

**Architecture:** Part 1 extends the existing `doRequest` retry loop in `client.go` to retry 429/503 and respect `Retry-After`. Part 2 adds `async.go`: a per-run buffering logger over `*Client.LogBatch`, driven by one worker goroutine that blocks on a channel, greedy-drains, and flushes when it catches up or a per-run bucket hits the batch cap. No timer anywhere.

**Tech Stack:** Go 1.23, stdlib only (`context`, `sync`, `errors`, `errors.Join`, `net/http`, `strconv`, `time`). No new dependencies.

## Global Constraints

- Module `github.com/guygrigsby/mlflow`, Go 1.23. No new dependencies (stdlib only).
- Test seams: `fakeClient(fn)` (fakes `Client.do`, in `testhelpers_test.go`) for logger tests; `httptest.NewServer` for transport tests. Follow these, don't invent new ones.
- `runs/log-batch` API limits (already enforced by `Client.LogBatch`): ≤1000 metrics, ≤100 params, ≤100 tags, ≤1000 total per call. The logger's `WithBatchSize` caps total records per flush and is clamped to `[1, 1000]`.
- Commit style: terse, verb-first, no dashes, keep the repo prefix scheme (`feat:`, `test:`, `docs:`). No Claude/Anthropic attribution or co-author trailers anywhere.
- Each task ends green (`go test ./...`) and gets its own commit.

---

## File Structure

- `client.go` (modify): add `parseRetryAfter`, extend `doRequest` retry loop.
- `client_more_test.go` (modify) or new `retry_test.go`: transport retry tests. Use the existing retry-test file `client_more_test.go` to keep retry tests together.
- `async.go` (create): `AsyncLogger`, `AsyncOption`, `record`, `bucket`, worker loop, `ErrLoggerClosed`.
- `async_test.go` (create): all logger behavior tests.
- `README.md`, `doc.go`, `example_test.go` (modify): document + example the logger.

---

## Task 1: Transport honors 429/503 + Retry-After

**Files:**
- Modify: `client.go` (add `parseRetryAfter`; edit `doRequest`, currently `client.go:87-125`)
- Test: `client_more_test.go`

**Interfaces:**
- Consumes: existing `doRequest(ctx, cfg, base, method, path string, body []byte) ([]byte, int, error)`, `apiError`.
- Produces: `parseRetryAfter(v string, now time.Time) (time.Duration, bool)` — package-level, pure, returns the wait and whether the header was present/parseable. Used only inside `doRequest` but exported-to-package for unit testing.

- [ ] **Step 1: Write the failing unit test for `parseRetryAfter`**

Add to `client_more_test.go`:

```go
func TestParseRetryAfter(t *testing.T) {
	base := time.Date(2025, 10, 21, 7, 28, 0, 0, time.UTC)
	cases := []struct {
		name   string
		in     string
		now    time.Time
		wantD  time.Duration
		wantOK bool
	}{
		{"empty", "", base, 0, false},
		{"seconds", "5", base, 5 * time.Second, true},
		{"seconds with space", " 5 ", base, 5 * time.Second, true},
		{"negative seconds clamps", "-3", base, 0, true},
		{"http-date future", "Tue, 21 Oct 2025 07:28:30 GMT", base, 30 * time.Second, true},
		{"http-date past clamps", "Tue, 21 Oct 2025 07:27:00 GMT", base, 0, true},
		{"garbage", "soon", base, 0, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d, ok := parseRetryAfter(c.in, c.now)
			if ok != c.wantOK || d != c.wantD {
				t.Fatalf("parseRetryAfter(%q) = %v,%v want %v,%v", c.in, d, ok, c.wantD, c.wantOK)
			}
		})
	}
}
```

- [ ] **Step 2: Run it, verify it fails**

Run: `go test ./... -run TestParseRetryAfter`
Expected: FAIL — `undefined: parseRetryAfter`.

- [ ] **Step 3: Implement `parseRetryAfter` in `client.go`**

Add `strconv` to the import block, then add:

```go
// parseRetryAfter interprets a Retry-After header value in either form:
// delta-seconds ("5") or an HTTP-date. It returns the wait clamped to >= 0 and
// whether the header was present and parseable. now is injected for testing.
func parseRetryAfter(v string, now time.Time) (time.Duration, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, false
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs < 0 {
			secs = 0
		}
		return time.Duration(secs) * time.Second, true
	}
	if t, err := http.ParseTime(v); err == nil {
		d := t.Sub(now)
		if d < 0 {
			d = 0
		}
		return d, true
	}
	return 0, false
}
```

- [ ] **Step 4: Run it, verify it passes**

Run: `go test ./... -run TestParseRetryAfter`
Expected: PASS.

- [ ] **Step 5: Write the failing integration tests (429/503 retry + Retry-After wait + ctx cancel)**

Add to `client_more_test.go`:

```go
func TestRetryOn429ThenSuccess(t *testing.T) {
	var n int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&n, 1) == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{})
	}))
	defer srv.Close()
	c, _ := NewClient(srv.URL)
	if err := c.do(context.Background(), http.MethodGet, "experiments/get", nil, nil); err != nil {
		t.Fatalf("want success after 429 retry, got %v", err)
	}
	if atomic.LoadInt32(&n) != 2 {
		t.Fatalf("attempts = %d, want 2", n)
	}
}

func TestRetryOn503(t *testing.T) {
	var n int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&n, 1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{})
	}))
	defer srv.Close()
	c, _ := NewClient(srv.URL)
	if err := c.do(context.Background(), http.MethodGet, "experiments/get", nil, nil); err != nil {
		t.Fatalf("want success after 503 retry, got %v", err)
	}
	if atomic.LoadInt32(&n) != 2 {
		t.Fatalf("attempts = %d, want 2", n)
	}
}

func TestRetryAfterSecondsWaits(t *testing.T) {
	var n int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&n, 1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{})
	}))
	defer srv.Close()
	c, _ := NewClient(srv.URL)
	start := time.Now()
	if err := c.do(context.Background(), http.MethodGet, "experiments/get", nil, nil); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 900*time.Millisecond {
		t.Fatalf("Retry-After: 1 should pause ~1s, waited %v", elapsed)
	}
}

func TestNon429FourxxNotRetried(t *testing.T) {
	var n int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&n, 1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()
	c, _ := NewClient(srv.URL)
	if err := c.do(context.Background(), http.MethodGet, "experiments/get", nil, nil); err == nil {
		t.Fatal("want error on 400")
	}
	if atomic.LoadInt32(&n) != 1 {
		t.Fatalf("400 must not retry, attempts = %d", n)
	}
}

func TestCtxCancelDuringRetryAfterWait(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()
	c, _ := NewClient(srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := c.do(ctx, http.MethodGet, "experiments/get", nil, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want context deadline error, got %v", err)
	}
}
```

Ensure `client_more_test.go` imports `sync/atomic`, `time`, `errors`, `net/http`, `net/http/httptest`, `encoding/json`, `context` (add any missing).

- [ ] **Step 6: Run, verify failures**

Run: `go test ./... -run 'Retry|Non429|CtxCancelDuringRetry'`
Expected: FAIL — 429/400/ctx tests fail (429 currently not retried; Retry-After ignored). `TestRetryOn503` may already pass (503 is 5xx today); that's fine.

- [ ] **Step 7: Edit `doRequest` to retry 429 and honor Retry-After**

Replace the loop body in `client.go` (`doRequest`, lines ~89-124) with:

```go
	var lastErr error
	var retryAfter time.Duration
	var haveRetryAfter bool
	for attempt := 0; attempt <= cfg.maxRetries; attempt++ {
		if attempt > 0 {
			d := time.Duration(math.Min(float64(time.Second)*math.Pow(2, float64(attempt-1)), float64(10*time.Second)))
			if haveRetryAfter {
				d = retryAfter
				haveRetryAfter = false
			}
			select {
			case <-time.After(d):
			case <-ctx.Done():
				return nil, 0, ctx.Err()
			}
		}
		req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
		if err != nil {
			return nil, 0, err
		}
		if len(body) > 0 {
			req.Header.Set("Content-Type", "application/json")
		}
		if cfg.bearer != "" {
			req.Header.Set("Authorization", "Bearer "+cfg.bearer)
		} else if cfg.user != "" {
			req.SetBasicAuth(cfg.user, cfg.pass)
		}
		resp, err := cfg.httpc.Do(req)
		if err != nil {
			lastErr = err // transport error: retry
			continue
		}
		data, _ := io.ReadAll(resp.Body)
		ra, raOK := parseRetryAfter(resp.Header.Get("Retry-After"), time.Now())
		status := resp.StatusCode
		resp.Body.Close()
		// Retry 429 (rate limited) and 5xx (includes 503). Everything else returns.
		if status == http.StatusTooManyRequests || status/100 == 5 {
			lastErr = apiError(data, status)
			if raOK {
				retryAfter, haveRetryAfter = ra, true
			}
			continue
		}
		return data, status, nil
	}
	return nil, 0, lastErr
```

- [ ] **Step 8: Run the full suite, verify green**

Run: `go test ./...`
Expected: PASS (all transport tests, no regressions).

- [ ] **Step 9: Commit**

```bash
git add client.go client_more_test.go
git commit -m "feat: retry 429/503 and honor Retry-After in transport"
```

---

## Task 2: AsyncLogger MVP — buffer, worker, single-run flush, Close

**Files:**
- Create: `async.go`
- Test: `async_test.go`

**Interfaces:**
- Consumes: `*Client`, `Client.LogBatch(ctx, runID string, metrics []Metric, params []Param, tags []RunTag) error`, types `Metric`, `Param`, `RunTag`.
- Produces:
  - `func (c *Client) NewAsyncLogger(opts ...AsyncOption) *AsyncLogger`
  - `type AsyncOption func(*asyncConfig)`
  - `func (a *AsyncLogger) LogMetric(ctx context.Context, runID, key string, value float64, tsMs, step int64) error`
  - `func (a *AsyncLogger) Close() error`
  - `var ErrLoggerClosed error`
  - internal: `record`, `bucket`, `asyncConfig`, `(*AsyncLogger).run()`, `(*AsyncLogger).send(ctx, record) error`

- [ ] **Step 1: Write the failing test (single run, flush on Close)**

Create `async_test.go`:

```go
package mlflow

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// batchSink is a fakeClient transport that records every runs/log-batch call.
type batchSink struct {
	mu      sync.Mutex
	calls   int32
	metrics map[string][]Metric
	params  map[string][]Param
	tags    map[string][]RunTag
	fail    func() error // optional: return an error from each call
	gate    chan struct{} // optional: if non-nil, first call blocks until closed
	gated   int32
}

func newBatchSink() *batchSink {
	return &batchSink{
		metrics: map[string][]Metric{},
		params:  map[string][]Param{},
		tags:    map[string][]RunTag{},
	}
}

func (s *batchSink) client() *Client {
	return fakeClient(func(method, path string, in, out any) error {
		if path != "runs/log-batch" {
			return nil
		}
		if s.gate != nil && atomic.AddInt32(&s.gated, 1) == 1 {
			<-s.gate
		}
		atomic.AddInt32(&s.calls, 1)
		req := in.(*logBatchReq)
		s.mu.Lock()
		s.metrics[req.RunID] = append(s.metrics[req.RunID], req.Metrics...)
		s.params[req.RunID] = append(s.params[req.RunID], req.Params...)
		s.tags[req.RunID] = append(s.tags[req.RunID], req.Tags...)
		s.mu.Unlock()
		if s.fail != nil {
			return s.fail()
		}
		return nil
	})
}

func (s *batchSink) metricCount(runID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.metrics[runID])
}

func TestAsyncLoggerFlushesOnClose(t *testing.T) {
	s := newBatchSink()
	a := s.client().NewAsyncLogger()
	for i := 0; i < 250; i++ {
		if err := a.LogMetric(context.Background(), "run1", "loss", float64(i), int64(i), int64(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
	if got := s.metricCount("run1"); got != 250 {
		t.Fatalf("delivered %d metrics, want 250", got)
	}
}
```

Note: `logBatchReq` is the existing unexported type in `logging.go`; the fake observes it via the `in any` argument because `LogBatch` passes `chunk *logBatchReq` to `c.do`.

- [ ] **Step 2: Run, verify it fails**

Run: `go test ./... -run TestAsyncLoggerFlushesOnClose`
Expected: FAIL — `NewAsyncLogger` undefined.

- [ ] **Step 3: Implement `async.go` (MVP)**

```go
package mlflow

import (
	"context"
	"errors"
	"sync"
)

// ErrLoggerClosed is returned by AsyncLogger methods called after Close.
var ErrLoggerClosed = errors.New("mlflow: async logger closed")

type recordKind int

const (
	kindMetric recordKind = iota
	kindParam
	kindTag
)

type record struct {
	runID  string
	kind   recordKind
	metric Metric
	param  Param
	tag    RunTag
}

type bucket struct {
	metrics []Metric
	params  []Param
	tags    []RunTag
	n       int
}

type asyncConfig struct {
	bufferSize int
	batchSize  int
	onError    func(error)
}

// AsyncOption configures an AsyncLogger.
type AsyncOption func(*asyncConfig)

// WithBufferSize sets the in-flight record channel capacity (default 8192).
func WithBufferSize(n int) AsyncOption { return func(c *asyncConfig) { c.bufferSize = n } }

// WithBatchSize caps records per flush, clamped to [1,1000] (default 1000, the
// API limit). Real batch size still tracks traffic via the greedy drain.
func WithBatchSize(n int) AsyncOption { return func(c *asyncConfig) { c.batchSize = n } }

// WithErrorHandler is invoked once per failed background flush. Errors are also
// aggregated and returned by Close.
func WithErrorHandler(h func(error)) AsyncOption { return func(c *asyncConfig) { c.onError = h } }

// AsyncLogger buffers metric/param/tag writes and flushes them as runs/log-batch
// calls from a single background worker. Fire-and-forget: Log* methods enqueue
// and return; flush failures surface via WithErrorHandler and Close.
type AsyncLogger struct {
	client  *Client
	cfg     asyncConfig
	records chan record
	closed  chan struct{}
	done    chan struct{}

	closeOnce sync.Once
	mu        sync.Mutex
	errs      []error
}

// NewAsyncLogger starts a buffered logger over c. Call Close to flush and stop.
func (c *Client) NewAsyncLogger(opts ...AsyncOption) *AsyncLogger {
	cfg := asyncConfig{bufferSize: 8192, batchSize: 1000}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.bufferSize < 1 {
		cfg.bufferSize = 1
	}
	if cfg.batchSize < 1 {
		cfg.batchSize = 1
	}
	if cfg.batchSize > 1000 {
		cfg.batchSize = 1000
	}
	a := &AsyncLogger{
		client:  c,
		cfg:     cfg,
		records: make(chan record, cfg.bufferSize),
		closed:  make(chan struct{}),
		done:    make(chan struct{}),
	}
	go a.run()
	return a
}

func (a *AsyncLogger) recordErr(err error) {
	if a.cfg.onError != nil {
		a.cfg.onError(err)
	}
	a.mu.Lock()
	a.errs = append(a.errs, err)
	a.mu.Unlock()
}

// run is the worker. It never waits on a clock: it blocks receiving the first
// record, greedy-drains whatever else is queued, then flushes once it catches up.
func (a *AsyncLogger) run() {
	defer close(a.done)
	b := &bucket{}
	curRun := ""
	flush := func() {
		if b.n == 0 {
			return
		}
		if err := a.client.LogBatch(context.Background(), curRun, b.metrics, b.params, b.tags); err != nil {
			a.recordErr(err)
		}
		b = &bucket{}
	}
	add := func(rec record) {
		curRun = rec.runID
		switch rec.kind {
		case kindMetric:
			b.metrics = append(b.metrics, rec.metric)
		case kindParam:
			b.params = append(b.params, rec.param)
		case kindTag:
			b.tags = append(b.tags, rec.tag)
		}
		b.n++
	}
	for {
		select {
		case rec, ok := <-a.records:
			if !ok {
				flush()
				return
			}
			add(rec)
		case <-a.closed:
			a.drainAll(add)
			flush()
			return
		}
		// greedy non-blocking drain, then flush once caught up
		for draining := true; draining; {
			select {
			case rec, ok := <-a.records:
				if !ok {
					flush()
					return
				}
				add(rec)
			default:
				draining = false
			}
		}
		flush()
	}
}

// drainAll pulls every record currently buffered (non-blocking) into add.
func (a *AsyncLogger) drainAll(add func(record)) {
	for {
		select {
		case rec := <-a.records:
			add(rec)
		default:
			return
		}
	}
}

func (a *AsyncLogger) send(ctx context.Context, rec record) error {
	select {
	case <-a.closed:
		return ErrLoggerClosed
	default:
	}
	select {
	case a.records <- rec:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-a.closed:
		return ErrLoggerClosed
	}
}

// LogMetric enqueues a metric for the run. It blocks only if the buffer is full,
// returning ctx.Err() if ctx is canceled while blocked, or ErrLoggerClosed after Close.
func (a *AsyncLogger) LogMetric(ctx context.Context, runID, key string, value float64, tsMs, step int64) error {
	return a.send(ctx, record{runID: runID, kind: kindMetric, metric: Metric{Key: key, Value: value, Timestamp: tsMs, Step: step}})
}

// Close flushes buffered records, stops the worker, and returns the aggregate of
// all flush errors. Idempotent.
func (a *AsyncLogger) Close() error {
	a.closeOnce.Do(func() { close(a.closed) })
	<-a.done
	a.mu.Lock()
	defer a.mu.Unlock()
	return errors.Join(a.errs...)
}
```

Note: this MVP uses a single `bucket` and `curRun` — correct only for one run. Task 3 generalizes to per-run buckets. The `add`/`flush` signatures stay; only their bodies change. The batch-size trigger is added in Task 4.

- [ ] **Step 4: Run, verify it passes**

Run: `go test ./... -run TestAsyncLoggerFlushesOnClose`
Expected: PASS.

- [ ] **Step 5: Run full suite (no regressions)**

Run: `go test ./...`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add async.go async_test.go
git commit -m "feat: AsyncLogger buffered single-run batch logging"
```

---

## Task 3: Per-run bucketing

**Files:**
- Modify: `async.go` (`run`, replace single bucket with `map[string]*bucket`)
- Test: `async_test.go`

**Interfaces:**
- Produces: no signature changes. `run` now keys buckets by run_id and flushes one `LogBatch` per run.

- [ ] **Step 1: Write the failing test (interleaved runs)**

Add to `async_test.go`:

```go
func TestAsyncLoggerPerRunBucketing(t *testing.T) {
	s := newBatchSink()
	a := s.client().NewAsyncLogger()
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		if err := a.LogMetric(ctx, "runA", "loss", float64(i), 0, int64(i)); err != nil {
			t.Fatal(err)
		}
		if err := a.LogMetric(ctx, "runB", "acc", float64(i), 0, int64(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if got := s.metricCount("runA"); got != 10 {
		t.Fatalf("runA = %d, want 10", got)
	}
	if got := s.metricCount("runB"); got != 10 {
		t.Fatalf("runB = %d, want 10", got)
	}
	// Each run's metrics must carry that run's key, never the other's.
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range s.metrics["runA"] {
		if m.Key != "loss" {
			t.Fatalf("runA got foreign metric %q", m.Key)
		}
	}
	for _, m := range s.metrics["runB"] {
		if m.Key != "acc" {
			t.Fatalf("runB got foreign metric %q", m.Key)
		}
	}
}
```

- [ ] **Step 2: Run, verify it fails**

Run: `go test ./... -run TestAsyncLoggerPerRunBucketing`
Expected: FAIL — single-bucket MVP misattributes runs (`curRun` is whichever record came last; counts and/or keys wrong).

- [ ] **Step 3: Rewrite `run` to bucket per run_id**

Replace the entire `run` method in `async.go` with:

```go
// run is the worker. It never waits on a clock: it blocks receiving the first
// record, greedy-drains whatever else is queued, then flushes once it catches up.
// Records are bucketed per run_id because runs/log-batch is per-run.
func (a *AsyncLogger) run() {
	defer close(a.done)
	buckets := map[string]*bucket{}
	add := func(rec record) {
		b := buckets[rec.runID]
		if b == nil {
			b = &bucket{}
			buckets[rec.runID] = b
		}
		switch rec.kind {
		case kindMetric:
			b.metrics = append(b.metrics, rec.metric)
		case kindParam:
			b.params = append(b.params, rec.param)
		case kindTag:
			b.tags = append(b.tags, rec.tag)
		}
		b.n++
	}
	flush := func(runID string) {
		b := buckets[runID]
		if b == nil || b.n == 0 {
			return
		}
		if err := a.client.LogBatch(context.Background(), runID, b.metrics, b.params, b.tags); err != nil {
			a.recordErr(err)
		}
		delete(buckets, runID)
	}
	flushAll := func() {
		for id := range buckets {
			flush(id)
		}
	}
	for {
		select {
		case rec, ok := <-a.records:
			if !ok {
				flushAll()
				return
			}
			add(rec)
		case <-a.closed:
			a.drainAll(add)
			flushAll()
			return
		}
		for draining := true; draining; {
			select {
			case rec, ok := <-a.records:
				if !ok {
					flushAll()
					return
				}
				add(rec)
			default:
				draining = false
			}
		}
		flushAll()
	}
}
```

- [ ] **Step 4: Run, verify it passes**

Run: `go test ./... -run 'TestAsyncLogger'`
Expected: PASS (both bucketing and the earlier single-run test).

- [ ] **Step 5: Commit**

```bash
git add async.go async_test.go
git commit -m "feat: AsyncLogger per-run bucketing"
```

---

## Task 4: Batch-size cap + greedy coalescing

**Files:**
- Modify: `async.go` (`run` — flush a bucket when it reaches `cfg.batchSize`)
- Test: `async_test.go`

**Interfaces:**
- Produces: no signature changes. A per-run bucket that reaches `cfg.batchSize` total records is flushed immediately, in addition to the catch-up flush.

- [ ] **Step 1: Write the failing tests (size cap + coalescing)**

Add to `async_test.go`:

```go
func TestAsyncLoggerBatchSizeCap(t *testing.T) {
	s := newBatchSink()
	// Hold the first flush so a burst piles up behind it.
	s.gate = make(chan struct{})
	a := s.client().NewAsyncLogger(WithBatchSize(10))
	ctx := context.Background()
	for i := 0; i < 95; i++ {
		if err := a.LogMetric(ctx, "run1", "loss", float64(i), 0, int64(i)); err != nil {
			t.Fatal(err)
		}
	}
	close(s.gate)
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if got := s.metricCount("run1"); got != 95 {
		t.Fatalf("delivered %d, want 95", got)
	}
	// No single observed batch may exceed the cap.
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sz := range s.batchSizes {
		if sz > 10 {
			t.Fatalf("batch of %d exceeds cap 10", sz)
		}
	}
}

func TestAsyncLoggerCoalescesBurst(t *testing.T) {
	s := newBatchSink()
	s.gate = make(chan struct{}) // stall worker on its first flush
	a := s.client().NewAsyncLogger() // default batchSize 1000
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		if err := a.LogMetric(ctx, "run1", "loss", float64(i), 0, int64(i)); err != nil {
			t.Fatal(err)
		}
	}
	close(s.gate)
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if got := s.metricCount("run1"); got != 100 {
		t.Fatalf("delivered %d, want 100", got)
	}
	// 100 records must coalesce into a handful of batches, not ~100.
	if c := atomic.LoadInt32(&s.calls); c > 5 {
		t.Fatalf("expected coalesced batches, got %d log-batch calls", c)
	}
}
```

This requires `batchSink` to record per-call batch size. Update `newBatchSink`/`client` in `async_test.go`: add field `batchSizes []int` and, inside the locked section of the transport, `s.batchSizes = append(s.batchSizes, len(req.Metrics)+len(req.Params)+len(req.Tags))`. Initialize `batchSizes` in `newBatchSink` (zero value nil slice is fine; appends work).

- [ ] **Step 2: Run, verify it fails**

Run: `go test ./... -run 'BatchSizeCap|Coalesces'`
Expected: `BatchSizeCap` FAILs (no size trigger yet — the whole burst flushes in one >10 batch). `Coalesces` likely passes already (catch-up flush coalesces); that's fine.

- [ ] **Step 3: Add the batch-size trigger in `run`**

In the rewritten `run` from Task 3, add a size check after each `add(rec)` in BOTH the top `case rec, ok := <-a.records:` branch and the inner draining `case`. After `add(rec)` insert:

```go
			if buckets[rec.runID].n >= a.cfg.batchSize {
				flush(rec.runID)
			}
```

So both spots read:

```go
			add(rec)
			if buckets[rec.runID].n >= a.cfg.batchSize {
				flush(rec.runID)
			}
```

(The `case <-a.closed:` drainAll path does not need the inline check — `flushAll` handles it; `drainAll` uses the plain `add`.)

- [ ] **Step 4: Run, verify it passes**

Run: `go test ./... -run 'BatchSizeCap|Coalesces|TestAsyncLogger'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add async.go async_test.go
git commit -m "feat: AsyncLogger batch-size flush trigger"
```

---

## Task 5: Backpressure — block on full, ctx cancel, log-after-close

**Files:**
- Modify: none (behavior already in `send`); this task proves it.
- Test: `async_test.go`

**Interfaces:**
- Consumes: `send`, `LogMetric`, `Close`, `ErrLoggerClosed` from Task 2.

- [ ] **Step 1: Write the guard tests**

Add to `async_test.go`:

```go
func TestAsyncLoggerCtxCancelWhenFull(t *testing.T) {
	s := newBatchSink()
	s.gate = make(chan struct{})
	a := s.client().NewAsyncLogger(WithBufferSize(1))
	ctx := context.Background()
	// m0 gets pulled by the worker, which then blocks in its first (gated) flush.
	if err := a.LogMetric(ctx, "run1", "loss", 0, 0, 0); err != nil {
		t.Fatal(err)
	}
	// Wait until the worker is parked inside the flush gate.
	for atomic.LoadInt32(&s.gated) == 0 {
		runtime.Gosched()
	}
	// Buffer (size 1) now empty; fill it.
	if err := a.LogMetric(ctx, "run1", "loss", 1, 0, 1); err != nil {
		t.Fatal(err)
	}
	// Next send must block; with a canceled ctx it returns promptly.
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := a.LogMetric(cctx, "run1", "loss", 2, 0, 2); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled when full, got %v", err)
	}
	close(s.gate)
	_ = a.Close()
}

func TestAsyncLoggerLogAfterCloseErrors(t *testing.T) {
	s := newBatchSink()
	a := s.client().NewAsyncLogger()
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.LogMetric(context.Background(), "run1", "loss", 0, 0, 0); !errors.Is(err, ErrLoggerClosed) {
		t.Fatalf("want ErrLoggerClosed, got %v", err)
	}
}
```

Add `"runtime"` to the `async_test.go` imports.

- [ ] **Step 2: Run, verify behavior**

Run: `go test ./... -run 'CtxCancelWhenFull|LogAfterClose'`
Expected: PASS (the `send` implementation from Task 2 already provides this). If `CtxCancelWhenFull` flakes, the gate/`gated` handshake from Task 4's sink is the dependency — ensure `s.gated` is incremented before `<-s.gate` in the transport (it is, via `atomic.AddInt32(&s.gated,1)`).

- [ ] **Step 3: Commit**

```bash
git add async_test.go
git commit -m "test: AsyncLogger backpressure, ctx cancel, log-after-close"
```

---

## Task 6: Error handler + Close aggregate

**Files:**
- Modify: none (wired in Task 2); this task proves it.
- Test: `async_test.go`

**Interfaces:**
- Consumes: `WithErrorHandler`, `recordErr`, `Close` returning `errors.Join(errs...)`.

- [ ] **Step 1: Write the failing/guard test**

Add to `async_test.go`:

```go
func TestAsyncLoggerErrorHandlerAndAggregate(t *testing.T) {
	s := newBatchSink()
	s.fail = func() error { return errors.New("boom") }
	var handled int32
	a := s.client().NewAsyncLogger(WithErrorHandler(func(error) { atomic.AddInt32(&handled, 1) }))
	if err := a.LogParam(context.Background(), "run1", "lr", "0.01"); err != nil {
		t.Fatal(err)
	}
	err := a.Close()
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("Close aggregate = %v, want it to contain boom", err)
	}
	if atomic.LoadInt32(&handled) == 0 {
		t.Fatal("error handler was never called")
	}
}
```

This test calls `LogParam`, which must exist (added next step if not already).

- [ ] **Step 2: Add `LogParam` and `SetTag` to `async.go`** (if not yet present)

```go
// LogParam enqueues a param for the run. Blocking semantics match LogMetric.
func (a *AsyncLogger) LogParam(ctx context.Context, runID, key, value string) error {
	return a.send(ctx, record{runID: runID, kind: kindParam, param: Param{Key: key, Value: value}})
}

// SetTag enqueues a run tag. Blocking semantics match LogMetric.
func (a *AsyncLogger) SetTag(ctx context.Context, runID, key, value string) error {
	return a.send(ctx, record{runID: runID, kind: kindTag, tag: RunTag{Key: key, Value: value}})
}
```

- [ ] **Step 3: Run, verify it passes**

Run: `go test ./... -run ErrorHandlerAndAggregate`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add async.go async_test.go
git commit -m "feat: AsyncLogger LogParam/SetTag and error aggregation"
```

---

## Task 7: Flush()

**Files:**
- Modify: `async.go` (add `flushReq` channel, `Flush`, handle in `run`)
- Test: `async_test.go`

**Interfaces:**
- Produces: `func (a *AsyncLogger) Flush(ctx context.Context) error` — force-flushes all currently-buffered records and waits for completion; returns ctx error if canceled while waiting, ErrLoggerClosed if closed, else the aggregate so far.

- [ ] **Step 1: Write the failing test (delivery before Close)**

Add to `async_test.go`:

```go
func TestAsyncLoggerFlush(t *testing.T) {
	s := newBatchSink()
	a := s.client().NewAsyncLogger()
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if err := a.LogMetric(ctx, "run1", "loss", float64(i), 0, int64(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.Flush(ctx); err != nil {
		t.Fatalf("Flush = %v", err)
	}
	// Delivered without Close.
	if got := s.metricCount("run1"); got != 5 {
		t.Fatalf("after Flush delivered %d, want 5", got)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
}
```

- [ ] **Step 2: Run, verify it fails**

Run: `go test ./... -run TestAsyncLoggerFlush$`
Expected: FAIL — `a.Flush` undefined. (Note: without Flush, the worker's catch-up flush would deliver eventually, but the test asserts delivery synchronously after the `Flush` call returns, which requires the handshake.)

- [ ] **Step 3: Add `flushReq`, `Flush`, and the worker case**

In `NewAsyncLogger`, add the field init `flushReq: make(chan chan struct{}),` and add the field to the struct:

```go
	flushReq chan chan struct{}
```

In `run`, add a `case` to the TOP-level select (alongside the records and closed cases):

```go
		case ack := <-a.flushReq:
			a.drainAll(add)
			flushAll()
			close(ack)
			continue
```

Add the method:

```go
// Flush force-flushes all records buffered at the time of the call and waits for
// those batches to complete. Returns ctx.Err() if ctx is canceled while waiting,
// ErrLoggerClosed if the logger is closed, otherwise the aggregate flush error so far.
func (a *AsyncLogger) Flush(ctx context.Context) error {
	ack := make(chan struct{})
	select {
	case a.flushReq <- ack:
	case <-a.closed:
		return ErrLoggerClosed
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-ack:
	case <-ctx.Done():
		return ctx.Err()
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return errors.Join(a.errs...)
}
```

Note: `flushReq` is unbuffered; the worker only reads it at the top of its loop. A `Flush` issued while the worker is mid-drain waits until the worker loops back — by then the in-progress drain has already flushed, so correctness holds and `Flush` still blocks until its own ack.

- [ ] **Step 4: Run, verify it passes**

Run: `go test ./... -run 'TestAsyncLogger'`
Expected: PASS (all logger tests).

- [ ] **Step 5: Run the full suite with the race detector**

Run: `go test -race ./...`
Expected: PASS, no data races. (The worker, `send`, `recordErr`/`mu`, and `closeOnce` are the concurrency surface; the race detector is the gate.)

- [ ] **Step 6: Commit**

```bash
git add async.go async_test.go
git commit -m "feat: AsyncLogger Flush"
```

---

## Task 8: Document the AsyncLogger

**Files:**
- Modify: `README.md` (add a short async-logging section), `example_test.go` (runnable example)

**Interfaces:**
- Consumes: the full public surface from Tasks 2-7.

- [ ] **Step 1: Add a runnable example to `example_test.go`**

Append:

```go
func ExampleAsyncLogger() {
	c, err := NewClient("https://mlflow.example.com", WithBearerToken("tok"))
	if err != nil {
		log.Fatal(err)
	}
	logger := c.NewAsyncLogger(WithErrorHandler(func(err error) {
		log.Printf("mlflow flush failed: %v", err)
	}))
	defer logger.Close() // flushes the remainder

	ctx := context.Background()
	for step := int64(0); step < 1000; step++ {
		_ = logger.LogMetric(ctx, "run-123", "loss", 0.1, 0, step)
	}
	// Output:
}
```

Ensure `example_test.go` imports `context` and `log` (check existing imports first; add only what's missing). The empty `// Output:` keeps it compiling+running without hitting a server (the calls enqueue; Close flushes against the unreachable host and the error handler logs — to keep the example hermetic, this example is illustrative; if `go test` flakes on the network call, change it to `func ExampleAsyncLogger()` without the `// Output:` line so it compiles but is not run, matching how doc-only examples are handled).

Simpler hermetic alternative if the above runs against the network: drop the `// Output:` directive so the example is compiled but not executed.

- [ ] **Step 2: Add a README section**

Add under the existing logging docs in `README.md`:

```markdown
### Async batch logging

For training loops that emit many metrics, `NewAsyncLogger` buffers writes and
flushes them as `runs/log-batch` calls from a single background worker. It blocks
only when the buffer is full (backpressure, never drops), batches per run, and
coalesces bursts automatically. Flush failures go to `WithErrorHandler` and are
also returned by `Close`.

```go
logger := client.NewAsyncLogger(
    mlflow.WithErrorHandler(func(err error) { log.Printf("flush: %v", err) }),
)
defer logger.Close()

for step := int64(0); step < n; step++ {
    logger.LogMetric(ctx, runID, "loss", loss, ts, step)
}
```

Options: `WithBufferSize` (channel capacity, default 8192), `WithBatchSize`
(records per flush, default/maximum 1000), `WithErrorHandler`. Call `Flush(ctx)`
to force a synchronous flush; `Close` flushes the remainder and returns the
aggregate error.
```

- [ ] **Step 3: Verify docs build and examples compile**

Run: `go test ./... && go vet ./...`
Expected: PASS, no vet complaints.

- [ ] **Step 4: Commit**

```bash
git add README.md example_test.go
git commit -m "docs: document AsyncLogger"
```

---

## Self-Review

**Spec coverage:**
- Part 1 (429/503 + Retry-After, both header forms, ctx race) → Task 1. ✓
- AsyncLogger surface (`NewAsyncLogger`, `WithBufferSize`, `WithBatchSize`, `WithErrorHandler`, `LogMetric/LogParam/SetTag`, `Flush`, `Close`) → Tasks 2/6/7. ✓
- Block-on-full backpressure, no drop → Task 5. ✓
- Error handler + Close aggregate → Task 6. ✓
- Per-run bucketing → Task 3. ✓
- Greedy drain, two triggers, no timer → Tasks 2 (catch-up) + 4 (size cap). ✓
- Deliberately skipped (multi-worker, shutdown timeout, drop policy) → not built; note as `ponytail:` comments when implementing the struct. ✓ (Add a `// ponytail: single worker; add WithFlushWorkers if one saturates` comment above `run`.)

**Placeholder scan:** No TBD/TODO; every code step shows complete code. Task 8's example has a documented hermetic fallback (drop `// Output:`), not a placeholder.

**Type consistency:** `record{runID,kind,metric,param,tag}`, `bucket{metrics,params,tags,n}`, `asyncConfig{bufferSize,batchSize,onError}`, `flush(runID string)`, `add(record)`, `flushAll()`, `drainAll(add)`, `send(ctx,record)`, `recordErr(error)` consistent across Tasks 2-7. `batchSink` test helper gains `batchSizes` (Task 4), `gate`/`gated`/`fail` used consistently.
