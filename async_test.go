package mlflow

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// batchSink is a fakeClient transport that records every runs/log-batch call.
type batchSink struct {
	mu         sync.Mutex
	calls      atomic.Int32
	metrics    map[string][]Metric
	params     map[string][]Param
	tags       map[string][]RunTag
	batchSizes []int      // records len(metrics)+len(params)+len(tags) per call
	fail       func() error // optional: return an error from each call; guarded by mu
	gate       chan struct{} // optional: if non-nil, first call blocks until closed
	gated      atomic.Int32
}

func newBatchSink() *batchSink {
	return &batchSink{
		metrics: map[string][]Metric{},
		params:  map[string][]Param{},
		tags:    map[string][]RunTag{},
	}
}

// setFail safely sets the fail hook from a test goroutine.
func (s *batchSink) setFail(f func() error) {
	s.mu.Lock()
	s.fail = f
	s.mu.Unlock()
}

func (s *batchSink) client() *Client {
	return fakeClient(func(method, path string, in, out any) error {
		if path != "runs/log-batch" {
			return nil
		}
		if s.gate != nil && s.gated.Add(1) == 1 {
			<-s.gate
		}
		s.calls.Add(1)
		req := in.(*logBatchReq)
		s.mu.Lock()
		s.metrics[req.RunID] = append(s.metrics[req.RunID], req.Metrics...)
		s.params[req.RunID] = append(s.params[req.RunID], req.Params...)
		s.tags[req.RunID] = append(s.tags[req.RunID], req.Tags...)
		s.batchSizes = append(s.batchSizes, len(req.Metrics)+len(req.Params)+len(req.Tags))
		failFn := s.fail
		s.mu.Unlock()
		if failFn != nil {
			return failFn()
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
	if c := s.calls.Load(); c > 5 {
		t.Fatalf("expected coalesced batches, got %d log-batch calls", c)
	}
}

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
	for s.gated.Load() == 0 {
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

func TestAsyncLoggerErrorHandlerAndAggregate(t *testing.T) {
	s := newBatchSink()
	s.setFail(func() error { return errors.New("boom") })
	var handled atomic.Int32
	a := s.client().NewAsyncLogger(WithErrorHandler(func(error) { handled.Add(1) }))
	if err := a.LogParam(context.Background(), "run1", "lr", "0.01"); err != nil {
		t.Fatal(err)
	}
	err := a.Close()
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("Close aggregate = %v, want it to contain boom", err)
	}
	if handled.Load() == 0 {
		t.Fatal("error handler was never called")
	}
}

// TestAsyncLoggerFlushPerCallErrors verifies that Flush reports only errors from
// the current flush, not from earlier flushes, while Close still returns the full
// aggregate.
func TestAsyncLoggerFlushPerCallErrors(t *testing.T) {
	boom := errors.New("boom")
	s := newBatchSink()
	s.setFail(func() error { return boom })
	a := s.client().NewAsyncLogger()
	ctx := context.Background()

	// First flush: fail is set, so Flush must return an error containing boom.
	if err := a.LogParam(ctx, "run1", "lr", "0.01"); err != nil {
		t.Fatal(err)
	}
	err := a.Flush(ctx)
	if err == nil || !errors.Is(err, boom) {
		t.Fatalf("Flush (fail set) = %v, want error containing boom", err)
	}

	// Second flush: fail cleared, so Flush must return nil even though boom is
	// still in a.errs from the first flush.
	s.setFail(nil)
	if err := a.LogMetric(ctx, "run1", "loss", 0.1, 0, 1); err != nil {
		t.Fatal(err)
	}
	if err := a.Flush(ctx); err != nil {
		t.Fatalf("Flush (fail cleared) = %v, want nil", err)
	}

	// Close returns the cumulative aggregate, which still contains boom.
	if err := a.Close(); !errors.Is(err, boom) {
		t.Fatalf("Close cumulative = %v, want error containing boom", err)
	}
}

// TestAsyncLoggerConcurrentFlushRace stress-tests concurrent Flush calls against
// a running logger to give the race detector coverage of the ack handshake.
func TestAsyncLoggerConcurrentFlushRace(t *testing.T) {
	s := newBatchSink()
	a := s.client().NewAsyncLogger()
	ctx, cancel := context.WithCancel(context.Background())

	const flushers = 4
	const iters = 30
	var wg sync.WaitGroup
	wg.Add(flushers)
	for i := 0; i < flushers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				_ = a.Flush(ctx)
			}
		}()
	}

	for i := 0; i < 100; i++ {
		_ = a.LogMetric(ctx, "run1", "loss", float64(i), 0, int64(i))
	}

	// Cancel first so any Flush blocked on <-ack can escape via ctx.Done().
	cancel()
	wg.Wait()
	if err := a.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
}
