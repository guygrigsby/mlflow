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

// Keep the compiler happy for fields used in later tasks.
var _ = errors.New
var _ = strings.Contains
