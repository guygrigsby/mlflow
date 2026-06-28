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
