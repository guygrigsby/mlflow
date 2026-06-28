package mlflow_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/guygrigsby/mlflow"
)

// Example shows a full run lifecycle: resolve an experiment, start a run, log a
// batch of metrics/params/tags, then mark the run finished.
func Example() {
	c, err := mlflow.NewClient("") // reads MLFLOW_TRACKING_URI
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()

	exp, err := c.GetOrCreateExperiment(ctx, "lm-100m-en")
	if err != nil {
		log.Fatal(err)
	}

	now := time.Now().UnixMilli()
	run, err := c.CreateRun(ctx, exp, mlflow.WithRunName("r1"), mlflow.WithStartTime(now))
	if err != nil {
		log.Fatal(err)
	}

	err = c.LogBatch(ctx, run.Info.RunID,
		[]mlflow.Metric{{Key: "loss", Value: 1.5, Timestamp: now, Step: 0}},
		[]mlflow.Param{{Key: "lr", Value: "4e-4"}},
		[]mlflow.RunTag{{Key: "stage", Value: "train"}})
	if err != nil {
		log.Fatal(err)
	}

	if err := c.UpdateRun(ctx, run.Info.RunID, mlflow.StatusFinished, time.Now().UnixMilli()); err != nil {
		log.Fatal(err)
	}
}

// ExampleIsNotFound shows branching on a missing resource.
func ExampleIsNotFound() {
	c, err := mlflow.NewClient("")
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()

	if _, err := c.GetExperimentByName(ctx, "does-not-exist"); mlflow.IsNotFound(err) {
		fmt.Println("not found, creating")
		_, _ = c.CreateExperiment(ctx, "does-not-exist")
	}
}

// ExampleClient_LogMetric streams a metric across steps during a training loop.
func ExampleClient_LogMetric() {
	c, err := mlflow.NewClient("")
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()

	runID := "abc123"
	losses := []float64{2.1, 1.7, 1.4}
	for step, loss := range losses {
		ts := time.Now().UnixMilli()
		if err := c.LogMetric(ctx, runID, "loss", loss, ts, int64(step)); err != nil {
			log.Fatal(err)
		}
	}
}

// ExampleAsyncLogger buffers metrics in a background worker and flushes them as
// batch calls. The example enqueues 1000 metrics without blocking (the buffer
// absorbs them), then Close flushes the remainder and returns any errors.
// This example does not include // Output: so it compiles without executing,
// avoiding network calls during go test.
func ExampleAsyncLogger() {
	c, err := mlflow.NewClient("https://mlflow.example.com", mlflow.WithBearerToken("tok"))
	if err != nil {
		log.Fatal(err)
	}
	logger := c.NewAsyncLogger(mlflow.WithErrorHandler(func(err error) {
		log.Printf("mlflow flush failed: %v", err)
	}))
	defer logger.Close()

	ctx := context.Background()
	for step := int64(0); step < 1000; step++ {
		_ = logger.LogMetric(ctx, "run-123", "loss", 0.1, 0, step)
	}
}
