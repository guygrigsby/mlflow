//go:build integration

package mlflow

import (
	"context"
	"os"
	"testing"
	"time"
)

// Run against a throwaway server:
//
//	mlflow server --backend-store-uri sqlite:////tmp/mlflow-itest.db --host 127.0.0.1 --port 5000
//	MLFLOW_TRACKING_URI=http://127.0.0.1:5000 go test -tags integration ./... -run TestIntegration -v
//
// Without MLFLOW_TRACKING_URI the test skips, so default `go test ./...` stays network-free.
func TestIntegrationRoundTrip(t *testing.T) {
	if os.Getenv("MLFLOW_TRACKING_URI") == "" {
		t.Skip("set MLFLOW_TRACKING_URI to run")
	}
	c, err := NewClient("")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	exp, err := c.GetOrCreateExperiment(ctx, "itest-"+time.Now().Format("150405"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMilli()
	run, err := c.CreateRun(ctx, exp, WithRunName("r"), WithStartTime(now))
	if err != nil {
		t.Fatal(err)
	}
	id := run.Info.RunID

	if err := c.LogBatch(ctx, id,
		[]Metric{{Key: "loss", Value: 1.5, Timestamp: now, Step: 0}},
		[]Param{{Key: "lr", Value: "4e-4"}},
		[]RunTag{{Key: "source", Value: "integration-test"}}); err != nil {
		t.Fatal(err)
	}
	if err := c.UpdateRun(ctx, id, StatusFinished, time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}

	got, err := c.GetRun(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if got.Info.Status != StatusFinished {
		t.Fatalf("status = %s", got.Info.Status)
	}
	if len(got.Data.Metrics) == 0 || got.Data.Metrics[0].Key != "loss" {
		t.Fatalf("metrics = %+v", got.Data.Metrics)
	}

	hist, err := c.GetMetricHistory(ctx, id, "loss")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) == 0 {
		t.Fatal("empty metric history")
	}
}
