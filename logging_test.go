package mlflow

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// makeMetrics returns a slice of n Metric values with distinct keys.
func makeMetrics(n int) []Metric {
	s := make([]Metric, n)
	for i := range s {
		s[i] = Metric{Key: "m", Value: float64(i), Timestamp: int64(i), Step: int64(i)}
	}
	return s
}

// makeParams returns a slice of n Param values.
func makeParams(n int) []Param {
	s := make([]Param, n)
	for i := range s {
		s[i] = Param{Key: "p", Value: "v"}
	}
	return s
}

// makeTags returns a slice of n RunTag values.
func makeTags(n int) []RunTag {
	s := make([]RunTag, n)
	for i := range s {
		s[i] = RunTag{Key: "t", Value: "v"}
	}
	return s
}

func TestLogBatch_Chunker(t *testing.T) {
	const (
		nMetrics = 2500
		nParams  = 150
		nTags    = 120
	)

	var calls []*logBatchReq
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodPost {
			t.Errorf("expected POST, got %s", method)
		}
		if path != "runs/log-batch" {
			t.Errorf("unexpected path %q", path)
		}
		// in arrives as *logBatchReq; round-trip through JSON to normalise type.
		data, err := json.Marshal(in)
		if err != nil {
			t.Fatalf("marshal chunk: %v", err)
		}
		var req logBatchReq
		if err := json.Unmarshal(data, &req); err != nil {
			t.Fatalf("unmarshal chunk: %v", err)
		}
		calls = append(calls, &req)
		return nil
	})

	metrics := makeMetrics(nMetrics)
	params := makeParams(nParams)
	tags := makeTags(nTags)

	if err := c.LogBatch(context.Background(), "run1", metrics, params, tags); err != nil {
		t.Fatalf("LogBatch: %v", err)
	}

	if len(calls) < 3 {
		t.Fatalf("expected >= 3 calls, got %d", len(calls))
	}

	var totalMetrics, totalParams, totalTags int
	for i, chunk := range calls {
		m, p, tg := len(chunk.Metrics), len(chunk.Params), len(chunk.Tags)
		total := m + p + tg

		if m > 1000 {
			t.Errorf("chunk %d: %d metrics exceeds 1000", i, m)
		}
		if p > 100 {
			t.Errorf("chunk %d: %d params exceeds 100", i, p)
		}
		if tg > 100 {
			t.Errorf("chunk %d: %d tags exceeds 100", i, tg)
		}
		if total > 1000 {
			t.Errorf("chunk %d: total %d exceeds 1000", i, total)
		}

		totalMetrics += m
		totalParams += p
		totalTags += tg
	}

	if totalMetrics != nMetrics {
		t.Errorf("metrics: sent %d, want %d", totalMetrics, nMetrics)
	}
	if totalParams != nParams {
		t.Errorf("params: sent %d, want %d", totalParams, nParams)
	}
	if totalTags != nTags {
		t.Errorf("tags: sent %d, want %d", totalTags, nTags)
	}
}

func TestLogBatch_SingleChunk(t *testing.T) {
	var called int
	c := fakeClient(func(method, path string, in, out any) error {
		called++
		if path != "runs/log-batch" {
			t.Errorf("unexpected path %q", path)
		}
		return nil
	})
	err := c.LogBatch(context.Background(), "r1",
		makeMetrics(3), makeParams(2), makeTags(1))
	if err != nil {
		t.Fatalf("LogBatch: %v", err)
	}
	if called != 1 {
		t.Errorf("expected 1 call, got %d", called)
	}
}

func TestLogBatch_Empty(t *testing.T) {
	var called int
	c := fakeClient(func(method, path string, in, out any) error {
		called++
		return nil
	})
	err := c.LogBatch(context.Background(), "r1", nil, nil, nil)
	if err != nil {
		t.Fatalf("LogBatch: %v", err)
	}
	if called != 0 {
		t.Errorf("expected 0 calls for empty input, got %d", called)
	}
}

func TestLogMetric(t *testing.T) {
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodPost {
			t.Errorf("expected POST, got %s", method)
		}
		if path != "runs/log-metric" {
			t.Errorf("expected runs/log-metric, got %q", path)
		}
		data, _ := json.Marshal(in)
		var body map[string]any
		json.Unmarshal(data, &body)
		if body["run_id"] != "run42" {
			t.Errorf("run_id = %v, want run42", body["run_id"])
		}
		if body["key"] != "loss" {
			t.Errorf("key = %v, want loss", body["key"])
		}
		if body["value"] != 0.5 {
			t.Errorf("value = %v, want 0.5", body["value"])
		}
		if body["timestamp"] != float64(1000) {
			t.Errorf("timestamp = %v, want 1000", body["timestamp"])
		}
		if body["step"] != float64(1) {
			t.Errorf("step = %v, want 1", body["step"])
		}
		return nil
	})
	if err := c.LogMetric(context.Background(), "run42", "loss", 0.5, 1000, 1); err != nil {
		t.Fatalf("LogMetric: %v", err)
	}
}

func TestLogParam(t *testing.T) {
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodPost {
			t.Errorf("expected POST, got %s", method)
		}
		if path != "runs/log-parameter" {
			t.Errorf("expected runs/log-parameter, got %q", path)
		}
		data, _ := json.Marshal(in)
		var body map[string]any
		json.Unmarshal(data, &body)
		if body["run_id"] != "runX" {
			t.Errorf("run_id = %v", body["run_id"])
		}
		if body["key"] != "lr" {
			t.Errorf("key = %v", body["key"])
		}
		if body["value"] != "0.01" {
			t.Errorf("value = %v", body["value"])
		}
		return nil
	})
	if err := c.LogParam(context.Background(), "runX", "lr", "0.01"); err != nil {
		t.Fatalf("LogParam: %v", err)
	}
}

func TestSetTag(t *testing.T) {
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodPost {
			t.Errorf("expected POST, got %s", method)
		}
		if path != "runs/set-tag" {
			t.Errorf("expected runs/set-tag, got %q", path)
		}
		data, _ := json.Marshal(in)
		var body map[string]any
		json.Unmarshal(data, &body)
		if body["run_id"] != "runY" {
			t.Errorf("run_id = %v", body["run_id"])
		}
		if body["key"] != "env" {
			t.Errorf("key = %v", body["key"])
		}
		if body["value"] != "prod" {
			t.Errorf("value = %v", body["value"])
		}
		return nil
	})
	if err := c.SetTag(context.Background(), "runY", "env", "prod"); err != nil {
		t.Fatalf("SetTag: %v", err)
	}
}

func TestLogInputs(t *testing.T) {
	datasets := []DatasetInput{
		{
			Tags: []RunTag{{Key: "context", Value: "train"}},
			Dataset: Dataset{
				Name:       "ds1",
				Digest:     "abc123",
				SourceType: "local",
			},
		},
	}
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodPost {
			t.Errorf("expected POST, got %s", method)
		}
		if path != "runs/log-inputs" {
			t.Errorf("expected runs/log-inputs, got %q", path)
		}
		data, _ := json.Marshal(in)
		var body struct {
			RunID    string         `json:"run_id"`
			Datasets []DatasetInput `json:"datasets"`
		}
		json.Unmarshal(data, &body)
		if body.RunID != "runZ" {
			t.Errorf("run_id = %q", body.RunID)
		}
		if len(body.Datasets) != 1 {
			t.Errorf("datasets len = %d, want 1", len(body.Datasets))
		}
		if body.Datasets[0].Dataset.Name != "ds1" {
			t.Errorf("dataset name = %q", body.Datasets[0].Dataset.Name)
		}
		return nil
	})
	if err := c.LogInputs(context.Background(), "runZ", datasets); err != nil {
		t.Fatalf("LogInputs: %v", err)
	}
}

func TestGetMetricHistory(t *testing.T) {
	want := []Metric{
		{Key: "acc", Value: 0.8, Timestamp: 100, Step: 1},
		{Key: "acc", Value: 0.9, Timestamp: 200, Step: 2},
	}
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodGet {
			t.Errorf("expected GET, got %s", method)
		}
		if !strings.HasPrefix(path, "metrics/get-history?") {
			t.Errorf("unexpected path prefix %q", path)
		}
		if !strings.Contains(path, "run_id=runA") {
			t.Errorf("path missing run_id: %q", path)
		}
		if !strings.Contains(path, "metric_key=accuracy") {
			t.Errorf("path missing metric_key: %q", path)
		}
		if in != nil {
			t.Errorf("expected nil in for GET, got %v", in)
		}
		resp := out.(*struct {
			Metrics []Metric `json:"metrics"`
		})
		resp.Metrics = want
		return nil
	})
	got, err := c.GetMetricHistory(context.Background(), "runA", "accuracy")
	if err != nil {
		t.Fatalf("GetMetricHistory: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d metrics, want %d", len(got), len(want))
	}
	for i, m := range got {
		if m != want[i] {
			t.Errorf("metric[%d] = %+v, want %+v", i, m, want[i])
		}
	}
}
