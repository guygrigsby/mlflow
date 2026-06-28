package mlflow

import (
	"context"
	"net/http"
	"net/url"
)

type logBatchReq struct {
	RunID   string   `json:"run_id"`
	Metrics []Metric `json:"metrics,omitempty"`
	Params  []Param  `json:"params,omitempty"`
	Tags    []RunTag `json:"tags,omitempty"`
}

// LogBatch posts metrics, params, and tags in chunks that satisfy the MLflow
// API limits per call: ≤1000 metrics, ≤100 params, ≤100 tags, ≤1000 total.
// Chunks are sent sequentially; the first error aborts and is returned.
func (c *Client) LogBatch(ctx context.Context, runID string, metrics []Metric, params []Param, tags []RunTag) error {
	const (
		maxMetrics = 1000
		maxParams  = 100
		maxTags    = 100
		maxTotal   = 1000
	)
	mi, pi, ti := 0, 0, 0
	for mi < len(metrics) || pi < len(params) || ti < len(tags) {
		budget := maxTotal

		mCount := min(maxMetrics, budget, len(metrics)-mi)
		budget -= mCount

		pCount := min(maxParams, budget, len(params)-pi)
		budget -= pCount

		tCount := min(maxTags, budget, len(tags)-ti)

		chunk := &logBatchReq{RunID: runID}
		if mCount > 0 {
			chunk.Metrics = metrics[mi : mi+mCount]
		}
		if pCount > 0 {
			chunk.Params = params[pi : pi+pCount]
		}
		if tCount > 0 {
			chunk.Tags = tags[ti : ti+tCount]
		}
		if err := c.do(ctx, http.MethodPost, "runs/log-batch", chunk, nil); err != nil {
			return err
		}
		mi += mCount
		pi += pCount
		ti += tCount
	}
	return nil
}

// LogMetric posts a single metric value to runs/log-metric.
func (c *Client) LogMetric(ctx context.Context, runID, key string, value float64, timestampMs, step int64) error {
	body := struct {
		RunID     string  `json:"run_id"`
		Key       string  `json:"key"`
		Value     float64 `json:"value"`
		Timestamp int64   `json:"timestamp"`
		Step      int64   `json:"step"`
	}{RunID: runID, Key: key, Value: value, Timestamp: timestampMs, Step: step}
	return c.do(ctx, http.MethodPost, "runs/log-metric", body, nil)
}

// LogParam posts a single parameter to runs/log-parameter.
func (c *Client) LogParam(ctx context.Context, runID, key, value string) error {
	body := struct {
		RunID string `json:"run_id"`
		Key   string `json:"key"`
		Value string `json:"value"`
	}{RunID: runID, Key: key, Value: value}
	return c.do(ctx, http.MethodPost, "runs/log-parameter", body, nil)
}

// SetTag posts a single run tag to runs/set-tag. It is an alias for SetRunTag.
func (c *Client) SetTag(ctx context.Context, runID, key, value string) error {
	return c.SetRunTag(ctx, runID, key, value)
}

// LogInputs posts dataset inputs to runs/log-inputs.
func (c *Client) LogInputs(ctx context.Context, runID string, datasets []DatasetInput) error {
	body := struct {
		RunID    string         `json:"run_id"`
		Datasets []DatasetInput `json:"datasets"`
	}{RunID: runID, Datasets: datasets}
	return c.do(ctx, http.MethodPost, "runs/log-inputs", body, nil)
}

// GetMetricHistory returns the full history of a metric from metrics/get-history.
func (c *Client) GetMetricHistory(ctx context.Context, runID, metricKey string) ([]Metric, error) {
	q := url.Values{"run_id": {runID}, "metric_key": {metricKey}}
	path := "metrics/get-history?" + q.Encode()
	var resp struct {
		Metrics []Metric `json:"metrics"`
	}
	if err := c.do(ctx, http.MethodGet, path, nil, &resp); err != nil {
		return nil, err
	}
	return resp.Metrics, nil
}
