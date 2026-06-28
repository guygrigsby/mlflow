package mlflow

import (
	"context"
	"net/http"
	"net/url"
)

type runCreate struct {
	ExperimentID string   `json:"experiment_id"`
	RunName      string   `json:"run_name,omitempty"`
	StartTime    int64    `json:"start_time,omitempty"`
	Tags         []RunTag `json:"tags,omitempty"`
}

// RunOption configures a CreateRun request.
type RunOption func(*runCreate)

// WithRunName sets the run name.
func WithRunName(name string) RunOption {
	return func(r *runCreate) { r.RunName = name }
}

// WithStartTime sets the run start time as unix milliseconds.
func WithStartTime(unixMs int64) RunOption {
	return func(r *runCreate) { r.StartTime = unixMs }
}

// WithRunTags appends tags to the run.
func WithRunTags(tags ...RunTag) RunOption {
	return func(r *runCreate) { r.Tags = append(r.Tags, tags...) }
}

// CreateRun creates a new run under experimentID.
func (c *Client) CreateRun(ctx context.Context, experimentID string, opts ...RunOption) (*Run, error) {
	rc := runCreate{ExperimentID: experimentID}
	for _, o := range opts {
		o(&rc)
	}
	var resp struct {
		Run Run `json:"run"`
	}
	if err := c.do(ctx, http.MethodPost, "runs/create", &rc, &resp); err != nil {
		return nil, err
	}
	return &resp.Run, nil
}

// GetRun fetches a run by ID.
func (c *Client) GetRun(ctx context.Context, runID string) (*Run, error) {
	path := "runs/get?run_id=" + url.QueryEscape(runID)
	var resp struct {
		Run Run `json:"run"`
	}
	if err := c.do(ctx, http.MethodGet, path, nil, &resp); err != nil {
		return nil, err
	}
	return &resp.Run, nil
}

type runUpdate struct {
	RunID   string    `json:"run_id"`
	Status  RunStatus `json:"status"`
	EndTime int64     `json:"end_time,omitempty"`
}

// UpdateRun sets the status and optionally the end time of a run.
// endTime is included only when nonzero.
func (c *Client) UpdateRun(ctx context.Context, runID string, status RunStatus, endTime int64) error {
	req := runUpdate{RunID: runID, Status: status, EndTime: endTime}
	return c.do(ctx, http.MethodPost, "runs/update", &req, nil)
}

// DeleteRun soft-deletes a run.
func (c *Client) DeleteRun(ctx context.Context, runID string) error {
	return c.do(ctx, http.MethodPost, "runs/delete", struct {
		RunID string `json:"run_id"`
	}{RunID: runID}, nil)
}

// RestoreRun undeletes a previously deleted run.
func (c *Client) RestoreRun(ctx context.Context, runID string) error {
	return c.do(ctx, http.MethodPost, "runs/restore", struct {
		RunID string `json:"run_id"`
	}{RunID: runID}, nil)
}

// SetRunTag sets a tag on a run.
func (c *Client) SetRunTag(ctx context.Context, runID, key, value string) error {
	return c.do(ctx, http.MethodPost, "runs/set-tag", struct {
		RunID string `json:"run_id"`
		Key   string `json:"key"`
		Value string `json:"value"`
	}{RunID: runID, Key: key, Value: value}, nil)
}

// DeleteRunTag removes a tag from a run.
func (c *Client) DeleteRunTag(ctx context.Context, runID, key string) error {
	return c.do(ctx, http.MethodPost, "runs/delete-tag", struct {
		RunID string `json:"run_id"`
		Key   string `json:"key"`
	}{RunID: runID, Key: key}, nil)
}

// SearchRuns searches runs matching req. Returns the matching runs and an
// opaque next-page token (empty when no further pages exist).
func (c *Client) SearchRuns(ctx context.Context, req SearchRunsRequest) ([]Run, string, error) {
	var resp struct {
		Runs          []Run  `json:"runs"`
		NextPageToken string `json:"next_page_token"`
	}
	if err := c.do(ctx, http.MethodPost, "runs/search", &req, &resp); err != nil {
		return nil, "", err
	}
	return resp.Runs, resp.NextPageToken, nil
}
