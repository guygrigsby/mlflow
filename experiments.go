package mlflow

import (
	"context"
	"net/http"
	"net/url"
)

// CreateExperiment creates a new experiment with the given name and optional tags.
// Returns the new experiment's ID.
func (c *Client) CreateExperiment(ctx context.Context, name string, tags ...ExperimentTag) (string, error) {
	req := struct {
		Name string          `json:"name"`
		Tags []ExperimentTag `json:"tags,omitempty"`
	}{Name: name, Tags: tags}
	var resp struct {
		ExperimentID string `json:"experiment_id"`
	}
	if err := c.do(ctx, http.MethodPost, "experiments/create", req, &resp); err != nil {
		return "", err
	}
	return resp.ExperimentID, nil
}

// GetExperimentByName fetches an experiment by name.
func (c *Client) GetExperimentByName(ctx context.Context, name string) (*Experiment, error) {
	path := "experiments/get-by-name?experiment_name=" + url.QueryEscape(name)
	var resp struct {
		Experiment Experiment `json:"experiment"`
	}
	if err := c.do(ctx, http.MethodGet, path, nil, &resp); err != nil {
		return nil, err
	}
	return &resp.Experiment, nil
}

// GetOrCreateExperiment returns the ID of an existing experiment by name, creating
// it if it does not exist.
func (c *Client) GetOrCreateExperiment(ctx context.Context, name string) (string, error) {
	exp, err := c.GetExperimentByName(ctx, name)
	if err == nil {
		return exp.ExperimentID, nil
	}
	if IsNotFound(err) {
		return c.CreateExperiment(ctx, name)
	}
	return "", err
}

// UpdateExperiment renames an experiment.
func (c *Client) UpdateExperiment(ctx context.Context, id, newName string) error {
	req := struct {
		ExperimentID string `json:"experiment_id"`
		NewName      string `json:"new_name"`
	}{ExperimentID: id, NewName: newName}
	return c.do(ctx, http.MethodPost, "experiments/update", req, nil)
}

// DeleteExperiment marks an experiment as deleted.
func (c *Client) DeleteExperiment(ctx context.Context, id string) error {
	req := struct {
		ExperimentID string `json:"experiment_id"`
	}{ExperimentID: id}
	return c.do(ctx, http.MethodPost, "experiments/delete", req, nil)
}

// RestoreExperiment restores a deleted experiment.
func (c *Client) RestoreExperiment(ctx context.Context, id string) error {
	req := struct {
		ExperimentID string `json:"experiment_id"`
	}{ExperimentID: id}
	return c.do(ctx, http.MethodPost, "experiments/restore", req, nil)
}

// SetExperimentTag sets a tag on an experiment.
func (c *Client) SetExperimentTag(ctx context.Context, id, key, value string) error {
	req := struct {
		ExperimentID string `json:"experiment_id"`
		Key          string `json:"key"`
		Value        string `json:"value"`
	}{ExperimentID: id, Key: key, Value: value}
	return c.do(ctx, http.MethodPost, "experiments/set-experiment-tag", req, nil)
}

// SearchExperiments searches experiments with the given criteria and returns
// matching experiments and the next-page token (empty string when no more pages).
func (c *Client) SearchExperiments(ctx context.Context, req SearchExperimentsRequest) ([]Experiment, string, error) {
	var resp struct {
		Experiments   []Experiment `json:"experiments"`
		NextPageToken string       `json:"next_page_token"`
	}
	if err := c.do(ctx, http.MethodPost, "experiments/search", req, &resp); err != nil {
		return nil, "", err
	}
	return resp.Experiments, resp.NextPageToken, nil
}
