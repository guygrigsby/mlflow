package mlflow

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
)

// --- Registered Models ---

// CreateRegisteredModel creates a new registered model with the given name and
// optional tags.
func (c *Client) CreateRegisteredModel(ctx context.Context, name string, tags ...RegisteredModelTag) (*RegisteredModel, error) {
	req := struct {
		Name string               `json:"name"`
		Tags []RegisteredModelTag `json:"tags,omitempty"`
	}{Name: name, Tags: tags}
	var resp struct {
		RegisteredModel *RegisteredModel `json:"registered_model"`
	}
	if err := c.do(ctx, http.MethodPost, "registered-models/create", req, &resp); err != nil {
		return nil, err
	}
	return resp.RegisteredModel, nil
}

// GetRegisteredModel retrieves a registered model by name.
func (c *Client) GetRegisteredModel(ctx context.Context, name string) (*RegisteredModel, error) {
	path := "registered-models/get?" + url.Values{"name": {name}}.Encode()
	var resp struct {
		RegisteredModel *RegisteredModel `json:"registered_model"`
	}
	if err := c.do(ctx, http.MethodGet, path, nil, &resp); err != nil {
		return nil, err
	}
	return resp.RegisteredModel, nil
}

// RenameRegisteredModel renames a registered model.
func (c *Client) RenameRegisteredModel(ctx context.Context, name, newName string) (*RegisteredModel, error) {
	req := struct {
		Name    string `json:"name"`
		NewName string `json:"new_name"`
	}{Name: name, NewName: newName}
	var resp struct {
		RegisteredModel *RegisteredModel `json:"registered_model"`
	}
	if err := c.do(ctx, http.MethodPost, "registered-models/rename", req, &resp); err != nil {
		return nil, err
	}
	return resp.RegisteredModel, nil
}

// UpdateRegisteredModel updates the description of a registered model.
func (c *Client) UpdateRegisteredModel(ctx context.Context, name, description string) (*RegisteredModel, error) {
	req := struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}{Name: name, Description: description}
	var resp struct {
		RegisteredModel *RegisteredModel `json:"registered_model"`
	}
	if err := c.do(ctx, http.MethodPost, "registered-models/update", req, &resp); err != nil {
		return nil, err
	}
	return resp.RegisteredModel, nil
}

// DeleteRegisteredModel deletes a registered model by name.
func (c *Client) DeleteRegisteredModel(ctx context.Context, name string) error {
	req := struct {
		Name string `json:"name"`
	}{Name: name}
	return c.do(ctx, http.MethodPost, "registered-models/delete", req, nil)
}

// SearchRegisteredModels searches registered models with optional filter, ordering,
// and pagination. Returns the matching models and the next page token (empty when
// there are no more pages).
func (c *Client) SearchRegisteredModels(ctx context.Context, filter string, maxResults int64, orderBy []string, pageToken string) ([]RegisteredModel, string, error) {
	q := url.Values{}
	if filter != "" {
		q.Set("filter", filter)
	}
	if maxResults > 0 {
		q.Set("max_results", strconv.FormatInt(maxResults, 10))
	}
	for _, o := range orderBy {
		q.Add("order_by", o)
	}
	if pageToken != "" {
		q.Set("page_token", pageToken)
	}
	path := "registered-models/search"
	if enc := q.Encode(); enc != "" {
		path += "?" + enc
	}
	var resp struct {
		RegisteredModels []RegisteredModel `json:"registered_models"`
		NextPageToken    string            `json:"next_page_token"`
	}
	if err := c.do(ctx, http.MethodGet, path, nil, &resp); err != nil {
		return nil, "", err
	}
	return resp.RegisteredModels, resp.NextPageToken, nil
}

// SetRegisteredModelTag sets a tag on a registered model.
func (c *Client) SetRegisteredModelTag(ctx context.Context, name, key, value string) error {
	req := struct {
		Name  string `json:"name"`
		Key   string `json:"key"`
		Value string `json:"value"`
	}{Name: name, Key: key, Value: value}
	return c.do(ctx, http.MethodPost, "registered-models/set-tag", req, nil)
}

// DeleteRegisteredModelTag deletes a tag from a registered model.
func (c *Client) DeleteRegisteredModelTag(ctx context.Context, name, key string) error {
	req := struct {
		Name string `json:"name"`
		Key  string `json:"key"`
	}{Name: name, Key: key}
	return c.do(ctx, http.MethodPost, "registered-models/delete-tag", req, nil)
}

// GetLatestVersions returns the latest model versions for a registered model,
// optionally filtered to the given stages.
func (c *Client) GetLatestVersions(ctx context.Context, name string, stages ...string) ([]ModelVersion, error) {
	req := struct {
		Name   string   `json:"name"`
		Stages []string `json:"stages,omitempty"`
	}{Name: name, Stages: stages}
	var resp struct {
		ModelVersions []ModelVersion `json:"model_versions"`
	}
	if err := c.do(ctx, http.MethodPost, "registered-models/get-latest-versions", req, &resp); err != nil {
		return nil, err
	}
	return resp.ModelVersions, nil
}

// --- Model Versions ---

// CreateModelVersion registers a new model version. runID may be empty.
func (c *Client) CreateModelVersion(ctx context.Context, name, source string, runID string, tags ...ModelVersionTag) (*ModelVersion, error) {
	req := struct {
		Name   string            `json:"name"`
		Source string            `json:"source"`
		RunID  string            `json:"run_id,omitempty"`
		Tags   []ModelVersionTag `json:"tags,omitempty"`
	}{Name: name, Source: source, RunID: runID, Tags: tags}
	var resp struct {
		ModelVersion *ModelVersion `json:"model_version"`
	}
	if err := c.do(ctx, http.MethodPost, "model-versions/create", req, &resp); err != nil {
		return nil, err
	}
	return resp.ModelVersion, nil
}

// GetModelVersion retrieves a specific model version by name and version number.
func (c *Client) GetModelVersion(ctx context.Context, name, version string) (*ModelVersion, error) {
	q := url.Values{"name": {name}, "version": {version}}
	path := "model-versions/get?" + q.Encode()
	var resp struct {
		ModelVersion *ModelVersion `json:"model_version"`
	}
	if err := c.do(ctx, http.MethodGet, path, nil, &resp); err != nil {
		return nil, err
	}
	return resp.ModelVersion, nil
}

// UpdateModelVersion updates the description of a model version.
func (c *Client) UpdateModelVersion(ctx context.Context, name, version, description string) (*ModelVersion, error) {
	req := struct {
		Name        string `json:"name"`
		Version     string `json:"version"`
		Description string `json:"description"`
	}{Name: name, Version: version, Description: description}
	var resp struct {
		ModelVersion *ModelVersion `json:"model_version"`
	}
	if err := c.do(ctx, http.MethodPost, "model-versions/update", req, &resp); err != nil {
		return nil, err
	}
	return resp.ModelVersion, nil
}

// DeleteModelVersion deletes a specific model version.
func (c *Client) DeleteModelVersion(ctx context.Context, name, version string) error {
	req := struct {
		Name    string `json:"name"`
		Version string `json:"version"`
	}{Name: name, Version: version}
	return c.do(ctx, http.MethodPost, "model-versions/delete", req, nil)
}

// SearchModelVersions searches model versions with optional filter, ordering, and
// pagination. Returns the matching versions and the next page token.
func (c *Client) SearchModelVersions(ctx context.Context, filter string, maxResults int64, orderBy []string, pageToken string) ([]ModelVersion, string, error) {
	q := url.Values{}
	if filter != "" {
		q.Set("filter", filter)
	}
	if maxResults > 0 {
		q.Set("max_results", strconv.FormatInt(maxResults, 10))
	}
	for _, o := range orderBy {
		q.Add("order_by", o)
	}
	if pageToken != "" {
		q.Set("page_token", pageToken)
	}
	path := "model-versions/search"
	if enc := q.Encode(); enc != "" {
		path += "?" + enc
	}
	var resp struct {
		ModelVersions []ModelVersion `json:"model_versions"`
		NextPageToken string         `json:"next_page_token"`
	}
	if err := c.do(ctx, http.MethodGet, path, nil, &resp); err != nil {
		return nil, "", err
	}
	return resp.ModelVersions, resp.NextPageToken, nil
}

// SetModelVersionTag sets a tag on a model version.
func (c *Client) SetModelVersionTag(ctx context.Context, name, version, key, value string) error {
	req := struct {
		Name    string `json:"name"`
		Version string `json:"version"`
		Key     string `json:"key"`
		Value   string `json:"value"`
	}{Name: name, Version: version, Key: key, Value: value}
	return c.do(ctx, http.MethodPost, "model-versions/set-tag", req, nil)
}

// DeleteModelVersionTag deletes a tag from a model version.
func (c *Client) DeleteModelVersionTag(ctx context.Context, name, version, key string) error {
	req := struct {
		Name    string `json:"name"`
		Version string `json:"version"`
		Key     string `json:"key"`
	}{Name: name, Version: version, Key: key}
	return c.do(ctx, http.MethodPost, "model-versions/delete-tag", req, nil)
}

// TransitionModelVersionStage transitions a model version to a new stage. When
// archiveExisting is true, all other versions in the target stage are archived.
func (c *Client) TransitionModelVersionStage(ctx context.Context, name, version, stage string, archiveExisting bool) (*ModelVersion, error) {
	req := struct {
		Name                    string `json:"name"`
		Version                 string `json:"version"`
		Stage                   string `json:"stage"`
		ArchiveExistingVersions bool   `json:"archive_existing_versions"`
	}{Name: name, Version: version, Stage: stage, ArchiveExistingVersions: archiveExisting}
	var resp struct {
		ModelVersion *ModelVersion `json:"model_version"`
	}
	if err := c.do(ctx, http.MethodPost, "model-versions/transition-stage", req, &resp); err != nil {
		return nil, err
	}
	return resp.ModelVersion, nil
}

// GetModelVersionDownloadURI returns the URI for downloading a model version's
// artifacts.
func (c *Client) GetModelVersionDownloadURI(ctx context.Context, name, version string) (string, error) {
	q := url.Values{"name": {name}, "version": {version}}
	path := "model-versions/get-download-uri?" + q.Encode()
	var resp struct {
		ArtifactURI string `json:"artifact_uri"`
	}
	if err := c.do(ctx, http.MethodGet, path, nil, &resp); err != nil {
		return "", err
	}
	return resp.ArtifactURI, nil
}

// --- Aliases ---

// SetRegisteredModelAlias creates or updates an alias pointing to a specific
// model version.
func (c *Client) SetRegisteredModelAlias(ctx context.Context, name, alias, version string) error {
	req := struct {
		Name    string `json:"name"`
		Alias   string `json:"alias"`
		Version string `json:"version"`
	}{Name: name, Alias: alias, Version: version}
	return c.do(ctx, http.MethodPost, "registered-models/alias", req, nil)
}

// DeleteRegisteredModelAlias removes an alias from a registered model.
func (c *Client) DeleteRegisteredModelAlias(ctx context.Context, name, alias string) error {
	q := url.Values{"alias": {alias}, "name": {name}}
	path := "registered-models/alias?" + q.Encode()
	return c.do(ctx, http.MethodDelete, path, nil, nil)
}

// GetModelVersionByAlias retrieves the model version that the given alias points
// to.
func (c *Client) GetModelVersionByAlias(ctx context.Context, name, alias string) (*ModelVersion, error) {
	q := url.Values{"alias": {alias}, "name": {name}}
	path := "registered-models/alias?" + q.Encode()
	var resp struct {
		ModelVersion *ModelVersion `json:"model_version"`
	}
	if err := c.do(ctx, http.MethodGet, path, nil, &resp); err != nil {
		return nil, err
	}
	return resp.ModelVersion, nil
}
