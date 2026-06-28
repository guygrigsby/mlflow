package mlflow

import (
	"context"
	"fmt"
	"net/url"
)

// ListArtifacts returns the artifact listing for runID under path.
// Pass path=="" to list the root. nextPageToken is non-empty when more pages remain.
func (c *Client) ListArtifacts(ctx context.Context, runID, path string) (files []FileInfo, nextPageToken string, err error) {
	q := url.Values{}
	q.Set("run_id", runID)
	if path != "" {
		q.Set("path", path)
	}
	var resp struct {
		Files         []FileInfo `json:"files"`
		NextPageToken string     `json:"next_page_token"`
	}
	if err = c.do(ctx, "GET", "artifacts/list?"+q.Encode(), nil, &resp); err != nil {
		return nil, "", err
	}
	return resp.Files, resp.NextPageToken, nil
}

// DownloadArtifact fetches the raw bytes of a single artifact file.
func (c *Client) DownloadArtifact(ctx context.Context, runID, path string) ([]byte, error) {
	if c.raw == nil {
		return nil, fmt.Errorf("mlflow: raw transport not wired")
	}
	q := url.Values{}
	q.Set("run_id", runID)
	q.Set("path", path)
	return c.raw(ctx, "get-artifact?"+q.Encode())
}
