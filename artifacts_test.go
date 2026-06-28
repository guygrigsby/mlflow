package mlflow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestListArtifacts(t *testing.T) {
	wantFiles := []FileInfo{
		{Path: "model/model.pkl", IsDir: false, FileSize: 1234},
		{Path: "plots", IsDir: true},
	}
	wantToken := "tok-next"
	const runID = "run123"
	const artifactPath = "model"

	c := fakeClient(func(method, path string, in, out any) error {
		if method != "GET" {
			t.Errorf("method = %q, want GET", method)
		}
		u, err := url.Parse(path)
		if err != nil {
			t.Fatalf("failed to parse path %q: %v", path, err)
		}
		if u.Path != "artifacts/list" {
			t.Errorf("path segment = %q, want artifacts/list", u.Path)
		}
		q := u.Query()
		if got := q.Get("run_id"); got != runID {
			t.Errorf("run_id = %q, want %q", got, runID)
		}
		if got := q.Get("path"); got != artifactPath {
			t.Errorf("path param = %q, want %q", got, artifactPath)
		}
		b, _ := json.Marshal(struct {
			Files         []FileInfo `json:"files"`
			NextPageToken string     `json:"next_page_token"`
		}{Files: wantFiles, NextPageToken: wantToken})
		return json.Unmarshal(b, out)
	})

	files, nextToken, err := c.ListArtifacts(context.Background(), runID, artifactPath)
	if err != nil {
		t.Fatalf("ListArtifacts error: %v", err)
	}
	if nextToken != wantToken {
		t.Errorf("nextPageToken = %q, want %q", nextToken, wantToken)
	}
	if len(files) != len(wantFiles) {
		t.Fatalf("len(files) = %d, want %d", len(files), len(wantFiles))
	}
	for i, f := range files {
		if f != wantFiles[i] {
			t.Errorf("files[%d] = %+v, want %+v", i, f, wantFiles[i])
		}
	}
}

func TestListArtifacts_EmptyPath(t *testing.T) {
	const runID = "run456"

	c := fakeClient(func(method, path string, in, out any) error {
		u, err := url.Parse(path)
		if err != nil {
			t.Fatalf("failed to parse path %q: %v", path, err)
		}
		q := u.Query()
		if _, ok := q["path"]; ok {
			t.Errorf("path param should be absent when empty, got %q", q.Get("path"))
		}
		if got := q.Get("run_id"); got != runID {
			t.Errorf("run_id = %q, want %q", got, runID)
		}
		return json.Unmarshal([]byte(`{"files":[]}`), out)
	})

	files, tok, err := c.ListArtifacts(context.Background(), runID, "")
	if err != nil {
		t.Fatalf("ListArtifacts error: %v", err)
	}
	if tok != "" {
		t.Errorf("nextPageToken = %q, want empty", tok)
	}
	if len(files) != 0 {
		t.Errorf("files = %v, want empty", files)
	}
}

func TestDownloadArtifact(t *testing.T) {
	const runID = "run789"
	const artifactPath = "model/weights.bin"
	wantBody := []byte("binary artifact content\x00\x01\x02")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/2.0/mlflow/get-artifact" {
			t.Errorf("request path = %q, want /api/2.0/mlflow/get-artifact", r.URL.Path)
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		q := r.URL.Query()
		if got := q.Get("run_id"); got != runID {
			t.Errorf("run_id = %q, want %q", got, runID)
		}
		if got := q.Get("path"); got != artifactPath {
			t.Errorf("path = %q, want %q", got, artifactPath)
		}
		w.WriteHeader(http.StatusOK)
		w.Write(wantBody) //nolint:errcheck
	}))
	defer srv.Close()

	c, err := NewClient(srv.URL)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	got, err := c.DownloadArtifact(context.Background(), runID, artifactPath)
	if err != nil {
		t.Fatalf("DownloadArtifact error: %v", err)
	}
	if string(got) != string(wantBody) {
		t.Errorf("body = %q, want %q", got, wantBody)
	}
}
