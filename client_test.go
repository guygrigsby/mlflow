package mlflow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestDoDecodesOKAndBuildsPath(t *testing.T) {
	var gotPath, gotAuth, gotCT string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotCT = r.Header.Get("Content-Type")
		json.NewEncoder(w).Encode(map[string]string{"experiment_id": "7"})
	}))
	defer srv.Close()

	c, err := NewClient(srv.URL+"/", WithBearerToken("tok")) // trailing slash trimmed
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		ExperimentID string `json:"experiment_id"`
	}
	if err := c.do(context.Background(), http.MethodPost, "experiments/create", map[string]string{"name": "x"}, &out); err != nil {
		t.Fatal(err)
	}
	if gotPath != "/api/2.0/mlflow/experiments/create" {
		t.Fatalf("path = %q", gotPath)
	}
	if gotAuth != "Bearer tok" {
		t.Fatalf("auth = %q", gotAuth)
	}
	if gotCT != "application/json" {
		t.Fatalf("content-type = %q", gotCT)
	}
	if out.ExperimentID != "7" {
		t.Fatalf("decoded = %q", out.ExperimentID)
	}
}

func TestDoBasicAuth(t *testing.T) {
	var u, p string
	var ok bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok = r.BasicAuth()
		json.NewEncoder(w).Encode(map[string]any{})
	}))
	defer srv.Close()
	c, _ := NewClient(srv.URL, WithBasicAuth("alice", "secret"))
	if err := c.do(context.Background(), http.MethodGet, "experiments/get", nil, nil); err != nil {
		t.Fatal(err)
	}
	if !ok || u != "alice" || p != "secret" {
		t.Fatalf("basic auth = %q/%q ok=%v", u, p, ok)
	}
}

func TestDoGETHasNoBody(t *testing.T) {
	var ct string
	var n int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ct = r.Header.Get("Content-Type")
		n = r.ContentLength
		json.NewEncoder(w).Encode(map[string]any{})
	}))
	defer srv.Close()
	c, _ := NewClient(srv.URL)
	if err := c.do(context.Background(), http.MethodGet, "runs/get?run_id=1", nil, nil); err != nil {
		t.Fatal(err)
	}
	if ct != "" || n > 0 {
		t.Fatalf("GET should send no body: ct=%q len=%d", ct, n)
	}
}

func TestDoMapsAPIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
		json.NewEncoder(w).Encode(map[string]string{
			"error_code": "RESOURCE_DOES_NOT_EXIST", "message": "nope"})
	}))
	defer srv.Close()
	c, _ := NewClient(srv.URL)
	err := c.do(context.Background(), http.MethodGet, "runs/get", nil, nil)
	if !IsNotFound(err) {
		t.Fatalf("want IsNotFound, got %v", err)
	}
}

func TestDoRetries5xxThenSucceeds(t *testing.T) {
	n := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n++
		if n < 3 {
			w.WriteHeader(503)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{})
	}))
	defer srv.Close()
	c, _ := NewClient(srv.URL, WithMaxRetries(3))
	if err := c.do(context.Background(), http.MethodPost, "runs/log-batch", map[string]any{}, nil); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("attempts = %d, want 3", n)
	}
}

func TestDoRetriesExhaustReturnsAPIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		json.NewEncoder(w).Encode(map[string]string{"error_code": "INTERNAL_ERROR", "message": "boom"})
	}))
	defer srv.Close()
	c, _ := NewClient(srv.URL, WithMaxRetries(1))
	err := c.do(context.Background(), http.MethodPost, "runs/create", map[string]any{}, nil)
	var ae *APIError
	if err == nil {
		t.Fatal("want error after exhausting retries")
	}
	if !asAPIError(err, &ae) || ae.HTTPStatus != 500 {
		t.Fatalf("want *APIError 500, got %v", err)
	}
}

func TestDoDoesNotRetry4xx(t *testing.T) {
	n := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n++
		w.WriteHeader(400)
		json.NewEncoder(w).Encode(map[string]string{"error_code": "INVALID_PARAMETER_VALUE", "message": "bad"})
	}))
	defer srv.Close()
	c, _ := NewClient(srv.URL, WithMaxRetries(3))
	_ = c.do(context.Background(), http.MethodPost, "runs/create", map[string]any{}, nil)
	if n != 1 {
		t.Fatalf("attempts = %d, want 1 (no retry on 4xx)", n)
	}
}

func TestRawReturnsBytes(t *testing.T) {
	want := []byte("\x00\x01rawbytes")
	var gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		w.Write(want)
	}))
	defer srv.Close()
	c, _ := NewClient(srv.URL)
	got, err := c.raw(context.Background(), "get-artifact?run_id=r1&path=a/b")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("raw bytes = %q", got)
	}
	if gotQuery != "run_id=r1&path=a/b" {
		t.Fatalf("query = %q", gotQuery)
	}
}

func TestNewClientReadsEnv(t *testing.T) {
	t.Setenv("MLFLOW_TRACKING_URI", "http://mlflow.example.com:5000")
	c, err := NewClient("")
	if err != nil || c == nil {
		t.Fatalf("env URI: %v", err)
	}
	os.Unsetenv("MLFLOW_TRACKING_URI")
	if _, err := NewClient(""); err == nil {
		t.Fatal("empty URI + no env should error")
	}
}

// asAPIError is a tiny test helper to avoid importing errors here.
func asAPIError(err error, target **APIError) bool {
	if ae, ok := err.(*APIError); ok {
		*target = ae
		return true
	}
	return false
}
