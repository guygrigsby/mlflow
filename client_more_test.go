package mlflow

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// roundTripFunc adapts a function to http.RoundTripper for simulating transport
// behavior without a server.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestDoRetriesTransportError(t *testing.T) {
	n := 0
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		n++
		if n < 3 {
			return nil, errors.New("dial tcp: connection refused")
		}
		return &http.Response{
			StatusCode: 200,
			Body:       http.NoBody,
			Header:     make(http.Header),
		}, nil
	})
	c, _ := NewClient("http://x", WithHTTPClient(&http.Client{Transport: rt}), WithMaxRetries(3))
	if err := c.do(context.Background(), http.MethodPost, "runs/create", map[string]any{}, nil); err != nil {
		t.Fatalf("want success after transport retries, got %v", err)
	}
	if n != 3 {
		t.Fatalf("attempts = %d, want 3", n)
	}
}

func TestDoTransportErrorExhaustsRetries(t *testing.T) {
	want := errors.New("connection refused")
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) { return nil, want })
	c, _ := NewClient("http://x", WithHTTPClient(&http.Client{Transport: rt}), WithMaxRetries(1))
	err := c.do(context.Background(), http.MethodGet, "runs/get", nil, nil)
	if err == nil {
		t.Fatal("want error after exhausting retries on transport failure")
	}
	if !errors.Is(err, want) {
		t.Fatalf("want wrapped %v, got %v", want, err)
	}
}

func TestDoHonorsContextCancelDuringBackoff(t *testing.T) {
	// Server always 503, so the client always wants to retry; the cancelled
	// context must break the backoff sleep and return promptly.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(503)
	}))
	defer srv.Close()
	c, _ := NewClient(srv.URL, WithMaxRetries(100)) // would loop ~forever without ctx
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := c.do(ctx, http.MethodPost, "runs/log-batch", map[string]any{}, nil)
	if err == nil {
		t.Fatal("want context error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("did not honor ctx during backoff: took %v", elapsed)
	}
}

func TestDoDecodeErrorOn2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("{not valid json"))
	}))
	defer srv.Close()
	c, _ := NewClient(srv.URL)
	var out struct {
		X string `json:"x"`
	}
	err := c.do(context.Background(), http.MethodGet, "experiments/get", nil, &out)
	if err == nil {
		t.Fatal("want decode error on malformed 2xx body")
	}
}

func TestWithHTTPClientIsUsed(t *testing.T) {
	used := false
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		used = true
		return &http.Response{StatusCode: 200, Body: http.NoBody, Header: make(http.Header)}, nil
	})
	c, _ := NewClient("http://x", WithHTTPClient(&http.Client{Transport: rt}))
	if err := c.do(context.Background(), http.MethodGet, "runs/get", nil, nil); err != nil {
		t.Fatal(err)
	}
	if !used {
		t.Fatal("WithHTTPClient transport was not used")
	}
}
