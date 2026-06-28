package mlflow

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
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

func TestParseRetryAfter(t *testing.T) {
	base := time.Date(2025, 10, 21, 7, 28, 0, 0, time.UTC)
	cases := []struct {
		name   string
		in     string
		now    time.Time
		wantD  time.Duration
		wantOK bool
	}{
		{"empty", "", base, 0, false},
		{"seconds", "5", base, 5 * time.Second, true},
		{"seconds with space", " 5 ", base, 5 * time.Second, true},
		{"negative seconds clamps", "-3", base, 0, true},
		{"http-date future", "Tue, 21 Oct 2025 07:28:30 GMT", base, 30 * time.Second, true},
		{"http-date past clamps", "Tue, 21 Oct 2025 07:27:00 GMT", base, 0, true},
		{"garbage", "soon", base, 0, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d, ok := parseRetryAfter(c.in, c.now)
			if ok != c.wantOK || d != c.wantD {
				t.Fatalf("parseRetryAfter(%q) = %v,%v want %v,%v", c.in, d, ok, c.wantD, c.wantOK)
			}
		})
	}
}

func TestRetryOn429ThenSuccess(t *testing.T) {
	var n int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&n, 1) == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{})
	}))
	defer srv.Close()
	c, _ := NewClient(srv.URL)
	if err := c.do(context.Background(), http.MethodGet, "experiments/get", nil, nil); err != nil {
		t.Fatalf("want success after 429 retry, got %v", err)
	}
	if atomic.LoadInt32(&n) != 2 {
		t.Fatalf("attempts = %d, want 2", n)
	}
}

func TestRetryOn503(t *testing.T) {
	var n int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&n, 1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{})
	}))
	defer srv.Close()
	c, _ := NewClient(srv.URL)
	if err := c.do(context.Background(), http.MethodGet, "experiments/get", nil, nil); err != nil {
		t.Fatalf("want success after 503 retry, got %v", err)
	}
	if atomic.LoadInt32(&n) != 2 {
		t.Fatalf("attempts = %d, want 2", n)
	}
}

func TestRetryAfterSecondsWaits(t *testing.T) {
	var n int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&n, 1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{})
	}))
	defer srv.Close()
	c, _ := NewClient(srv.URL)
	start := time.Now()
	if err := c.do(context.Background(), http.MethodGet, "experiments/get", nil, nil); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 900*time.Millisecond {
		t.Fatalf("Retry-After: 1 should pause ~1s, waited %v", elapsed)
	}
}

func TestNon429FourxxNotRetried(t *testing.T) {
	var n int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&n, 1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()
	c, _ := NewClient(srv.URL)
	if err := c.do(context.Background(), http.MethodGet, "experiments/get", nil, nil); err == nil {
		t.Fatal("want error on 400")
	}
	if atomic.LoadInt32(&n) != 1 {
		t.Fatalf("400 must not retry, attempts = %d", n)
	}
}

func TestCtxCancelDuringRetryAfterWait(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()
	c, _ := NewClient(srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := c.do(ctx, http.MethodGet, "experiments/get", nil, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want context deadline error, got %v", err)
	}
}
