package mlflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strings"
	"time"
)

// transport is the injectable seam. NewClient sets Client.do to the real HTTP
// implementation; tests set it to a fake so methods are unit-testable without a
// server. API methods call c.do exclusively.
type transport func(ctx context.Context, method, path string, in, out any) error

type Client struct {
	do  transport                                              // never nil after NewClient
	raw func(ctx context.Context, path string) ([]byte, error) // raw GET, used by DownloadArtifact
}

type config struct {
	httpc      *http.Client
	bearer     string
	user, pass string
	maxRetries int
}

type Option func(*config)

// WithHTTPClient overrides the default &http.Client{Timeout: 30s}.
func WithHTTPClient(h *http.Client) Option { return func(c *config) { c.httpc = h } }

// WithBearerToken sets an Authorization: Bearer <tok> header on every request.
func WithBearerToken(tok string) Option { return func(c *config) { c.bearer = tok } }

// WithBasicAuth sets HTTP basic auth on every request.
func WithBasicAuth(u, p string) Option { return func(c *config) { c.user, c.pass = u, p } }

// WithMaxRetries sets the retry budget for 5xx + transport errors (default 3).
// Retries are not idempotent-aware: a write whose response is lost in transit
// (transport error or 5xx after the server applied it) may be re-sent, so a
// retried CreateRun/LogMetric/LogBatch can double-write. This is acceptable
// under the best-effort, accept-gaps contract; set 0 to disable.
func WithMaxRetries(n int) Option { return func(c *config) { c.maxRetries = n } }

// NewClient builds a Client. trackingURI == "" reads MLFLOW_TRACKING_URI from the
// environment; empty in both is an error. A trailing slash is trimmed and
// "/api/2.0/mlflow/<path>" is appended by the transport.
func NewClient(trackingURI string, opts ...Option) (*Client, error) {
	if trackingURI == "" {
		trackingURI = os.Getenv("MLFLOW_TRACKING_URI")
	}
	if trackingURI == "" {
		return nil, errors.New("mlflow: no tracking URI (arg empty and MLFLOW_TRACKING_URI unset)")
	}
	cfg := config{httpc: &http.Client{Timeout: 30 * time.Second}, maxRetries: 3}
	for _, o := range opts {
		o(&cfg)
	}
	base := strings.TrimRight(trackingURI, "/")
	c := &Client{}
	c.do = func(ctx context.Context, method, path string, in, out any) error {
		return httpDo(ctx, cfg, base, method, path, in, out)
	}
	c.raw = func(ctx context.Context, path string) ([]byte, error) {
		data, status, err := doRequest(ctx, cfg, base, http.MethodGet, path, nil)
		if err != nil {
			return nil, err
		}
		if status/100 == 2 {
			return data, nil
		}
		return nil, apiError(data, status)
	}
	return c, nil
}

// doRequest performs the URL build, auth, and retry loop, returning the final
// response body and status. It retries 5xx and transport errors with exponential
// backoff up to cfg.maxRetries; 4xx is returned immediately (no retry).
func doRequest(ctx context.Context, cfg config, base, method, path string, body []byte) ([]byte, int, error) {
	url := base + "/api/2.0/mlflow/" + path
	var lastErr error
	for attempt := 0; attempt <= cfg.maxRetries; attempt++ {
		if attempt > 0 {
			d := time.Duration(math.Min(float64(time.Second)*math.Pow(2, float64(attempt-1)), float64(10*time.Second)))
			select {
			case <-time.After(d):
			case <-ctx.Done():
				return nil, 0, ctx.Err()
			}
		}
		req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
		if err != nil {
			return nil, 0, err
		}
		if len(body) > 0 {
			req.Header.Set("Content-Type", "application/json")
		}
		if cfg.bearer != "" {
			req.Header.Set("Authorization", "Bearer "+cfg.bearer)
		} else if cfg.user != "" {
			req.SetBasicAuth(cfg.user, cfg.pass)
		}
		resp, err := cfg.httpc.Do(req)
		if err != nil {
			lastErr = err // transport error: retry
			continue
		}
		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode/100 == 5 {
			lastErr = apiError(data, resp.StatusCode) // server error: retry
			continue
		}
		return data, resp.StatusCode, nil
	}
	return nil, 0, lastErr
}

// httpDo is the real transport: marshal in, POST/GET, decode 2xx into out, map
// non-2xx into *APIError.
func httpDo(ctx context.Context, cfg config, base, method, path string, in, out any) error {
	var body []byte
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return fmt.Errorf("mlflow: marshal request: %w", err)
		}
		body = b
	}
	data, status, err := doRequest(ctx, cfg, base, method, path, body)
	if err != nil {
		return err
	}
	if status/100 == 2 {
		if out != nil && len(data) > 0 {
			if err := json.Unmarshal(data, out); err != nil {
				return fmt.Errorf("mlflow: decode response: %w", err)
			}
		}
		return nil
	}
	return apiError(data, status)
}

// apiError decodes the MLflow error envelope into *APIError, falling back to the
// raw body as the message.
func apiError(data []byte, status int) *APIError {
	ae := &APIError{HTTPStatus: status}
	var env struct {
		ErrorCode string `json:"error_code"`
		Message   string `json:"message"`
	}
	if json.Unmarshal(data, &env) == nil {
		ae.Code, ae.Message = env.ErrorCode, env.Message
	}
	if ae.Message == "" {
		ae.Message = string(data)
	}
	return ae
}
