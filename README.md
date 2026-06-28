# mlflow

[![Go Reference](https://pkg.go.dev/badge/github.com/guygrigsby/mlflow.svg)](https://pkg.go.dev/github.com/guygrigsby/mlflow)
[![Go Report Card](https://goreportcard.com/badge/github.com/guygrigsby/mlflow)](https://goreportcard.com/report/github.com/guygrigsby/mlflow)

Standalone, idiomatic Go client for the [MLflow](https://mlflow.org) REST API v2.0
(tracking + model registry). There is no official Go MLflow client; community efforts
are server-focused and partial. This is a clean, dependency-free tracking + registry
client so Go programs can report experiments, runs, metrics, and models to MLflow
natively.

- **No dependencies.** Standard library only. Requires Go 1.23+.
- **Complete tracking + registry.** Experiments, runs, logging, model registry
  (registered models, versions, stages, aliases), and artifact list/download.
- **Idiomatic.** `context.Context` first, explicit errors, typed structs, functional
  options, an injectable transport so methods are unit-testable without a server.

## Install

```bash
go get github.com/guygrigsby/mlflow
```

## Usage

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/guygrigsby/mlflow"
)

func main() {
	// "" reads MLFLOW_TRACKING_URI from the environment.
	c, err := mlflow.NewClient("")
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()

	exp, err := c.GetOrCreateExperiment(ctx, "lm-100m-en")
	if err != nil {
		log.Fatal(err)
	}

	now := time.Now().UnixMilli()
	run, err := c.CreateRun(ctx, exp, mlflow.WithRunName("r1"), mlflow.WithStartTime(now))
	if err != nil {
		log.Fatal(err)
	}
	id := run.Info.RunID

	err = c.LogBatch(ctx, id,
		[]mlflow.Metric{{Key: "loss", Value: 1.5, Timestamp: now, Step: 0}},
		[]mlflow.Param{{Key: "lr", Value: "4e-4"}},
		[]mlflow.RunTag{{Key: "stage", Value: "train"}})
	if err != nil {
		log.Fatal(err)
	}

	if err := c.UpdateRun(ctx, id, mlflow.StatusFinished, time.Now().UnixMilli()); err != nil {
		log.Fatal(err)
	}
}
```

## Configuration

`NewClient` takes the tracking URI (or `""` to read `MLFLOW_TRACKING_URI`) plus options:

```go
mlflow.NewClient(uri,
	mlflow.WithBearerToken(token),       // Authorization: Bearer
	mlflow.WithBasicAuth(user, pass),    // HTTP basic auth
	mlflow.WithHTTPClient(httpClient),   // default: 30s timeout
	mlflow.WithMaxRetries(3),            // 5xx + transport errors, exponential backoff
)
```

The client is synchronous and best-effort: it retries transient failures (5xx and
transport errors) with exponential backoff, never retries 4xx, and never buffers.
Callers decide what to do on error.

## Errors

Non-2xx responses map to `*mlflow.APIError` (`Code`, `Message`, `HTTPStatus`). Use
`mlflow.IsNotFound(err)` to test for `RESOURCE_DOES_NOT_EXIST`:

```go
exp, err := c.GetExperimentByName(ctx, "missing")
if mlflow.IsNotFound(err) {
	// create it, etc.
}
```

## Scope

Implements 100% of the MLflow REST API v2.0 for tracking and the model registry, plus
artifact `list` and proxy `get-artifact` (download). Out of scope: artifact-store
**upload** (the per-backend S3/GCS/Azure plumbing), the auth/permissions admin plugin,
and MLflow 3.x GenAI surfaces (traces, logged-models, datasets).

## License

[MIT](LICENSE)
