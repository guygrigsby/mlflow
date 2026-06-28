// Package mlflow is a client for the MLflow REST API (tracking + model registry).
//
// Construct a Client with NewClient (it reads MLFLOW_TRACKING_URI from the
// environment when given ""), then call typed methods. The client is synchronous
// and best-effort: it retries transient failures but never buffers, so callers
// decide what to do on error.
package mlflow
