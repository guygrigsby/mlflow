package mlflow

type RunStatus string

const (
	StatusRunning   RunStatus = "RUNNING"
	StatusScheduled RunStatus = "SCHEDULED"
	StatusFinished  RunStatus = "FINISHED"
	StatusFailed    RunStatus = "FAILED"
	StatusKilled    RunStatus = "KILLED"
)

type Experiment struct {
	ExperimentID     string          `json:"experiment_id"`
	Name             string          `json:"name"`
	ArtifactLocation string          `json:"artifact_location,omitempty"`
	LifecycleStage   string          `json:"lifecycle_stage,omitempty"`
	Tags             []ExperimentTag `json:"tags,omitempty"`
}

type ExperimentTag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Run struct {
	Info RunInfo `json:"info"`
	Data RunData `json:"data"`
}

type RunInfo struct {
	RunID          string    `json:"run_id"`
	ExperimentID   string    `json:"experiment_id"`
	RunName        string    `json:"run_name,omitempty"`
	Status         RunStatus `json:"status,omitempty"`
	StartTime      int64     `json:"start_time,omitempty"` // unix ms
	EndTime        int64     `json:"end_time,omitempty"`   // unix ms
	ArtifactURI    string    `json:"artifact_uri,omitempty"`
	LifecycleStage string    `json:"lifecycle_stage,omitempty"`
}

type RunData struct {
	Metrics []Metric `json:"metrics,omitempty"`
	Params  []Param  `json:"params,omitempty"`
	Tags    []RunTag `json:"tags,omitempty"`
}

type Metric struct {
	Key       string  `json:"key"`
	Value     float64 `json:"value"`
	Timestamp int64   `json:"timestamp"` // unix ms
	Step      int64   `json:"step"`
}

type Param struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type RunTag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// SearchExperimentsRequest is the request body for SearchExperiments.
type SearchExperimentsRequest struct {
	Filter     string   `json:"filter,omitempty"`
	MaxResults int64    `json:"max_results,omitempty"`
	OrderBy    []string `json:"order_by,omitempty"`
	PageToken  string   `json:"page_token,omitempty"`
	ViewType   string   `json:"view_type,omitempty"`
}

// SearchRunsRequest is the request body for SearchRuns.
type SearchRunsRequest struct {
	ExperimentIDs []string `json:"experiment_ids"`
	Filter        string   `json:"filter,omitempty"`
	MaxResults    int32    `json:"max_results,omitempty"`
	OrderBy       []string `json:"order_by,omitempty"`
	PageToken     string   `json:"page_token,omitempty"`
	RunViewType   string   `json:"run_view_type,omitempty"`
}

// DatasetInput is a dataset associated with a run via LogInputs.
type DatasetInput struct {
	Tags    []RunTag `json:"tags,omitempty"`
	Dataset Dataset  `json:"dataset"`
}

// Dataset describes a logged dataset.
type Dataset struct {
	Name       string `json:"name"`
	Digest     string `json:"digest,omitempty"`
	SourceType string `json:"source_type,omitempty"`
	Source     string `json:"source,omitempty"`
	Schema     string `json:"schema,omitempty"`
	Profile    string `json:"profile,omitempty"`
}

// FileInfo describes a single artifact entry returned by ListArtifacts.
type FileInfo struct {
	Path     string `json:"path"`
	IsDir    bool   `json:"is_dir"`
	FileSize int64  `json:"file_size,omitempty"`
}
