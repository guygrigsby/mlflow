package mlflow

// RegisteredModel describes a model registered in the MLflow Model Registry.
type RegisteredModel struct {
	Name                 string                 `json:"name"`
	CreationTimestamp    int64                  `json:"creation_timestamp,omitempty"`
	LastUpdatedTimestamp int64                  `json:"last_updated_timestamp,omitempty"`
	Description          string                 `json:"description,omitempty"`
	LatestVersions       []ModelVersion         `json:"latest_versions,omitempty"`
	Tags                 []RegisteredModelTag   `json:"tags,omitempty"`
	Aliases              []RegisteredModelAlias `json:"aliases,omitempty"`
}

// ModelVersion describes a specific version of a registered model.
type ModelVersion struct {
	Name                 string            `json:"name"`
	Version              string            `json:"version"`
	CreationTimestamp    int64             `json:"creation_timestamp,omitempty"`
	LastUpdatedTimestamp int64             `json:"last_updated_timestamp,omitempty"`
	CurrentStage         string            `json:"current_stage,omitempty"`
	Description          string            `json:"description,omitempty"`
	Source               string            `json:"source,omitempty"`
	RunID                string            `json:"run_id,omitempty"`
	Status               string            `json:"status,omitempty"`
	Tags                 []ModelVersionTag `json:"tags,omitempty"`
	Aliases              []string          `json:"aliases,omitempty"`
}

// RegisteredModelTag is a key-value tag on a registered model.
type RegisteredModelTag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// ModelVersionTag is a key-value tag on a model version.
type ModelVersionTag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// RegisteredModelAlias maps an alias name to a model version number.
type RegisteredModelAlias struct {
	Alias   string `json:"alias"`
	Version string `json:"version"`
}
