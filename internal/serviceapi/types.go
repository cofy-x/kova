package serviceapi

import "time"

const (
	JobStatusQueued    = "queued"
	JobStatusStarting  = "starting"
	JobStatusRunning   = "running"
	JobStatusSucceeded = "succeeded"
	JobStatusFailed    = "failed"
	JobStatusCancelled = "cancelled"
)

type BuildJob struct {
	ID             string     `json:"id"`
	Status         string     `json:"status"`
	PodName        string     `json:"pod_name"`
	Namespace      string     `json:"namespace"`
	Error          string     `json:"error,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	StartedAt      *time.Time `json:"started_at,omitempty"`
	FinishedAt     *time.Time `json:"finished_at,omitempty"`
	ExpiresAt      *time.Time `json:"expires_at,omitempty"`
	BuildkitAddr   string     `json:"buildkit_addr,omitempty"`
	SourceDigest   string     `json:"source_digest,omitempty"`
	IdempotencyKey string     `json:"idempotency_key,omitempty"`
	Requester      string     `json:"requester"`
}

type BuildResult struct {
	Format         string `json:"format"`
	Status         string `json:"status"`
	Repository     string `json:"repository"`
	ManifestDigest string `json:"manifest_digest,omitempty"`
	MediaType      string `json:"media_type,omitempty"`
	Size           int64  `json:"size,omitempty"`
	Error          string `json:"error,omitempty"`
}

type BuildResults struct {
	ID                string        `json:"id"`
	SourceDigest      string        `json:"source_digest,omitempty"`
	IdempotencyKey    string        `json:"idempotency_key,omitempty"`
	ResultArtifactURI string        `json:"result_artifact_uri,omitempty"`
	Results           []BuildResult `json:"results"`
}

type JobList struct {
	Jobs []BuildJob `json:"jobs"`
}
