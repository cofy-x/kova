package serviceapi

import "time"

const APIVersion = "v1"

type CreateBuildRequest struct {
	SourceURI      string   `json:"source_uri"`
	SourceDigest   string   `json:"source_digest"`
	Targets        []string `json:"targets"`
	Format         string   `json:"format,omitempty"`
	Concurrency    int      `json:"concurrency,omitempty"`
	Timeout        int      `json:"timeout,omitempty"`
	OOMCooldown    string   `json:"oom_cooldown,omitempty"`
	FailFast       bool     `json:"fail_fast,omitempty"`
	Verbose        bool     `json:"verbose,omitempty"`
	Variables      []string `json:"variables,omitempty"`
	IdempotencyKey string   `json:"idempotency_key,omitempty"`
}

type VersionInfo struct {
	APIVersion string `json:"api_version"`
	Version    string `json:"version"`
	Commit     string `json:"commit"`
	BuildDate  string `json:"build_date"`
}

const (
	JobStatusQueued    = "queued"
	JobStatusStarting  = "starting"
	JobStatusRunning   = "running"
	JobStatusSucceeded = "succeeded"
	JobStatusFailed    = "failed"
	JobStatusCancelled = "cancelled"
)

type BuildJob struct {
	ID                    string     `json:"id"`
	Status                string     `json:"status"`
	PodName               string     `json:"pod_name"`
	Namespace             string     `json:"namespace"`
	Error                 string     `json:"error,omitempty"`
	Reason                string     `json:"reason,omitempty"`
	CreatedAt             time.Time  `json:"created_at"`
	StartedAt             *time.Time `json:"started_at,omitempty"`
	FinishedAt            *time.Time `json:"finished_at,omitempty"`
	ExpiresAt             *time.Time `json:"expires_at,omitempty"`
	BuildkitAddr          string     `json:"buildkit_addr,omitempty"`
	SourceDigest          string     `json:"source_digest,omitempty"`
	SourceURI             string     `json:"source_uri,omitempty"`
	IdempotencyKey        string     `json:"idempotency_key,omitempty"`
	Requester             string     `json:"requester"`
	CancellationRequested bool       `json:"cancellation_requested,omitempty"`
	RequestedConcurrency  int        `json:"requested_concurrency,omitempty"`
	AllocatedConcurrency  int32      `json:"allocated_concurrency,omitempty"`
}

type BuildOutput struct {
	Format         string `json:"format"`
	Image          string `json:"image"`
	ManifestDigest string `json:"manifest_digest"`
}

type BuildResults struct {
	ID             string        `json:"id"`
	SourceURI      string        `json:"source_uri"`
	SourceDigest   string        `json:"source_digest"`
	IdempotencyKey string        `json:"idempotency_key,omitempty"`
	Outputs        []BuildOutput `json:"outputs"`
}

type JobList struct {
	Jobs     []BuildJob `json:"jobs"`
	Continue string     `json:"continue,omitempty"`
}
