// Package v1 defines the stable Kova Service HTTP API v1 contract.
package v1

import "time"

const APIVersion = "v1"

type Platform string

const (
	PlatformLinuxAMD64 Platform = "linux/amd64"
	PlatformLinuxARM64 Platform = "linux/arm64"
)

const (
	MaxLogicalTargets       = 100
	MaxConcreteOutputs      = MaxLogicalTargets * 2
	MaxTargetLength         = 512
	MaxBuildConcurrency     = MaxLogicalTargets
	MaxSourceURILength      = 2048
	MaxBuildVariables       = 100
	MaxBuildVariableLength  = 2048
	MaxIdempotencyKeyLength = 256
	MaxListBuildsPageSize   = 500
	MaxLogTailLines         = 10_000
)

type CreateBuildRequest struct {
	SourceURI      string       `json:"source_uri"`
	SourceDigest   string       `json:"source_digest"`
	Targets        []TargetSpec `json:"targets"`
	Format         string       `json:"format,omitempty"`
	Concurrency    int          `json:"concurrency,omitempty"`
	Timeout        int          `json:"timeout,omitempty"`
	OOMCooldown    string       `json:"oom_cooldown,omitempty"`
	FailFast       bool         `json:"fail_fast,omitempty"`
	Verbose        bool         `json:"verbose,omitempty"`
	Variables      []string     `json:"variables,omitempty"`
	IdempotencyKey string       `json:"idempotency_key,omitempty"`
}

type TargetSpec struct {
	Target   string   `json:"target"`
	Platform Platform `json:"platform"`
}

type VersionInfo struct {
	APIVersion string `json:"api_version"`
	Version    string `json:"version"`
	Commit     string `json:"commit"`
	BuildDate  string `json:"build_date"`
}

type ReadyStatus struct {
	Status string `json:"status"`
}

type JobStatus string

const (
	JobStatusQueued    JobStatus = "queued"
	JobStatusStarting  JobStatus = "starting"
	JobStatusRunning   JobStatus = "running"
	JobStatusSucceeded JobStatus = "succeeded"
	JobStatusFailed    JobStatus = "failed"
	JobStatusCancelled JobStatus = "cancelled"
)

type BuildFailureCode string

const (
	BuildFailureInvalidSource             BuildFailureCode = "invalid_source"
	BuildFailureInvalidTargets            BuildFailureCode = "invalid_targets"
	BuildFailureRunnerUnavailable         BuildFailureCode = "runner_unavailable"
	BuildFailureWorkerPlatformUnavailable BuildFailureCode = "worker_platform_unavailable"
	BuildFailureSubmissionFailed          BuildFailureCode = "build_submission_failed"
	BuildFailureVerificationFailed        BuildFailureCode = "result_verification_failed"
	BuildFailureExecutionFailed           BuildFailureCode = "build_failed"
	BuildFailureCancelled                 BuildFailureCode = "cancelled"
)

type BuildJob struct {
	ID                    string           `json:"id"`
	Status                JobStatus        `json:"status"`
	Error                 string           `json:"error,omitempty"`
	FailureCode           BuildFailureCode `json:"failure_code,omitempty"`
	CreatedAt             time.Time        `json:"created_at"`
	StartedAt             *time.Time       `json:"started_at,omitempty"`
	FinishedAt            *time.Time       `json:"finished_at,omitempty"`
	ExpiresAt             *time.Time       `json:"expires_at,omitempty"`
	SourceDigest          string           `json:"source_digest,omitempty"`
	SourceURI             string           `json:"source_uri,omitempty"`
	IdempotencyKey        string           `json:"idempotency_key,omitempty"`
	Requester             string           `json:"requester"`
	CancellationRequested bool             `json:"cancellation_requested,omitempty"`
	RequestedConcurrency  int              `json:"requested_concurrency,omitempty"`
	AllocatedConcurrency  int32            `json:"allocated_concurrency,omitempty"`
}

type BuildOutput struct {
	Format         string   `json:"format"`
	Image          string   `json:"image"`
	ManifestDigest string   `json:"manifest_digest"`
	ImmutableRef   string   `json:"immutable_ref"`
	Platform       Platform `json:"platform"`
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

type ErrorCode string

const (
	ErrorCodeInvalidRequest        ErrorCode = "invalid_request"
	ErrorCodeUnauthenticated       ErrorCode = "unauthenticated"
	ErrorCodeForbidden             ErrorCode = "forbidden"
	ErrorCodeNotFound              ErrorCode = "not_found"
	ErrorCodeConflict              ErrorCode = "conflict"
	ErrorCodeQueueCapacityExceeded ErrorCode = "queue_capacity_exceeded"
	ErrorCodeLogsUnavailable       ErrorCode = "logs_unavailable"
	ErrorCodeInternal              ErrorCode = "internal"
)

type ErrorResponse struct {
	Code      ErrorCode `json:"code"`
	Message   string    `json:"message"`
	Retryable bool      `json:"retryable"`
}
