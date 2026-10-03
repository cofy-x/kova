package config

import (
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

type Config struct {
	Listen                     string
	Namespace                  string
	RunnerImage                string
	RunnerImageDigest          string
	WorkerPoolID               string
	RunnerImagePullPolicy      string
	RunnerImagePullSecret      string
	RunnerNodeSelector         map[string]string
	RunnerEnv                  map[string]string
	RunnerResources            corev1.ResourceRequirements
	SourceFetchResources       corev1.ResourceRequirements
	SourceVolumeSizeLimit      *resource.Quantity
	RegistryPlainHTTP          []string
	BuildkitPlatformAddrs      map[string]string
	JobTTL                     time.Duration
	AuthToken                  string
	AuthMode                   string
	AuthStaticPrincipal        string
	WaitTimeout                time.Duration
	PollInterval               time.Duration
	PollRetryWindow            time.Duration
	MaxBuildDuration           time.Duration
	VerificationAttemptTimeout time.Duration
	VerificationWindow         time.Duration
	MaxActiveJobs              int
	MaxActiveJobsPerRequester  int
	MaxQueuedJobs              int
	MaxQueuedJobsPerRequester  int
	WorkerSlots                int
	ControllerConcurrency      int
}
