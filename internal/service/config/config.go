package config

import "time"

type Config struct {
	Listen                string
	Namespace             string
	RunnerImage           string
	RunnerImagePullPolicy string
	RunnerImagePullSecret string
	RunnerNodeSelector    map[string]string
	RunnerEnv             map[string]string
	RegistryPlainHTTP     []string
	BuildkitAddr          string
	SourcePVCClaim        string
	ArtifactDriver        string
	ArtifactRoot          string
	ArtifactSecret        string
	S3Endpoint            string
	S3Bucket              string
	S3Region              string
	S3AccessKey           string
	S3SecretKey           string
	S3SessionToken        string
	S3Secure              bool
	JobTTL                time.Duration
	MaxUploadBytes        int64
	AuthToken             string
	AuthMode              string
	AuthStaticPrincipal   string
	WaitTimeout           time.Duration
	PollInterval          time.Duration
	MaxActiveJobs         int
	WorkerSlots           int
}
