package config

import "time"

type Config struct {
	Listen                    string
	Namespace                 string
	RunnerImage               string
	RunnerImagePullPolicy     string
	RunnerImagePullSecret     string
	RunnerNodeSelector        map[string]string
	RunnerEnv                 map[string]string
	RegistryPlainHTTP         []string
	BuildkitAddr              string
	JobTTL                    time.Duration
	AuthToken                 string
	AuthMode                  string
	AuthStaticPrincipal       string
	WaitTimeout               time.Duration
	PollInterval              time.Duration
	MaxActiveJobs             int
	MaxActiveJobsPerRequester int
	MaxQueuedJobsPerRequester int
	WorkerSlots               int
	ControllerConcurrency     int
}
