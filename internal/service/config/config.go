package config

import "time"

type Config struct {
	Listen                string
	Namespace             string
	RunnerImage           string
	RunnerImagePullPolicy string
	RunnerImagePullSecret string
	RunnerNodeSelector    map[string]string
	BuildkitAddr          string
	SourcePVCClaim        string
	SourceRoot            string
	SourceMountPath       string
	JobTTL                time.Duration
	AuthToken             string
	WaitTimeout           time.Duration
	PollInterval          time.Duration
}
