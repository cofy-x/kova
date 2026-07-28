package httpapi

import "github.com/cofy-x/kova/internal/serviceapi"

const (
	JobStatusQueued    = serviceapi.JobStatusQueued
	JobStatusStarting  = serviceapi.JobStatusStarting
	JobStatusRunning   = serviceapi.JobStatusRunning
	JobStatusSucceeded = serviceapi.JobStatusSucceeded
	JobStatusFailed    = serviceapi.JobStatusFailed
	JobStatusCancelled = serviceapi.JobStatusCancelled
)

type BuildJob = serviceapi.BuildJob
type BuildResult = serviceapi.BuildResult
type buildResultsResponse = serviceapi.BuildResults
type jobListResponse = serviceapi.JobList
