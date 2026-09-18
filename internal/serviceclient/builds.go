package serviceclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cofy-x/kova/internal/serviceapi"
)

type CreateBuildOptions struct {
	SourceURI      string
	SourceDigest   string
	Targets        []string
	Format         string
	Concurrency    int
	Timeout        int
	OOMCooldown    time.Duration
	FailFast       bool
	Verbose        bool
	Variables      []string
	IdempotencyKey string
}

func (c *Client) CreateBuild(ctx context.Context, opts CreateBuildOptions) (serviceapi.BuildJob, error) {
	if err := c.CheckCompatible(ctx); err != nil {
		return serviceapi.BuildJob{}, err
	}
	body, err := json.Marshal(serviceapi.CreateBuildRequest{
		SourceURI: opts.SourceURI, SourceDigest: opts.SourceDigest, Targets: opts.Targets,
		Format: opts.Format, Concurrency: opts.Concurrency, Timeout: opts.Timeout,
		OOMCooldown: opts.OOMCooldown.String(), FailFast: opts.FailFast, Verbose: opts.Verbose,
		Variables: opts.Variables, IdempotencyKey: opts.IdempotencyKey,
	})
	if err != nil {
		return serviceapi.BuildJob{}, err
	}
	req, err := c.request(ctx, http.MethodPost, "/v1/builds", bytes.NewReader(body))
	if err != nil {
		return serviceapi.BuildJob{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	var job serviceapi.BuildJob
	err = c.do(req, &job)
	return job, err
}

func (c *Client) List(ctx context.Context) (serviceapi.JobList, error) {
	return c.ListPage(ctx, 100, "")
}

func (c *Client) ListPage(ctx context.Context, limit int, continueToken string) (serviceapi.JobList, error) {
	var jobs serviceapi.JobList
	values := url.Values{}
	values.Set("limit", strconv.Itoa(limit))
	if continueToken != "" {
		values.Set("continue", continueToken)
	}
	err := c.getJSON(ctx, "/v1/builds?"+values.Encode(), &jobs)
	return jobs, err
}

func (c *Client) Get(ctx context.Context, id string) (serviceapi.BuildJob, error) {
	endpoint, err := buildEndpoint(id, "")
	if err != nil {
		return serviceapi.BuildJob{}, err
	}
	var job serviceapi.BuildJob
	err = c.getJSON(ctx, endpoint, &job)
	return job, err
}

func (c *Client) Results(ctx context.Context, id string) (serviceapi.BuildResults, error) {
	endpoint, err := buildEndpoint(id, "results")
	if err != nil {
		return serviceapi.BuildResults{}, err
	}
	var results serviceapi.BuildResults
	err = c.getJSON(ctx, endpoint, &results)
	return results, err
}

func (c *Client) Logs(ctx context.Context, id string, tail int64) ([]byte, error) {
	endpoint, err := buildEndpoint(id, "logs")
	if err != nil {
		return nil, err
	}
	return c.getBytes(ctx, fmt.Sprintf("%s?tail_lines=%d", endpoint, tail))
}

func (c *Client) Cancel(ctx context.Context, id string) (serviceapi.BuildJob, error) {
	endpoint, err := buildEndpoint(id, "cancel")
	if err != nil {
		return serviceapi.BuildJob{}, err
	}
	req, err := c.request(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return serviceapi.BuildJob{}, err
	}
	var job serviceapi.BuildJob
	err = c.do(req, &job)
	return job, err
}

var jobIDPattern = regexp.MustCompile(`^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$`)

func buildEndpoint(id, action string) (string, error) {
	id = strings.TrimSpace(id)
	if len(id) > 253 || !jobIDPattern.MatchString(id) {
		return "", fmt.Errorf("invalid job ID %q", id)
	}
	endpoint := "/v1/builds/" + id
	if action != "" {
		endpoint += "/" + action
	}
	return endpoint, nil
}

func (c *Client) Wait(ctx context.Context, id string, interval time.Duration) (serviceapi.BuildJob, error) {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	for {
		job, err := c.Get(ctx, id)
		if err != nil {
			return serviceapi.BuildJob{}, err
		}
		switch job.Status {
		case serviceapi.JobStatusSucceeded:
			return job, nil
		case serviceapi.JobStatusFailed, serviceapi.JobStatusCancelled:
			return job, fmt.Errorf("job %s ended with status %s: %s", job.ID, job.Status, job.Error)
		}
		select {
		case <-ctx.Done():
			return serviceapi.BuildJob{}, ctx.Err()
		case <-time.After(interval):
		}
	}
}
