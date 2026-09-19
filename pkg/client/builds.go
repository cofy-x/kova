package client

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

	apiv1 "github.com/cofy-x/kova/pkg/api/v1"
)

func (c *Client) CreateBuild(ctx context.Context, request apiv1.CreateBuildRequest) (apiv1.BuildJob, error) {
	body, err := json.Marshal(request)
	if err != nil {
		return apiv1.BuildJob{}, err
	}
	req, err := c.request(ctx, http.MethodPost, "/v1/builds", bytes.NewReader(body))
	if err != nil {
		return apiv1.BuildJob{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	var job apiv1.BuildJob
	err = c.do(req, &job)
	return job, err
}

func (c *Client) ListBuilds(ctx context.Context) (apiv1.JobList, error) {
	return c.ListBuildsPage(ctx, 100, "")
}

func (c *Client) ListBuildsPage(ctx context.Context, limit int, continueToken string) (apiv1.JobList, error) {
	var jobs apiv1.JobList
	if limit < 1 || limit > apiv1.MaxListBuildsPageSize {
		return jobs, fmt.Errorf("limit must be between 1 and %d", apiv1.MaxListBuildsPageSize)
	}
	values := url.Values{}
	values.Set("limit", strconv.Itoa(limit))
	if continueToken != "" {
		values.Set("continue", continueToken)
	}
	err := c.getJSON(ctx, "/v1/builds?"+values.Encode(), &jobs)
	return jobs, err
}

func (c *Client) GetBuild(ctx context.Context, id string) (apiv1.BuildJob, error) {
	endpoint, err := buildEndpoint(id, "")
	if err != nil {
		return apiv1.BuildJob{}, err
	}
	var job apiv1.BuildJob
	err = c.getJSON(ctx, endpoint, &job)
	return job, err
}

func (c *Client) GetResults(ctx context.Context, id string) (apiv1.BuildResults, error) {
	endpoint, err := buildEndpoint(id, "results")
	if err != nil {
		return apiv1.BuildResults{}, err
	}
	var results apiv1.BuildResults
	err = c.getJSON(ctx, endpoint, &results)
	return results, err
}

func (c *Client) GetLogs(ctx context.Context, id string, tailLines int64) ([]byte, error) {
	if tailLines < 0 || tailLines > apiv1.MaxLogTailLines {
		return nil, fmt.Errorf("tail lines must be between 0 and %d", apiv1.MaxLogTailLines)
	}
	endpoint, err := buildEndpoint(id, "logs")
	if err != nil {
		return nil, err
	}
	return c.getBytes(ctx, fmt.Sprintf("%s?tail_lines=%d", endpoint, tailLines))
}

func (c *Client) CancelBuild(ctx context.Context, id string) (apiv1.BuildJob, error) {
	endpoint, err := buildEndpoint(id, "cancel")
	if err != nil {
		return apiv1.BuildJob{}, err
	}
	req, err := c.request(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return apiv1.BuildJob{}, err
	}
	var job apiv1.BuildJob
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

func (c *Client) WaitBuild(ctx context.Context, id string, interval time.Duration) (apiv1.BuildJob, error) {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	for {
		job, err := c.GetBuild(ctx, id)
		if err != nil {
			return apiv1.BuildJob{}, err
		}
		switch job.Status {
		case apiv1.JobStatusSucceeded:
			return job, nil
		case apiv1.JobStatusFailed, apiv1.JobStatusCancelled:
			return job, fmt.Errorf("job %s ended with status %s: %s", job.ID, job.Status, job.Error)
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return apiv1.BuildJob{}, ctx.Err()
		case <-timer.C:
		}
	}
}
