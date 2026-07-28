package runnerexec

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/daemonclient"
	"github.com/cofy-x/kova/internal/kube"
	"github.com/cofy-x/kova/internal/runner"
)

type Client struct {
	Kube         kube.API
	BuildkitAddr string
}

func (c Client) SubmitBuild(ctx context.Context, build *kovav1.KovaBuild, sourcePath string) error {
	var stdout, stderr bytes.Buffer
	err := c.Kube.Exec(ctx, build.Namespace, build.Status.RunnerPodName, kube.ExecOptions{
		Stdout:  &stdout,
		Stderr:  &stderr,
		Command: daemonclient.TransportCommand("POST", daemonclient.BuildPath, BuildQuery(build, c.BuildkitAddr), sourcePath),
	})
	if err != nil {
		return ExecError("submit build", stderr.Bytes(), err)
	}
	state, err := runner.ParseBuildState(stdout.Bytes())
	if err != nil {
		return fmt.Errorf("parse build response: %w: %s", err, strings.TrimSpace(stdout.String()))
	}
	if state.Status == "failed" || state.Status == "error" {
		return fmt.Errorf("build request failed: %s", strings.TrimSpace(stdout.String()))
	}
	return nil
}

func (c Client) BuildStatus(ctx context.Context, build *kovav1.KovaBuild) (runner.BuildState, error) {
	var stdout, stderr bytes.Buffer
	err := c.Kube.Exec(ctx, build.Namespace, build.Status.RunnerPodName, kube.ExecOptions{
		Stdout:  &stdout,
		Stderr:  &stderr,
		Command: daemonclient.TransportCommand("GET", daemonclient.StatusPath, "", ""),
	})
	if err != nil {
		return runner.BuildState{}, ExecError("build status", stderr.Bytes(), err)
	}
	state, err := runner.ParseBuildState(stdout.Bytes())
	if err != nil {
		return runner.BuildState{}, fmt.Errorf("parse build status: %w", err)
	}
	return state, nil
}

func (c Client) CancelBuild(ctx context.Context, build *kovav1.KovaBuild) error {
	var stderr bytes.Buffer
	err := c.Kube.Exec(ctx, build.Namespace, build.Status.RunnerPodName, kube.ExecOptions{
		Stderr:  &stderr,
		Command: daemonclient.TransportCommand("POST", daemonclient.CancelPath, "", ""),
	})
	if err != nil {
		return ExecError("cancel build", stderr.Bytes(), err)
	}
	return nil
}

func (c Client) Post(ctx context.Context, build *kovav1.KovaBuild, path string, query string) ([]byte, error) {
	var out, stderr bytes.Buffer
	err := c.Kube.Exec(ctx, build.Namespace, build.Status.RunnerPodName, kube.ExecOptions{
		Stdout:  &out,
		Stderr:  &stderr,
		Command: daemonclient.TransportCommand("POST", "/api/v1/"+path, query, ""),
	})
	if err != nil {
		return nil, ExecError(path, stderr.Bytes(), err)
	}
	return out.Bytes(), nil
}

func BuildQuery(build *kovav1.KovaBuild, buildkitAddr string) string {
	values := url.Values{}
	if strings.TrimSpace(buildkitAddr) != "" {
		values.Set("addrs", strings.TrimSpace(buildkitAddr))
	}
	opts := build.Spec.Build
	setString(values, "format", opts.Format)
	setString(values, "target", opts.Target)
	setString(values, "oom-cooldown", opts.OOMCooldown)
	concurrency := opts.Concurrency
	if build.Status.AllocatedConcurrency > 0 {
		concurrency = int(build.Status.AllocatedConcurrency)
	}
	if concurrency > 0 {
		values.Set("concurrency", strconv.Itoa(concurrency))
	}
	if opts.Timeout > 0 {
		values.Set("timeout", strconv.Itoa(opts.Timeout))
	}
	if opts.Retry > 0 {
		values.Set("retry", strconv.Itoa(opts.Retry))
	}
	if opts.FailFast {
		values.Set("fail-fast", "true")
	}
	if opts.SkipFail {
		values.Set("skip-fail", "true")
	}
	if opts.Verbose {
		values.Set("verbose", "true")
	}
	for _, value := range opts.Vars {
		values.Add("var", value)
	}
	return values.Encode()
}

func ExecError(action string, stderr []byte, err error) error {
	if len(stderr) > 0 {
		return fmt.Errorf("%s: %w: %s", action, err, bytes.TrimSpace(stderr))
	}
	return fmt.Errorf("%s: %w", action, err)
}

func setString(values url.Values, key string, value string) {
	if strings.TrimSpace(value) != "" {
		values.Set(key, value)
	}
}
