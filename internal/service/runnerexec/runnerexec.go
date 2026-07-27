package runnerexec

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/kube"
	"github.com/cofy-x/kova/internal/runner"
)

const DaemonSocket = "/tmp/kova.sock"

type Client struct {
	Kube         kube.API
	BuildkitAddr string
}

func (c Client) SubmitBuild(ctx context.Context, build *kovav1.KovaBuild, sourceMountPath string) error {
	zipPath := filepath.Join(sourceMountPath, build.Spec.Source.PVC.Path)
	var stdout, stderr bytes.Buffer
	err := c.Kube.Exec(ctx, build.Namespace, build.Status.RunnerPodName, kube.ExecOptions{
		Stdout:  &stdout,
		Stderr:  &stderr,
		Command: []string{"curl", "-sS", "-X", "POST", "-T", zipPath, "--unix-socket", DaemonSocket, "http://localhost/api/v1/build?" + BuildQuery(build, c.BuildkitAddr)},
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
		Command: []string{"curl", "-sS", "--unix-socket", DaemonSocket, "http://localhost/api/v1/build/status"},
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
		Command: []string{"curl", "-sS", "-X", "POST", "--unix-socket", DaemonSocket, "http://localhost/api/v1/build/cancel"},
	})
	if err != nil {
		return ExecError("cancel build", stderr.Bytes(), err)
	}
	return nil
}

func (c Client) Post(ctx context.Context, build *kovav1.KovaBuild, path string, query string) ([]byte, error) {
	script := `set -eu
socket=$1
url=$2
body=$(mktemp)
cleanup() { rm -f "$body"; }
trap cleanup EXIT
code=$(curl -sS -o "$body" -w "%{http_code}" -X POST --unix-socket "$socket" "$url")
if [ "$code" -lt 200 ] || [ "$code" -ge 300 ]; then
  cat "$body" >&2
  exit 1
fi
cat "$body"`
	var out, stderr bytes.Buffer
	err := c.Kube.Exec(ctx, build.Namespace, build.Status.RunnerPodName, kube.ExecOptions{
		Stdout: &out,
		Stderr: &stderr,
		Command: []string{
			"sh", "-lc", script, "sh",
			DaemonSocket, "http://localhost/api/v1/" + path + "?" + query,
		},
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
	if opts.Concurrency > 0 {
		values.Set("concurrency", strconv.Itoa(opts.Concurrency))
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
