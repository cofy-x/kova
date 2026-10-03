package daemonclient

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
)

const (
	DefaultSocket                    = "/tmp/kova.sock"
	HealthPath                       = "/api/v1/health"
	BuildPath                        = "/api/v1/build"
	StatusPath                       = "/api/v1/build/status"
	CancelPath                       = "/api/v1/build/cancel"
	ExportPath                       = "/api/v1/export"
	PreheatPath                      = "/api/v1/preheat"
	IdempotentBuildRequestCapability = "idempotent-build-request-v1"
	// RunnerPodUIDEnv is injected from the Pod's immutable spec through the
	// Downward API only for Genesis runner Pods. Its presence makes an exact
	// expected Pod UID mandatory before any in-container runner operation.
	RunnerPodUIDEnv = "KOVA_RUNNER_POD_UID"
)

type Client struct {
	httpClient *http.Client
}

func New(socketPath string) *Client {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var dialer net.Dialer
			return dialer.DialContext(ctx, "unix", socketPath)
		},
	}
	return &Client{httpClient: &http.Client{Transport: transport}}
}

func (c *Client) Do(ctx context.Context, method, path string, query url.Values, body io.Reader, output io.Writer) error {
	if !strings.HasPrefix(path, "/api/v1/") {
		return fmt.Errorf("daemon API path must start with /api/v1/")
	}
	requestURL := "http://kova" + path
	if len(query) > 0 {
		requestURL += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, requestURL, body)
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if _, err := io.Copy(output, resp.Body); err != nil {
		return err
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("daemon request returned HTTP %d", resp.StatusCode)
	}
	return nil
}

func TransportCommand(method, path, query string, inputPath string) []string {
	command := []string{"kovad", "transport", "--method", method, "--path", path}
	if query != "" {
		command = append(command, "--query", query)
	}
	if inputPath != "" {
		command = append(command, "--input", inputPath)
	}
	return command
}

func TransportCommandForPodUID(method, path, query, inputPath, expectedPodUID string) []string {
	command := TransportCommand(method, path, query, inputPath)
	if expectedPodUID != "" {
		command = append(command, "--expected-pod-uid", expectedPodUID)
	}
	return command
}

// ValidateExpectedPodUID runs inside the target container before touching an
// input file or daemon socket. An absent env preserves the standalone legacy
// runner, but never accepts a caller claiming a Pod UID. A Genesis container
// with the Downward API env rejects absent or mismatched expectations.
func ValidateExpectedPodUID(expectedPodUID string) error {
	actualPodUID, present := os.LookupEnv(RunnerPodUIDEnv)
	if !present && expectedPodUID == "" {
		return nil
	}
	if !present || actualPodUID == "" || expectedPodUID == "" || expectedPodUID != actualPodUID {
		return fmt.Errorf("runner Pod UID fence rejected the command")
	}
	return nil
}
