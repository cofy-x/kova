package daemonclient

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
)

const (
	DefaultSocket = "/tmp/kova.sock"
	HealthPath    = "/api/v1/health"
	BuildPath     = "/api/v1/build"
	StatusPath    = "/api/v1/build/status"
	CancelPath    = "/api/v1/build/cancel"
	ExportPath    = "/api/v1/export"
	PreheatPath   = "/api/v1/preheat"
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
