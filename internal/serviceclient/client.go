package serviceclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cofy-x/kova/internal/serviceapi"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type Config struct {
	BaseURL    string
	Token      string
	Kubeconfig string
	CAFile     string
	Insecure   bool
	HTTPClient *http.Client
}

type Client struct {
	baseURL *url.URL
	http    *http.Client
	token   string
}

type CreateBuildOptions struct {
	ArchivePath    string
	Target         string
	Format         string
	Concurrency    int
	Timeout        int
	Retry          int
	OOMCooldown    time.Duration
	FailFast       bool
	SkipFail       bool
	Verbose        bool
	Variables      []string
	IdempotencyKey string
}

func New(cfg Config) (*Client, error) {
	baseURL, err := url.Parse(strings.TrimSpace(cfg.BaseURL))
	if err != nil || baseURL.Scheme == "" || baseURL.Host == "" {
		return nil, fmt.Errorf("service URL must be an absolute http or https URL")
	}
	if baseURL.Scheme != "http" && baseURL.Scheme != "https" {
		return nil, fmt.Errorf("service URL scheme must be http or https")
	}
	if baseURL.User != nil || baseURL.RawQuery != "" || baseURL.Fragment != "" {
		return nil, fmt.Errorf("service URL must not contain credentials, a query, or a fragment")
	}
	baseURL.Path = strings.TrimRight(baseURL.Path, "/")
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient, err = authenticatedHTTPClient(cfg, baseURL)
		if err != nil {
			return nil, err
		}
	}
	return &Client{baseURL: baseURL, http: httpClient, token: strings.TrimSpace(cfg.Token)}, nil
}

func authenticatedHTTPClient(cfg Config, baseURL *url.URL) (*http.Client, error) {
	var restConfig *rest.Config
	var err error
	if strings.TrimSpace(cfg.Token) == "" && strings.TrimSpace(cfg.Kubeconfig) != "" {
		restConfig, err = clientcmd.BuildConfigFromFlags("", cfg.Kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("load service credentials from kubeconfig: %w", err)
		}
	} else if strings.TrimSpace(cfg.Token) == "" {
		return nil, fmt.Errorf("service authentication requires KOVA_SERVICE_TOKEN or a kubeconfig")
	} else {
		restConfig = &rest.Config{}
	}
	restConfig.Host = baseURL.String()
	restConfig.TLSClientConfig.CAFile = strings.TrimSpace(cfg.CAFile)
	restConfig.TLSClientConfig.CAData = nil
	restConfig.TLSClientConfig.Insecure = cfg.Insecure
	client, err := rest.HTTPClientFor(restConfig)
	if err != nil {
		return nil, fmt.Errorf("configure service credentials: %w", err)
	}
	client.Timeout = 0
	return client, nil
}

func (c *Client) CreateBuild(ctx context.Context, opts CreateBuildOptions) (serviceapi.BuildJob, error) {
	if err := c.CheckCompatible(ctx); err != nil {
		return serviceapi.BuildJob{}, err
	}
	archive, err := os.Open(opts.ArchivePath)
	if err != nil {
		return serviceapi.BuildJob{}, err
	}
	defer archive.Close()

	reader, writer := io.Pipe()
	multipartWriter := multipart.NewWriter(writer)
	writeErr := make(chan error, 1)
	go func() {
		err := writeCreateBuildForm(multipartWriter, archive, opts)
		if closeErr := multipartWriter.Close(); err == nil {
			err = closeErr
		}
		_ = writer.CloseWithError(err)
		writeErr <- err
	}()
	req, err := c.request(ctx, http.MethodPost, "/v1/builds", reader)
	if err != nil {
		return serviceapi.BuildJob{}, err
	}
	req.Header.Set("Content-Type", multipartWriter.FormDataContentType())
	var job serviceapi.BuildJob
	err = c.do(req, &job)
	_ = reader.CloseWithError(err)
	if formErr := <-writeErr; err == nil && formErr != nil {
		err = formErr
	}
	return job, err
}

func (c *Client) Version(ctx context.Context) (serviceapi.VersionInfo, error) {
	var info serviceapi.VersionInfo
	err := c.getJSON(ctx, "/version", &info)
	return info, err
}

func (c *Client) CheckCompatible(ctx context.Context) error {
	info, err := c.Version(ctx)
	if err != nil {
		return fmt.Errorf("query Kova service version: %w", err)
	}
	if info.APIVersion != serviceapi.APIVersion {
		return fmt.Errorf("incompatible Kova service API %q; client requires %q", info.APIVersion, serviceapi.APIVersion)
	}
	return nil
}

func (c *Client) Ready(ctx context.Context) error {
	var status struct {
		Status string `json:"status"`
	}
	if err := c.getJSON(ctx, "/readyz", &status); err != nil {
		return err
	}
	if status.Status != "ready" {
		return fmt.Errorf("service readiness status is %q", status.Status)
	}
	return nil
}

func writeCreateBuildForm(writer *multipart.Writer, archive io.Reader, opts CreateBuildOptions) error {
	file, err := writer.CreateFormFile("file", path.Base(opts.ArchivePath))
	if err != nil {
		return err
	}
	if _, err := io.Copy(file, archive); err != nil {
		return err
	}
	fields := [][2]string{
		{"target", opts.Target}, {"format", opts.Format},
		{"concurrency", strconv.Itoa(opts.Concurrency)}, {"timeout", strconv.Itoa(opts.Timeout)},
		{"retry", strconv.Itoa(opts.Retry)}, {"oom-cooldown", opts.OOMCooldown.String()},
		{"fail-fast", strconv.FormatBool(opts.FailFast)}, {"skip-fail", strconv.FormatBool(opts.SkipFail)},
		{"verbose", strconv.FormatBool(opts.Verbose)}, {"idempotency_key", opts.IdempotencyKey},
	}
	for _, field := range fields {
		if field[1] != "" {
			if err := writer.WriteField(field[0], field[1]); err != nil {
				return err
			}
		}
	}
	for _, value := range opts.Variables {
		if err := writer.WriteField("var", value); err != nil {
			return err
		}
	}
	return nil
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

func (c *Client) getJSON(ctx context.Context, endpoint string, out any) error {
	req, err := c.request(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	return c.do(req, out)
}

func (c *Client) getBytes(ctx context.Context, endpoint string) ([]byte, error) {
	req, err := c.request(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, responseError(resp)
	}
	return io.ReadAll(resp.Body)
}

func (c *Client) request(ctx context.Context, method, endpoint string, body io.Reader) (*http.Request, error) {
	relative, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	u := *c.baseURL
	u.Path = strings.TrimRight(c.baseURL.Path, "/") + relative.Path
	u.RawQuery = relative.RawQuery
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	return req, nil
}

func (c *Client) do(req *http.Request, out any) error {
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return responseError(resp)
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode service response: %w", err)
	}
	return nil
}

func responseError(resp *http.Response) error {
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var payload struct {
		Error string `json:"error"`
	}
	_ = json.Unmarshal(raw, &payload)
	message := strings.TrimSpace(payload.Error)
	if message == "" {
		message = strings.TrimSpace(string(raw))
	}
	if message == "" {
		message = http.StatusText(resp.StatusCode)
	}
	return fmt.Errorf("Kova service returned %s: %s", resp.Status, message)
}
