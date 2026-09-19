package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	apiv1 "github.com/cofy-x/kova/pkg/api/v1"
)

func TestClientUsesBearerTokenAndPublicTypes(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/version" && r.URL.Path != "/readyz" && r.Header.Get("Authorization") != "Bearer secret" {
			writeTestError(w, http.StatusUnauthorized, apiv1.ErrorCodeUnauthenticated, false)
			return
		}
		switch r.URL.Path {
		case "/version":
			_ = json.NewEncoder(w).Encode(apiv1.VersionInfo{APIVersion: apiv1.APIVersion})
		case "/readyz":
			_ = json.NewEncoder(w).Encode(apiv1.ReadyStatus{Status: "ready"})
		case "/v1/builds":
			_ = json.NewEncoder(w).Encode(apiv1.JobList{Jobs: []apiv1.BuildJob{{ID: "job-1", Status: apiv1.JobStatusRunning}}})
		case "/v1/builds/job-1":
			if r.Method == http.MethodPost {
				_ = json.NewEncoder(w).Encode(apiv1.BuildJob{ID: "job-1", Status: apiv1.JobStatusCancelled})
				return
			}
			_ = json.NewEncoder(w).Encode(apiv1.BuildJob{ID: "job-1", Status: apiv1.JobStatusSucceeded})
		case "/v1/builds/job-1/results":
			_ = json.NewEncoder(w).Encode(apiv1.BuildResults{ID: "job-1", Outputs: []apiv1.BuildOutput{{ManifestDigest: digest, ImmutableRef: "registry.example.com/team/image@" + digest}}})
		case "/v1/builds/job-1/logs":
			_, _ = w.Write([]byte("hello\n"))
		case "/v1/builds/job-1/cancel":
			_ = json.NewEncoder(w).Encode(apiv1.BuildJob{ID: "job-1", Status: apiv1.JobStatusRunning, CancellationRequested: true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := New(Config{BaseURL: server.URL, Token: "secret", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := client.CheckCompatible(ctx); err != nil {
		t.Fatal(err)
	}
	if err := client.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	list, err := client.ListBuilds(ctx)
	if err != nil || len(list.Jobs) != 1 {
		t.Fatalf("list=%#v err=%v", list, err)
	}
	job, err := client.GetBuild(ctx, "job-1")
	if err != nil || job.Status != apiv1.JobStatusSucceeded {
		t.Fatalf("job=%#v err=%v", job, err)
	}
	results, err := client.GetResults(ctx, "job-1")
	if err != nil || results.Outputs[0].ImmutableRef == "" {
		t.Fatalf("results=%#v err=%v", results, err)
	}
	logs, err := client.GetLogs(ctx, "job-1", 100)
	if err != nil || string(logs) != "hello\n" {
		t.Fatalf("logs=%q err=%v", logs, err)
	}
	if _, err := client.CancelBuild(ctx, "job-1"); err != nil {
		t.Fatal(err)
	}
}

func TestCreateBuildDoesNotRetryAndReturnsTypedAPIError(t *testing.T) {
	var posts atomic.Int32
	var versionRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/version" {
			versionRequests.Add(1)
			_ = json.NewEncoder(w).Encode(apiv1.VersionInfo{APIVersion: apiv1.APIVersion})
			return
		}
		posts.Add(1)
		w.Header().Set("Retry-After", "7")
		writeTestError(w, http.StatusTooManyRequests, apiv1.ErrorCodeQueueCapacityExceeded, true)
	}))
	defer server.Close()
	client, err := New(Config{BaseURL: server.URL, Token: "do-not-leak", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.CreateBuild(context.Background(), apiv1.CreateBuildRequest{IdempotencyKey: "safe-retry-key"})
	if err == nil {
		t.Fatal("expected API error")
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("error is not APIError: %T %v", err, err)
	}
	if apiErr.StatusCode != http.StatusTooManyRequests || apiErr.Code != apiv1.ErrorCodeQueueCapacityExceeded || !apiErr.Retryable || apiErr.RetryAfter != 7*time.Second {
		t.Fatalf("APIError=%#v", apiErr)
	}
	if posts.Load() != 1 {
		t.Fatalf("CreateBuild attempted %d POSTs", posts.Load())
	}
	if versionRequests.Load() != 0 {
		t.Fatalf("CreateBuild made %d implicit compatibility requests", versionRequests.Load())
	}
	if strings.Contains(err.Error(), "do-not-leak") {
		t.Fatalf("bearer token leaked in error: %v", err)
	}
}

func TestClientRejectsOutOfBoundsQueriesLocally(t *testing.T) {
	client, err := New(Config{BaseURL: "https://kova.example.com", Token: "token", HTTPClient: &http.Client{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.ListBuildsPage(context.Background(), 0, ""); err == nil {
		t.Fatal("expected zero page size to fail")
	}
	if _, err := client.ListBuildsPage(context.Background(), apiv1.MaxListBuildsPageSize+1, ""); err == nil {
		t.Fatal("expected oversized page to fail")
	}
	if _, err := client.GetLogs(context.Background(), "job-1", apiv1.MaxLogTailLines+1); err == nil {
		t.Fatal("expected oversized log tail to fail")
	}
}

func TestClientBoundsResponseBodies(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", 1025)))
	}))
	defer server.Close()
	client, err := New(Config{BaseURL: server.URL, Token: "token", HTTPClient: server.Client(), MaxResponseBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetLogs(context.Background(), "job-1", 1); err == nil || !strings.Contains(err.Error(), "exceeds 1024 bytes") {
		t.Fatalf("unexpected response limit error: %v", err)
	}
	if _, err := New(Config{BaseURL: server.URL, Token: "token", HTTPClient: server.Client(), MaxResponseBytes: -1}); err == nil {
		t.Fatal("expected negative response limit to fail")
	}
}

func TestWaitBuildRespectsContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(apiv1.BuildJob{ID: "job-1", Status: apiv1.JobStatusRunning})
	}))
	defer server.Close()
	client, err := New(Config{BaseURL: server.URL, Token: "token", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = client.WaitBuild(ctx, "job-1", time.Second)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitBuild error=%v", err)
	}
}

func TestRetryAfterHTTPDate(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	delay := parseRetryAfter(now.Add(5*time.Second).Format(http.TimeFormat), now)
	if delay != 5*time.Second {
		t.Fatalf("delay=%s", delay)
	}
}

func TestClientAppliesTLSAndKubeconfigAuthentication(t *testing.T) {
	tlsServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(`{"jobs":[]}`))
	}))
	defer tlsServer.Close()
	tlsClient, err := New(Config{BaseURL: tlsServer.URL, Token: "secret", Insecure: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tlsClient.ListBuilds(context.Background()); err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer kube-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(`{"jobs":[]}`))
	}))
	defer server.Close()
	kubeconfig := filepath.Join(t.TempDir(), "config")
	raw := `apiVersion: v1
kind: Config
clusters:
- name: cluster
  cluster:
    server: https://kubernetes.invalid
contexts:
- name: context
  context:
    cluster: cluster
    user: user
current-context: context
users:
- name: user
  user:
    token: kube-token
`
	if err := os.WriteFile(kubeconfig, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	kubeClient, err := New(Config{BaseURL: server.URL, Kubeconfig: kubeconfig})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kubeClient.ListBuilds(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func writeTestError(w http.ResponseWriter, status int, code apiv1.ErrorCode, retryable bool) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(apiv1.ErrorResponse{Code: code, Message: "request failed", Retryable: retryable})
}
