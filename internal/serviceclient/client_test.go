package serviceclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/cofy-x/kova/internal/serviceapi"
)

func TestClientUsesBearerTokenAndTypedResponses(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret" {
			http.Error(w, "missing token", http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/v1/builds":
			_ = json.NewEncoder(w).Encode(serviceapi.JobList{Jobs: []serviceapi.BuildJob{{ID: "job-1", Status: serviceapi.JobStatusRunning}}})
		case "/v1/builds/job-1":
			_ = json.NewEncoder(w).Encode(serviceapi.BuildJob{ID: "job-1", Status: serviceapi.JobStatusSucceeded})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := New(Config{BaseURL: server.URL, Token: "secret", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	list, err := client.List(context.Background())
	if err != nil || len(list.Jobs) != 1 || list.Jobs[0].ID != "job-1" {
		t.Fatalf("list=%#v err=%v", list, err)
	}
	job, err := client.Get(context.Background(), "job-1")
	if err != nil || job.Status != serviceapi.JobStatusSucceeded {
		t.Fatalf("job=%#v err=%v", job, err)
	}
}

func TestClientAppliesTLSOptionsWithBearerToken(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret" {
			http.Error(w, "missing token", http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(`{"jobs":[]}`))
	}))
	defer server.Close()
	client, err := New(Config{BaseURL: server.URL, Token: "secret", Insecure: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.List(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCreateBuildStreamsMultipartArchive(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/version" {
			_ = json.NewEncoder(w).Encode(serviceapi.VersionInfo{APIVersion: serviceapi.APIVersion})
			return
		}
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		file, _, err := r.FormFile("file")
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		_ = file.Close()
		if r.FormValue("target") != "registry.example/app:dev" || r.FormValue("format") != "oci" {
			http.Error(w, "invalid fields", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(serviceapi.BuildJob{ID: "job-1", Status: serviceapi.JobStatusQueued})
	}))
	defer server.Close()
	archive := filepath.Join(t.TempDir(), "source.zip")
	if err := os.WriteFile(archive, []byte("archive"), 0o600); err != nil {
		t.Fatal(err)
	}
	client, err := New(Config{BaseURL: server.URL, Token: "secret", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	job, err := client.CreateBuild(context.Background(), CreateBuildOptions{
		ArchivePath: archive, Target: "registry.example/app:dev", Format: "oci", Concurrency: 1,
	})
	if err != nil || job.ID != "job-1" {
		t.Fatalf("job=%#v err=%v", job, err)
	}
}

func TestNewRejectsUnsafeServiceURLs(t *testing.T) {
	for _, value := range []string{"", "ftp://example.com", "https://user@example.com", "https://example.com?q=1"} {
		if _, err := New(Config{BaseURL: value, Token: "token"}); err == nil {
			t.Fatalf("expected %q to fail", value)
		}
	}
}

func TestClientUsesKubeconfigCredentialsWithoutPersistingAToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer kube-token" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
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
	client, err := New(Config{BaseURL: server.URL, Kubeconfig: kubeconfig})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.List(context.Background()); err != nil {
		t.Fatal(err)
	}
}
