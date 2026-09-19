package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/kube"
	serviceauth "github.com/cofy-x/kova/internal/service/auth"
	"github.com/cofy-x/kova/internal/service/config"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type fakeKube struct {
	mu        sync.Mutex
	execs     [][]string
	deleted   []string
	logs      string
	deleteErr error
}

type authorizerFunc func(context.Context, serviceauth.Principal, serviceauth.Attributes) error

func (fn authorizerFunc) Authorize(ctx context.Context, principal serviceauth.Principal, attrs serviceauth.Attributes) error {
	return fn(ctx, principal, attrs)
}

func (f *fakeKube) GetSecretData(context.Context, string, string, string) (string, error) {
	return "", nil
}

func (f *fakeKube) PodExists(context.Context, string, string) (bool, error) {
	return false, nil
}

func (f *fakeKube) CreatePod(context.Context, *corev1.Pod) error {
	return nil
}

func (f *fakeKube) WaitPodReady(context.Context, string, string, time.Duration) error {
	return nil
}

func (f *fakeKube) DeletePod(_ context.Context, namespace string, name string) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, namespace+"/"+name)
	return nil
}

func (f *fakeKube) WritePodLogsTail(_ context.Context, _ string, _ string, _ int64, out io.Writer) error {
	_, err := io.WriteString(out, f.logs)
	return err
}

func (f *fakeKube) ListPods(context.Context, string, io.Writer, bool) error {
	return nil
}

func (f *fakeKube) ListPodsWithOptions(context.Context, string, io.Writer, kube.ListPodsOptions) error {
	return nil
}

func (f *fakeKube) Exec(_ context.Context, _ string, _ string, opts kube.ExecOptions) error {
	f.mu.Lock()
	f.execs = append(f.execs, append([]string{}, opts.Command...))
	f.mu.Unlock()
	command := strings.Join(opts.Command, " ")
	switch {
	case strings.Contains(command, "/api/v1/export"):
		if opts.Stdout != nil {
			_, _ = io.WriteString(opts.Stdout, "one\n")
		}
	case strings.Contains(command, "/api/v1/preheat"):
		if opts.Stdout != nil {
			_, _ = io.WriteString(opts.Stdout, `{"status":"completed"}`)
		}
	}
	return nil
}

func (f *fakeKube) ScaleDeployment(context.Context, string, string, int32) error {
	return nil
}

func newTestServer(t *testing.T, kube *fakeKube) *Server {
	t.Helper()
	return newTestServerWithRoot(t, kube, t.TempDir())
}

func newTestServerWithRoot(t *testing.T, kube *fakeKube, root string) *Server {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := kovav1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	client := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).Build()
	authenticator, err := serviceauth.New(serviceauth.ModeStatic, "token", "test-user", nil)
	if err != nil {
		t.Fatal(err)
	}
	return NewServer(testConfig(root), kube, client, client, authenticator, serviceauth.AllowAllAuthorizer{})
}

func testConfig(root string) config.Config {
	return config.Config{
		Namespace:             "jobs",
		RunnerImage:           "registry.local/kova:dev",
		RunnerImagePullPolicy: "IfNotPresent",
		BuildkitPlatformAddrs: map[string]string{"linux/amd64": "tcp://kova.kova.svc:9094"},
		JobTTL:                time.Hour,
		AuthToken:             "token",
		AuthMode:              serviceauth.ModeStatic,
		AuthStaticPrincipal:   "test-user",
		WaitTimeout:           time.Second,
		PollInterval:          time.Millisecond,
	}
}

func multipartBuildRequest(t *testing.T, fields map[string]string) *http.Request {
	return multipartBuildRequestWithTarget(t, fields, fields["target"])
}

func multipartBuildRequestWithTarget(t *testing.T, fields map[string]string, archiveTarget string) *http.Request {
	if archiveTarget == "" {
		archiveTarget = "registry.local/example:dev"
	}
	return multipartBuildRequestWithTargets(t, fields, []string{archiveTarget})
}

func multipartBuildRequestWithTargets(t *testing.T, fields map[string]string, archiveTargets []string) *http.Request {
	t.Helper()
	body := map[string]any{
		"source_uri":    "oci://registry.local/sources/test@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"source_digest": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		"targets":       requestTargets(archiveTargets),
		"concurrency":   1,
	}
	platform := fields["platform"]
	for key, value := range fields {
		switch key {
		case "target":
			body["targets"] = requestTargets([]string{value})
		case "platform":
			// Applied after target replacement so request construction does not
			// depend on randomized Go map iteration order.
			continue
		case "formats":
			body["format"] = "both"
		case "concurrency", "timeout":
			number, err := strconv.Atoi(value)
			if err != nil {
				body[key] = value
			} else {
				body[key] = number
			}
		case "fail-fast":
			parsed, err := strconv.ParseBool(value)
			if err != nil {
				body["fail_fast"] = value
			} else {
				body["fail_fast"] = parsed
			}
		case "verbose":
			parsed, err := strconv.ParseBool(value)
			if err != nil {
				body["verbose"] = value
			} else {
				body["verbose"] = parsed
			}
		case "oom-cooldown":
			body["oom_cooldown"] = value
		case "var":
			body["variables"] = []string{value}
		case "idempotency_key":
			body[key] = value
		default:
			body[key] = value
		}
	}
	if platform != "" {
		targets := body["targets"].([]map[string]string)
		for index := range targets {
			targets[index]["platform"] = platform
		}
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/builds", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	return req
}

func requestTargets(targets []string) []map[string]string {
	result := make([]map[string]string, 0, len(targets))
	for _, target := range targets {
		result = append(result, map[string]string{"target": target, "platform": "linux/amd64"})
	}
	return result
}

func kubeObjectKey(namespace string, name string) client.ObjectKey {
	return client.ObjectKey{Namespace: namespace, Name: name}
}
