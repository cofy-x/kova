package httpapi

import (
	"archive/zip"
	"bytes"
	"context"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/kube"
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
	return NewServer(testConfig(root), kube, client, client)
}

func testConfig(root string) config.Config {
	return config.Config{
		Namespace:             "jobs",
		RunnerImage:           "registry.local/kova:dev",
		RunnerImagePullPolicy: "IfNotPresent",
		BuildkitAddr:          "tcp://kova.kova.svc:9094",
		SourcePVCClaim:        "kova-sources",
		SourceRoot:            root,
		SourceMountPath:       root,
		JobTTL:                time.Hour,
		AuthToken:             "token",
		WaitTimeout:           time.Second,
		PollInterval:          time.Millisecond,
	}
}

func multipartBuildRequest(t *testing.T, fields map[string]string) *http.Request {
	return multipartBuildRequestWithTarget(t, fields, fields["target"])
}

func multipartBuildRequestWithTarget(t *testing.T, fields map[string]string, archiveTarget string) *http.Request {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	file, err := writer.CreateFormFile("file", "source.zip")
	if err != nil {
		t.Fatal(err)
	}
	var archive bytes.Buffer
	zw := zip.NewWriter(&archive)
	dockerfile, err := zw.Create("image/Dockerfile")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = dockerfile.Write([]byte("FROM scratch\n"))
	metadata, err := zw.Create("image/metadata.json")
	if err != nil {
		t.Fatal(err)
	}
	target := archiveTarget
	if target == "" {
		target = "registry.local/example:dev"
	}
	_, _ = metadata.Write([]byte(`{"target":"` + target + `"}`))
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(archive.Bytes()); err != nil {
		t.Fatal(err)
	}
	for key, value := range fields {
		if err := writer.WriteField(key, value); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/builds", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	return req
}

func kubeObjectKey(namespace string, name string) client.ObjectKey {
	return client.ObjectKey{Namespace: namespace, Name: name}
}
