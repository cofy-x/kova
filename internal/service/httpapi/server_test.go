package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/artifactstore"
	serviceauth "github.com/cofy-x/kova/internal/service/auth"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type staleCachedReaderClient struct {
	client.Client
	stale bool
}

func (c *staleCachedReaderClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if c.stale {
		return apierrors.NewNotFound(schema.GroupResource{Group: kovav1.Group, Resource: "kovabuilds"}, key.Name)
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

func TestCreateBuildRequiresAuth(t *testing.T) {
	srv := newTestServer(t, &fakeKube{})
	req := httptest.NewRequest(http.MethodGet, "/v1/builds", nil)
	rec := httptest.NewRecorder()

	srv.routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestOwnerAccessIsIsolatedWithoutAdministrativeRBAC(t *testing.T) {
	srv := newTestServer(t, &fakeKube{})
	srv.authz = authorizerFunc(func(context.Context, serviceauth.Principal, serviceauth.Attributes) error {
		return fmt.Errorf("denied")
	})
	owned := &kovav1.KovaBuild{
		ObjectMeta: metav1.ObjectMeta{Name: "owned", Namespace: "jobs", Labels: map[string]string{requesterLabel: requesterID("test-user")}},
		Spec:       kovav1.KovaBuildSpec{Requester: kovav1.KovaBuildRequester{Username: "test-user"}},
	}
	other := &kovav1.KovaBuild{
		ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "jobs", Labels: map[string]string{requesterLabel: requesterID("other-user")}},
		Spec:       kovav1.KovaBuildSpec{Requester: kovav1.KovaBuildRequester{Username: "other-user"}},
	}
	if err := srv.client.Create(context.Background(), owned); err != nil {
		t.Fatal(err)
	}
	if err := srv.client.Create(context.Background(), other); err != nil {
		t.Fatal(err)
	}

	request := func(endpoint string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, endpoint, nil)
		req.Header.Set("Authorization", "Bearer token")
		rec := httptest.NewRecorder()
		srv.routes().ServeHTTP(rec, req)
		return rec
	}
	if rec := request("/v1/builds/owned"); rec.Code != http.StatusOK {
		t.Fatalf("owner get status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec := request("/v1/builds/other"); rec.Code != http.StatusForbidden {
		t.Fatalf("other get status=%d body=%s", rec.Code, rec.Body.String())
	}
	list := request("/v1/builds")
	if list.Code != http.StatusOK || !strings.Contains(list.Body.String(), `"id":"owned"`) || strings.Contains(list.Body.String(), `"id":"other"`) {
		t.Fatalf("filtered list status=%d body=%s", list.Code, list.Body.String())
	}
}

func TestCreateBuildRequiresCreateAuthorization(t *testing.T) {
	srv := newTestServer(t, &fakeKube{})
	srv.authz = authorizerFunc(func(_ context.Context, _ serviceauth.Principal, attrs serviceauth.Attributes) error {
		if attrs.Resource != serviceauth.ServiceBuildResource {
			t.Fatalf("authorization resource = %q", attrs.Resource)
		}
		return fmt.Errorf("denied")
	})
	req := multipartBuildRequest(t, map[string]string{"format": "oci", "target": "registry.local/example:dev"})
	req.Header.Set("Authorization", "Bearer token")
	rec := httptest.NewRecorder()
	srv.routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestTailLogLines(t *testing.T) {
	for _, tc := range []struct {
		name  string
		raw   string
		lines int64
		want  string
	}{
		{name: "last two", raw: "one\ntwo\nthree\n", lines: 2, want: "two\nthree\n"},
		{name: "all", raw: "one\ntwo\n", lines: 10, want: "one\ntwo\n"},
		{name: "zero", raw: "one\n", lines: 0, want: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := string(tailLogLines([]byte(tc.raw), tc.lines)); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestCreateBuildWritesSourceAndCreatesCR(t *testing.T) {
	root := t.TempDir()
	srv := newTestServerWithRoot(t, &fakeKube{}, root)
	req := multipartBuildRequest(t, map[string]string{
		"format":      "oci",
		"target":      "registry.local/example:dev",
		"concurrency": "2",
		"var":         "KOVA_IMAGE_REGISTRY=registry.local",
	})
	req.Header.Set("Authorization", "Bearer token")
	rec := httptest.NewRecorder()

	srv.routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var job BuildJob
	if err := json.Unmarshal(rec.Body.Bytes(), &job); err != nil {
		t.Fatal(err)
	}
	var build kovav1.KovaBuild
	if err := srv.client.Get(context.Background(), kubeObjectKey("jobs", job.ID), &build); err != nil {
		t.Fatal(err)
	}
	if build.Spec.Build.Format != "oci" || build.Spec.Build.Concurrency != 2 {
		t.Fatalf("build options = %#v", build.Spec.Build)
	}
	if len(build.Spec.Targets) != 1 || build.Spec.Targets[0] != "registry.local/example:dev" {
		t.Fatalf("build targets = %#v", build.Spec.Targets)
	}
	uri, err := url.Parse(build.Spec.Source.URI)
	if err != nil || uri.Scheme != "file" || build.Spec.Source.Digest == "" {
		t.Fatalf("source = %#v err=%v", build.Spec.Source, err)
	}
	if _, err := os.Stat(uri.Path); err != nil {
		t.Fatalf("source zip not written: %v", err)
	}
}

func TestCreateBuildRejectsRequesterQueueOverflow(t *testing.T) {
	srv := newTestServer(t, &fakeKube{})
	srv.cfg.MaxQueuedJobsPerRequester = 1
	existing := &kovav1.KovaBuild{
		ObjectMeta: metav1.ObjectMeta{Name: "queued", Namespace: "jobs", Labels: map[string]string{requesterLabel: requesterID("test-user")}},
		Spec:       kovav1.KovaBuildSpec{Requester: kovav1.KovaBuildRequester{Username: "test-user"}},
		Status:     kovav1.KovaBuildStatus{Phase: kovav1.PhaseQueued},
	}
	if err := srv.client.Create(context.Background(), existing); err != nil {
		t.Fatal(err)
	}
	req := multipartBuildRequest(t, map[string]string{"format": "oci", "target": "registry.local/example:dev"})
	req.Header.Set("Authorization", "Bearer token")
	rec := httptest.NewRecorder()
	srv.routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("status=%d headers=%v body=%s", rec.Code, rec.Header(), rec.Body.String())
	}
}

func TestCreateBuildUsesArchiveTargetWithoutOverride(t *testing.T) {
	srv := newTestServer(t, &fakeKube{})
	req := multipartBuildRequest(t, map[string]string{"format": "oci"})
	req.Header.Set("Authorization", "Bearer token")
	rec := httptest.NewRecorder()

	srv.routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestCreateBuildStoresBatchArchiveTargets(t *testing.T) {
	srv := newTestServer(t, &fakeKube{})
	targets := []string{"registry.local/example:a", "registry.local/example:b"}
	req := multipartBuildRequestWithTargets(t, map[string]string{"format": "both"}, targets)
	req.Header.Set("Authorization", "Bearer token")
	rec := httptest.NewRecorder()

	srv.routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var job BuildJob
	if err := json.Unmarshal(rec.Body.Bytes(), &job); err != nil {
		t.Fatal(err)
	}
	var build kovav1.KovaBuild
	if err := srv.client.Get(context.Background(), kubeObjectKey("jobs", job.ID), &build); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(build.Spec.Targets) != fmt.Sprint(targets) {
		t.Fatalf("build targets = %#v, want %#v", build.Spec.Targets, targets)
	}
}

func TestCreateBuildRejectsOversizedRequest(t *testing.T) {
	srv := newTestServer(t, &fakeKube{})
	srv.cfg.MaxUploadBytes = 32
	req := multipartBuildRequest(t, map[string]string{"format": "oci", "target": "registry.local/example:dev"})
	req.Header.Set("Authorization", "Bearer token")
	rec := httptest.NewRecorder()

	srv.routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestCreateBuildRejectsUnsupportedField(t *testing.T) {
	srv := newTestServer(t, &fakeKube{})
	req := multipartBuildRequest(t, map[string]string{"addrs": "tcp://other:9094"})
	req.Header.Set("Authorization", "Bearer token")
	rec := httptest.NewRecorder()

	srv.routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestCreateBuildRejectsArchiveTargetMismatch(t *testing.T) {
	srv := newTestServer(t, &fakeKube{})
	fields := map[string]string{"target": "registry.local/requested:dev", "format": "oci"}
	req := multipartBuildRequestWithTarget(t, fields, "registry.local/other:dev")
	req.Header.Set("Authorization", "Bearer token")
	rec := httptest.NewRecorder()
	srv.routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "does not match") {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestCreateBuildRejectsInvalidOptions(t *testing.T) {
	for name, fields := range map[string]map[string]string{
		"format":      {"format": "bad"},
		"concurrency": {"concurrency": "0"},
		"retry":       {"retry": "-1"},
		"bool":        {"verbose": "maybe"},
		"duration":    {"oom-cooldown": "-1s"},
		"var":         {"var": "missing-equals"},
	} {
		t.Run(name, func(t *testing.T) {
			srv := newTestServer(t, &fakeKube{})
			req := multipartBuildRequest(t, fields)
			req.Header.Set("Authorization", "Bearer token")
			rec := httptest.NewRecorder()

			srv.routes().ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestCreateBuildIsIdempotentAndRejectsConflicts(t *testing.T) {
	srv := newTestServer(t, &fakeKube{})
	fields := map[string]string{
		"formats":         "oci,nydus",
		"target":          "registry.local/tasksets/demo:payload",
		"idempotency_key": "taskset-demo-aaaaaaaa",
	}
	create := func(fields map[string]string) *httptest.ResponseRecorder {
		req := multipartBuildRequest(t, fields)
		req.Header.Set("Authorization", "Bearer token")
		rec := httptest.NewRecorder()
		srv.routes().ServeHTTP(rec, req)
		return rec
	}
	first := create(fields)
	if first.Code != http.StatusAccepted {
		t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
	}
	second := create(fields)
	if second.Code != http.StatusOK {
		t.Fatalf("second status=%d body=%s", second.Code, second.Body.String())
	}
	var a, b BuildJob
	if err := json.Unmarshal(first.Body.Bytes(), &a); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(second.Body.Bytes(), &b); err != nil {
		t.Fatal(err)
	}
	if a.ID != b.ID || a.SourceDigest == "" || a.SourceDigest != b.SourceDigest {
		t.Fatalf("jobs = %#v %#v", a, b)
	}

	conflictFields := make(map[string]string, len(fields))
	for key, value := range fields {
		conflictFields[key] = value
	}
	conflictFields["target"] = "registry.local/tasksets/other:payload"
	conflict := create(conflictFields)
	if conflict.Code != http.StatusConflict {
		t.Fatalf("conflict status=%d body=%s", conflict.Code, conflict.Body.String())
	}
	var original kovav1.KovaBuild
	if err := srv.client.Get(context.Background(), kubeObjectKey("jobs", a.ID), &original); err != nil {
		t.Fatal(err)
	}
	uri, err := url.Parse(original.Spec.Source.URI)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(uri.Path); err != nil {
		t.Fatalf("conflicting request removed the committed source: %v", err)
	}
}

func TestCreateBuildIdempotencyUsesStrongReaderAfterAlreadyExists(t *testing.T) {
	root := t.TempDir()
	scheme := runtime.NewScheme()
	if err := kovav1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	strong := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).Build()
	cached := &staleCachedReaderClient{Client: strong}
	store, err := artifactstore.NewFilesystem(root)
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := serviceauth.New(serviceauth.ModeStatic, "token", "test-user", nil)
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer(testConfig(root), &fakeKube{}, cached, strong, store, authenticator, serviceauth.AllowAllAuthorizer{})
	fields := map[string]string{
		"formats":         "oci,nydus",
		"target":          "registry.local/tasksets/demo:payload",
		"idempotency_key": "taskset-demo-strong-read",
	}
	create := func() *httptest.ResponseRecorder {
		req := multipartBuildRequest(t, fields)
		req.Header.Set("Authorization", "Bearer token")
		rec := httptest.NewRecorder()
		srv.routes().ServeHTTP(rec, req)
		return rec
	}
	if first := create(); first.Code != http.StatusAccepted {
		t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
	}
	cached.stale = true
	if second := create(); second.Code != http.StatusOK {
		t.Fatalf("second status=%d body=%s", second.Code, second.Body.String())
	}
}

func TestBuildResultsReturnsTypedStoredResults(t *testing.T) {
	srv := newTestServer(t, &fakeKube{})
	build := &kovav1.KovaBuild{
		ObjectMeta: metav1.ObjectMeta{Name: "typed", Namespace: "jobs"},
		Spec:       kovav1.KovaBuildSpec{Source: kovav1.KovaBuildSourceSpec{URI: "file:///tmp/source.zip", Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, IdempotencyKey: "key", Build: kovav1.KovaBuildOptions{Format: "both", Target: "registry.local/demo:payload"}},
		Status:     kovav1.KovaBuildStatus{Phase: kovav1.PhaseSucceeded, Results: []kovav1.BuildResult{{Format: "oci", Status: "succeeded", Repository: "registry.local/demo:payload", ManifestDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", MediaType: "application/vnd.oci.image.manifest.v1+json", Size: 123}}},
	}
	if err := srv.client.Create(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	if err := srv.client.Status().Update(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/builds/typed/results", nil)
	req.Header.Set("Authorization", "Bearer token")
	rec := httptest.NewRecorder()
	srv.routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var response buildResultsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Results) != 1 || response.Results[0].ManifestDigest == "" || response.IdempotencyKey != "key" {
		t.Fatalf("response=%#v", response)
	}
}

func TestListGetLogsExportPreheatAndCancel(t *testing.T) {
	kube := &fakeKube{logs: "hello\n"}
	srv := newTestServer(t, kube)
	now := metav1.Now()
	build := &kovav1.KovaBuild{
		TypeMeta: metav1.TypeMeta{APIVersion: kovav1.Group + "/" + kovav1.Version, Kind: "KovaBuild"},
		ObjectMeta: metav1.ObjectMeta{
			Name:              "abc",
			Namespace:         "jobs",
			CreationTimestamp: now,
		},
		Status: kovav1.KovaBuildStatus{
			Phase:         kovav1.PhaseRunning,
			RunnerPodName: "kova-job-abc",
		},
	}
	if err := srv.client.Create(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	if err := srv.client.Status().Update(context.Background(), build); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		method string
		path   string
		want   string
	}{
		{http.MethodGet, "/v1/builds", "abc"},
		{http.MethodGet, "/v1/builds/abc", "running"},
		{http.MethodGet, "/v1/builds/abc/logs", "hello\n"},
		{http.MethodPost, "/v1/builds/abc/export", "one\n"},
		{http.MethodPost, "/v1/builds/abc/preheat", `{"status":"completed"}`},
	} {
		req := httptest.NewRequest(tc.method, tc.path, nil)
		req.Header.Set("Authorization", "Bearer token")
		rec := httptest.NewRecorder()
		srv.routes().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s status = %d body=%s", tc.path, rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), tc.want) {
			t.Fatalf("%s body = %q, want substring %q", tc.path, rec.Body.String(), tc.want)
		}
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/builds/abc/cancel", nil)
	req.Header.Set("Authorization", "Bearer token")
	rec := httptest.NewRecorder()
	srv.routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("cancel status = %d body=%s", rec.Code, rec.Body.String())
	}
	var cancelled kovav1.KovaBuild
	if err := srv.client.Get(context.Background(), kubeObjectKey("jobs", "abc"), &cancelled); err != nil {
		t.Fatal(err)
	}
	if cancelled.Status.Phase != kovav1.PhaseRunning {
		t.Fatalf("API handler wrote terminal status directly: %s", cancelled.Status.Phase)
	}
	if cancelled.Annotations[kovav1.CancellationRequestedAnnotation] == "" {
		t.Fatalf("cancellation request was not persisted: %#v", cancelled.Annotations)
	}
}

func TestCancelDoesNotOperateRunnerDirectly(t *testing.T) {
	kube := &fakeKube{deleteErr: errors.New("delete failed")}
	srv := newTestServer(t, kube)
	build := &kovav1.KovaBuild{
		TypeMeta: metav1.TypeMeta{APIVersion: kovav1.Group + "/" + kovav1.Version, Kind: "KovaBuild"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "abc",
			Namespace: "jobs",
		},
		Status: kovav1.KovaBuildStatus{
			Phase:         kovav1.PhaseRunning,
			RunnerPodName: "kova-job-abc",
		},
	}
	if err := srv.client.Create(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	if err := srv.client.Status().Update(context.Background(), build); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/builds/abc/cancel", nil)
	req.Header.Set("Authorization", "Bearer token")
	rec := httptest.NewRecorder()
	srv.routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if len(kube.deleted) != 0 {
		t.Fatalf("API handler deleted runner Pods directly: %#v", kube.deleted)
	}
}
