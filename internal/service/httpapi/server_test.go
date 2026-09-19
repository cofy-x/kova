package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	serviceauth "github.com/cofy-x/kova/internal/service/auth"
	apiv1 "github.com/cofy-x/kova/pkg/api/v1"

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
	assertAPIError(t, request("/v1/builds/other"), http.StatusForbidden, apiv1.ErrorCodeForbidden, false)
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

func TestCreateBuildCreatesCRFromImmutableSource(t *testing.T) {
	root := t.TempDir()
	srv := newTestServerWithRoot(t, &fakeKube{}, root)
	req := multipartBuildRequest(t, map[string]string{
		"format":      "oci",
		"target":      "registry.local/example:dev",
		"concurrency": "1",
		"var":         "KOVA_IMAGE_REGISTRY=registry.local",
	})
	req.Header.Set("Authorization", "Bearer token")
	rec := httptest.NewRecorder()

	srv.routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var job apiv1.BuildJob
	if err := json.Unmarshal(rec.Body.Bytes(), &job); err != nil {
		t.Fatal(err)
	}
	var build kovav1.KovaBuild
	if err := srv.client.Get(context.Background(), kubeObjectKey("jobs", job.ID), &build); err != nil {
		t.Fatal(err)
	}
	if build.Spec.Build.Format != "oci" || build.Spec.Build.Concurrency != 1 {
		t.Fatalf("build options = %#v", build.Spec.Build)
	}
	if len(build.Spec.Targets) != 1 || build.Spec.Targets[0] != "registry.local/example:dev" {
		t.Fatalf("build targets = %#v", build.Spec.Targets)
	}
	if !strings.HasPrefix(build.Spec.Source.URI, "oci://") || build.Spec.Source.Digest == "" {
		t.Fatalf("source = %#v", build.Spec.Source)
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
	if rec.Header().Get("Retry-After") == "" {
		t.Fatalf("status=%d headers=%v body=%s", rec.Code, rec.Header(), rec.Body.String())
	}
	assertAPIError(t, rec, http.StatusTooManyRequests, apiv1.ErrorCodeQueueCapacityExceeded, true)
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
	var job apiv1.BuildJob
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

func TestCreateBuildRejectsInvalidOptions(t *testing.T) {
	for name, fields := range map[string]map[string]string{
		"format":      {"format": "bad"},
		"concurrency": {"concurrency": "0"},
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

func TestCreateBuildEnforcesPublicRequestBounds(t *testing.T) {
	srv := newTestServer(t, &fakeKube{})
	valid := map[string]any{
		"source_uri":    "oci://registry.local/sources/test@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"source_digest": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		"targets":       []string{"registry.local/example:dev"},
		"concurrency":   1,
	}
	request := func(body map[string]any, suffix, contentType string) *httptest.ResponseRecorder {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/v1/builds", bytes.NewReader(append(raw, suffix...)))
		req.Header.Set("Authorization", "Bearer token")
		req.Header.Set("Content-Type", contentType)
		rec := httptest.NewRecorder()
		srv.routes().ServeHTTP(rec, req)
		return rec
	}

	assertAPIError(t, request(valid, "{}", "application/json"), http.StatusBadRequest, apiv1.ErrorCodeInvalidRequest, false)
	assertAPIError(t, request(valid, "", "text/plain"), http.StatusBadRequest, apiv1.ErrorCodeInvalidRequest, false)

	tooManyVariables := make([]string, apiv1.MaxBuildVariables+1)
	for index := range tooManyVariables {
		tooManyVariables[index] = fmt.Sprintf("KOVA_VALUE_%d=value", index)
	}
	withVariables := cloneRequest(valid)
	withVariables["variables"] = tooManyVariables
	assertAPIError(t, request(withVariables, "", "application/json"), http.StatusBadRequest, apiv1.ErrorCodeInvalidRequest, false)

	withLongVariable := cloneRequest(valid)
	withLongVariable["variables"] = []string{"KOVA_VALUE=" + strings.Repeat("x", apiv1.MaxBuildVariableLength)}
	assertAPIError(t, request(withLongVariable, "", "application/json"), http.StatusBadRequest, apiv1.ErrorCodeInvalidRequest, false)

	withDuplicateVariables := cloneRequest(valid)
	withDuplicateVariables["variables"] = []string{"KOVA_VALUE=one", "KOVA_VALUE=two"}
	assertAPIError(t, request(withDuplicateVariables, "", "application/json"), http.StatusBadRequest, apiv1.ErrorCodeInvalidRequest, false)

	withWhitespaceKey := cloneRequest(valid)
	withWhitespaceKey["idempotency_key"] = " request-123"
	assertAPIError(t, request(withWhitespaceKey, "", "application/json"), http.StatusBadRequest, apiv1.ErrorCodeInvalidRequest, false)

	withLongSource := cloneRequest(valid)
	withLongSource["source_uri"] = "https://sources.example.com/" + strings.Repeat("x", apiv1.MaxSourceURILength)
	assertAPIError(t, request(withLongSource, "", "application/json"), http.StatusBadRequest, apiv1.ErrorCodeInvalidRequest, false)

	oversized := cloneRequest(valid)
	oversized["source_uri"] = "https://sources.example.com/" + strings.Repeat("x", maxCreateBuildRequestBytes)
	assertAPIError(t, request(oversized, "", "application/json"), http.StatusBadRequest, apiv1.ErrorCodeInvalidRequest, false)
}

func cloneRequest(input map[string]any) map[string]any {
	output := make(map[string]any, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func TestCreateBuildEnforcesBoundedTaggedTargets(t *testing.T) {
	valid := make([]string, kovav1.MaxLogicalTargets)
	for index := range valid {
		valid[index] = fmt.Sprintf("registry.example.com/team/image-%03d:dev", index)
	}
	for name, targets := range map[string][]string{
		"too-many":    append(append([]string(nil), valid...), "registry.example.com/team/overflow:dev"),
		"duplicate":   {"registry.example.com/team/image:dev", "registry.example.com/team/image:dev"},
		"empty":       {""},
		"whitespace":  {" registry.example.com/team/image:dev"},
		"invalid":     {"not a reference"},
		"digest-only": {"registry.example.com/team/image@sha256:" + strings.Repeat("a", 64)},
		"too-long":    {"registry.example.com/team/" + strings.Repeat("a", 500) + ":dev"},
	} {
		t.Run(name, func(t *testing.T) {
			srv := newTestServer(t, &fakeKube{})
			req := multipartBuildRequestWithTargets(t, map[string]string{"format": "both"}, targets)
			req.Header.Set("Authorization", "Bearer token")
			rec := httptest.NewRecorder()
			srv.routes().ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
		})
	}

	srv := newTestServer(t, &fakeKube{})
	req := multipartBuildRequestWithTargets(t, map[string]string{"format": "both", "concurrency": "100"}, valid)
	req.Header.Set("Authorization", "Bearer token")
	rec := httptest.NewRecorder()
	srv.routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("100 targets status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestCreateBuildRejectsConcurrencyAboveTargetCount(t *testing.T) {
	srv := newTestServer(t, &fakeKube{})
	req := multipartBuildRequestWithTargets(t, map[string]string{"concurrency": "2"}, []string{"registry.example.com/team/image:dev"})
	req.Header.Set("Authorization", "Bearer token")
	rec := httptest.NewRecorder()
	srv.routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
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
	var a, b apiv1.BuildJob
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
	assertAPIError(t, conflict, http.StatusConflict, apiv1.ErrorCodeConflict, false)
	var original kovav1.KovaBuild
	if err := srv.client.Get(context.Background(), kubeObjectKey("jobs", a.ID), &original); err != nil {
		t.Fatal(err)
	}
	if original.Spec.Source.URI == "" {
		t.Fatal("original immutable source was not retained")
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
	authenticator, err := serviceauth.New(serviceauth.ModeStatic, "token", "test-user", nil)
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer(testConfig(root), &fakeKube{}, cached, strong, authenticator, serviceauth.AllowAllAuthorizer{})
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

func TestBuildResultsReturnsVerifiedOutputs(t *testing.T) {
	srv := newTestServer(t, &fakeKube{})
	build := &kovav1.KovaBuild{
		ObjectMeta: metav1.ObjectMeta{Name: "typed", Namespace: "jobs"},
		Spec:       kovav1.KovaBuildSpec{Source: kovav1.KovaBuildSourceSpec{URI: "https://sources.example.com/source.zip", Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, IdempotencyKey: "key", Build: kovav1.KovaBuildOptions{Format: "both"}},
		Status:     kovav1.KovaBuildStatus{Phase: kovav1.PhaseSucceeded, Outputs: []kovav1.BuildOutput{{Format: "oci", Image: "registry.local/demo:payload", ManifestDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}}},
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
	var response apiv1.BuildResults
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Outputs) != 1 || response.Outputs[0].ManifestDigest == "" || response.IdempotencyKey != "key" {
		t.Fatalf("response=%#v", response)
	}
	if response.Outputs[0].ImmutableRef != "registry.local/demo@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" {
		t.Fatalf("immutable_ref=%q", response.Outputs[0].ImmutableRef)
	}
}

func TestBuildJobExposesStableFailureWithoutRuntimeInternals(t *testing.T) {
	srv := newTestServer(t, &fakeKube{})
	build := &kovav1.KovaBuild{
		ObjectMeta: metav1.ObjectMeta{Name: "failed", Namespace: "jobs"},
		Status: kovav1.KovaBuildStatus{
			Phase: kovav1.PhaseFailed, Reason: "ResultVerificationFailed",
			Message:       "registry credential secret-name failed",
			RunnerPodName: "kova-job-failed",
		},
	}
	if err := srv.client.Create(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	if err := srv.client.Status().Update(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/builds/failed", nil)
	req.Header.Set("Authorization", "Bearer token")
	rec := httptest.NewRecorder()
	srv.routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var job apiv1.BuildJob
	if err := json.Unmarshal(rec.Body.Bytes(), &job); err != nil {
		t.Fatal(err)
	}
	if job.FailureCode != apiv1.BuildFailureVerificationFailed || job.Error != "one or more build results could not be verified" {
		t.Fatalf("job=%#v", job)
	}
	for _, forbidden := range []string{"secret-name", "pod_name", "namespace", "buildkit_addr"} {
		if strings.Contains(rec.Body.String(), forbidden) {
			t.Fatalf("public job leaked %q: %s", forbidden, rec.Body.String())
		}
	}
}

func TestPublicBuildFailureContract(t *testing.T) {
	for reason, want := range map[string]apiv1.BuildFailureCode{
		"InvalidSource":            apiv1.BuildFailureInvalidSource,
		"InvalidTargets":           apiv1.BuildFailureInvalidTargets,
		"RunnerCreateFailed":       apiv1.BuildFailureRunnerUnavailable,
		"RunnerUnavailable":        apiv1.BuildFailureRunnerUnavailable,
		"BuildSubmissionFailed":    apiv1.BuildFailureSubmissionFailed,
		"ResultVerificationFailed": apiv1.BuildFailureVerificationFailed,
		"BuildFailed":              apiv1.BuildFailureExecutionFailed,
		"Cancelled":                apiv1.BuildFailureCancelled,
	} {
		if got := publicBuildFailureCode(reason); got != want {
			t.Errorf("reason %q mapped to %q, want %q", reason, got, want)
		}
		if publicBuildError(reason) == "" {
			t.Errorf("reason %q has no safe public message", reason)
		}
	}
}

func TestBuildResultsRejectsInvalidStoredReferenceWithoutRetry(t *testing.T) {
	srv := newTestServer(t, &fakeKube{})
	build := &kovav1.KovaBuild{
		ObjectMeta: metav1.ObjectMeta{Name: "invalid-output", Namespace: "jobs"},
		Status: kovav1.KovaBuildStatus{Phase: kovav1.PhaseSucceeded, Outputs: []kovav1.BuildOutput{{
			Format: "oci", Image: "registry.local/demo", ManifestDigest: "sha256:" + strings.Repeat("b", 64),
		}}},
	}
	if err := srv.client.Create(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	if err := srv.client.Status().Update(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/builds/invalid-output/results", nil)
	req.Header.Set("Authorization", "Bearer token")
	rec := httptest.NewRecorder()
	srv.routes().ServeHTTP(rec, req)
	assertAPIError(t, rec, http.StatusInternalServerError, apiv1.ErrorCodeInternal, false)
	if strings.Contains(rec.Body.String(), "registry.local") {
		t.Fatalf("invalid stored output leaked: %s", rec.Body.String())
	}
}

func TestImmutableReferenceNormalization(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	for _, tc := range []struct {
		name  string
		image string
		want  string
	}{
		{name: "docker hub", image: "alpine:3.20", want: "index.docker.io/library/alpine@" + digest},
		{name: "registry port", image: "registry.example.com:5443/team/image:dev", want: "registry.example.com:5443/team/image@" + digest},
		{name: "nested repository", image: "registry.example.com/team/nested/image:release", want: "registry.example.com/team/nested/image@" + digest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := immutableReference(tc.image, digest)
			if err != nil || got != tc.want {
				t.Fatalf("immutableReference()=(%q, %v), want %q", got, err, tc.want)
			}
		})
	}
	for _, image := range []string{
		"registry.example.com/team/image",
		"registry.example.com/team/image@" + digest,
		" registry.example.com/team/image:dev",
		"oci://registry.example.com/team/image:dev",
	} {
		if _, err := immutableReference(image, digest); err == nil {
			t.Fatalf("expected image %q to fail", image)
		}
	}
	if _, err := immutableReference("registry.example.com/team/image:dev", "sha256:bad"); err == nil {
		t.Fatal("expected invalid digest to fail")
	}
}

func TestStructuredAPIErrorsDoNotExposeInternalDetails(t *testing.T) {
	srv := newTestServer(t, &fakeKube{})

	req := httptest.NewRequest(http.MethodGet, "/v1/builds", nil)
	rec := httptest.NewRecorder()
	srv.routes().ServeHTTP(rec, req)
	assertAPIError(t, rec, http.StatusUnauthorized, apiv1.ErrorCodeUnauthenticated, false)

	srv.cfg.RunnerImage = ""
	req = multipartBuildRequest(t, map[string]string{"target": "registry.local/example:dev"})
	req.Header.Set("Authorization", "Bearer token")
	rec = httptest.NewRecorder()
	srv.routes().ServeHTTP(rec, req)
	assertAPIError(t, rec, http.StatusInternalServerError, apiv1.ErrorCodeInternal, false)
	if strings.Contains(rec.Body.String(), "runner image") {
		t.Fatalf("internal implementation detail leaked: %s", rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/v1/builds/missing", nil)
	req.Header.Set("Authorization", "Bearer token")
	rec = httptest.NewRecorder()
	srv.routes().ServeHTTP(rec, req)
	assertAPIError(t, rec, http.StatusNotFound, apiv1.ErrorCodeNotFound, false)

	srv = newTestServer(t, &fakeKube{})
	req = multipartBuildRequest(t, map[string]string{"var": "SECRET=do-not-expose"})
	req.Header.Set("Authorization", "Bearer token")
	rec = httptest.NewRecorder()
	srv.routes().ServeHTTP(rec, req)
	assertAPIError(t, rec, http.StatusBadRequest, apiv1.ErrorCodeInvalidRequest, false)
	if strings.Contains(rec.Body.String(), "do-not-expose") {
		t.Fatalf("invalid variable value leaked: %s", rec.Body.String())
	}
}

func TestLogsUnavailableRetryability(t *testing.T) {
	srv := newTestServer(t, &fakeKube{})
	build := &kovav1.KovaBuild{
		ObjectMeta: metav1.ObjectMeta{Name: "logs", Namespace: "jobs"},
		Status:     kovav1.KovaBuildStatus{Phase: kovav1.PhaseQueued},
	}
	if err := srv.client.Create(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	oversizedReq := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/v1/builds/logs/logs?tail_lines=%d", apiv1.MaxLogTailLines+1), nil)
	oversizedReq.Header.Set("Authorization", "Bearer token")
	oversizedRec := httptest.NewRecorder()
	srv.routes().ServeHTTP(oversizedRec, oversizedReq)
	assertAPIError(t, oversizedRec, http.StatusBadRequest, apiv1.ErrorCodeInvalidRequest, false)

	request := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/v1/builds/logs/logs", nil)
		req.Header.Set("Authorization", "Bearer token")
		rec := httptest.NewRecorder()
		srv.routes().ServeHTTP(rec, req)
		return rec
	}
	assertAPIError(t, request(), http.StatusNotFound, apiv1.ErrorCodeLogsUnavailable, true)

	build.Status.Phase = kovav1.PhaseSucceeded
	build.Status.RunnerPodName = "kova-job-logs"
	if err := srv.client.Status().Update(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	assertAPIError(t, request(), http.StatusGone, apiv1.ErrorCodeLogsUnavailable, false)
}

func assertAPIError(t *testing.T, rec *httptest.ResponseRecorder, status int, code apiv1.ErrorCode, retryable bool) {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var response apiv1.ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Code != code || response.Message == "" || response.Retryable != retryable {
		t.Fatalf("error response=%#v", response)
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
