package buildcontroller

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/kube"
	"github.com/cofy-x/kova/internal/service/config"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func testVerifiedImage(t *testing.T, arch string) (string, string, string) {
	t.Helper()
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	server := httptest.NewServer(registry.New())
	t.Cleanup(server.Close)
	host := strings.TrimPrefix(server.URL, "http://")
	ref, err := name.NewTag(host+"/demo:dev", name.Insecure)
	if err != nil {
		t.Fatal(err)
	}
	imageConfig, err := empty.Image.ConfigFile()
	if err != nil {
		t.Fatal(err)
	}
	imageConfig.OS, imageConfig.Architecture = "linux", arch
	image, err := mutate.ConfigFile(empty.Image, imageConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(ref, image); err != nil {
		t.Fatal(err)
	}
	digest, err := image.Digest()
	if err != nil {
		t.Fatal(err)
	}
	return host, ref.Name(), digest.String()
}

func TestVerificationRetriesTransientExportAfterControllerRestartWithoutRepost(t *testing.T) {
	host, ref, digest := testVerifiedImage(t, "amd64")
	build := lifecycleBuild("resume-verification", kovav1.PhaseRunning)
	build.Spec.Targets = buildTargets(ref)
	client := crfake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build).Build()
	exports, buildPosts := 0, 0
	kubeClient := &fakeKube{execFn: func(opts kube.ExecOptions) error {
		command := strings.Join(opts.Command, " ")
		switch {
		case strings.Contains(command, "--method GET"):
			_, _ = fmt.Fprintf(opts.Stdout, `{"status":"completed","requestId":%q}`, string(build.UID))
		case strings.Contains(command, "/api/v1/export"):
			exports++
			if exports == 1 {
				return errors.New("temporary exec disconnect")
			}
			_, _ = fmt.Fprintf(opts.Stdout, "{\"target\":%q,\"success\":true,\"manifest_digest\":%q}\n", ref, digest)
		case strings.Contains(command, "--method POST"):
			buildPosts++
		default:
			t.Fatalf("unexpected runner command: %s", command)
		}
		return nil
	}}
	cfg := config.Config{VerificationAttemptTimeout: time.Second, VerificationWindow: time.Minute, RegistryPlainHTTP: []string{host}}
	r := KovaBuildReconciler{Client: client, Scheme: testScheme(t), Kube: kubeClient, Cfg: cfg}
	key := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: build.Namespace, Name: build.Name}}
	if _, err := r.Reconcile(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	if got := storedLifecycleBuild(t, client, build.Name).Status.Phase; got != kovav1.PhaseVerifying {
		t.Fatalf("phase = %s", got)
	}
	if _, err := r.Reconcile(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	stored := storedLifecycleBuild(t, client, build.Name)
	if stored.Status.Phase != kovav1.PhaseVerifying || stored.Status.VerificationAttempts != 1 || stored.Status.VerificationLastError == "" {
		t.Fatalf("status after transient error = %#v", stored.Status)
	}
	// Simulate the persisted retry time elapsing and another leader taking over.
	past := metav1.NewTime(time.Now().Add(-time.Second))
	stored.Status.VerificationNextAttemptAt = &past
	if err := client.Status().Update(context.Background(), stored); err != nil {
		t.Fatal(err)
	}
	newLeader := KovaBuildReconciler{Client: client, Scheme: testScheme(t), Kube: kubeClient, Cfg: cfg}
	if _, err := newLeader.Reconcile(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	stored = storedLifecycleBuild(t, client, build.Name)
	if stored.Status.Phase != kovav1.PhaseSucceeded || stored.Status.VerificationAttempts != 2 || len(stored.Status.Outputs) != 1 || stored.Status.Outputs[0].ManifestDigest != digest || buildPosts != 0 || exports != 2 {
		t.Fatalf("status=%#v buildPosts=%d exports=%d", stored.Status, buildPosts, exports)
	}
}

func TestVerificationFailsImmediatelyOnPlatformMismatch(t *testing.T) {
	host, ref, digest := testVerifiedImage(t, "arm64")
	build := lifecycleBuild("wrong-platform", kovav1.PhaseVerifying)
	build.Spec.Targets = buildTargets(ref)
	now := metav1.Now()
	deadline := metav1.NewTime(now.Add(time.Minute))
	build.Status.VerificationStartedAt = &now
	build.Status.VerificationDeadlineAt = &deadline
	build.Status.VerificationResults = []kovav1.BuildVerificationResult{{Format: "oci", Image: ref, Platform: "linux/amd64", PushedDigest: digest, State: "pending"}}
	client := crfake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build).Build()
	kubeClient := &fakeKube{execFn: func(kube.ExecOptions) error { t.Fatal("digest receipt should skip runner export"); return nil }}
	r := KovaBuildReconciler{Client: client, Scheme: testScheme(t), Kube: kubeClient, Cfg: config.Config{RegistryPlainHTTP: []string{host}}}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: build.Namespace, Name: build.Name}}); err != nil {
		t.Fatal(err)
	}
	stored := storedLifecycleBuild(t, client, build.Name)
	if stored.Status.Phase != kovav1.PhaseFailed || stored.Status.Reason != "ResultVerificationFailed" || stored.Status.VerificationAttempts != 1 || len(kubeClient.deleted) != 1 {
		t.Fatalf("status=%#v deleted=%#v", stored.Status, kubeClient.deleted)
	}
}

func TestCancellationOfVerifyingBuildSkipsRunnerAndDeletesPod(t *testing.T) {
	build := lifecycleBuild("cancel-verification", kovav1.PhaseVerifying)
	build.Annotations = map[string]string{kovav1.CancellationRequestedAnnotation: "2026-01-01T00:00:00Z"}
	client := crfake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build).Build()
	kubeClient := &fakeKube{execFn: func(kube.ExecOptions) error { t.Fatal("cancelled verification must not call runner"); return nil }}
	r := KovaBuildReconciler{Client: client, Scheme: testScheme(t), Kube: kubeClient}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: build.Namespace, Name: build.Name}}); err != nil {
		t.Fatal(err)
	}
	stored := storedLifecycleBuild(t, client, build.Name)
	if stored.Status.Phase != kovav1.PhaseCancelled || len(kubeClient.deleted) != 1 {
		t.Fatalf("status=%#v deleted=%#v", stored.Status, kubeClient.deleted)
	}
}

func TestVerificationDeadlineFailsPendingOutputWithoutRunnerPost(t *testing.T) {
	build := lifecycleBuild("expired-verification", kovav1.PhaseVerifying)
	started := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	deadline := metav1.NewTime(time.Now().Add(-time.Minute))
	build.Status.VerificationStartedAt = &started
	build.Status.VerificationDeadlineAt = &deadline
	build.Status.VerificationLastError = "registry temporarily unavailable"
	build.Status.VerificationResults = []kovav1.BuildVerificationResult{{Format: "oci", Image: build.Spec.Targets[0].Target, Platform: "linux/amd64", State: "pending"}}
	client := crfake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build).Build()
	kubeClient := &fakeKube{execFn: func(kube.ExecOptions) error { t.Fatal("expired verification must not call runner"); return nil }}
	r := KovaBuildReconciler{Client: client, Scheme: testScheme(t), Kube: kubeClient}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: build.Namespace, Name: build.Name}}); err != nil {
		t.Fatal(err)
	}
	stored := storedLifecycleBuild(t, client, build.Name)
	if stored.Status.Phase != kovav1.PhaseFailed || stored.Status.Reason != "ResultVerificationFailed" || !strings.Contains(stored.Status.VerificationLastError, "deadline") || stored.Status.VerificationResults[0].State != "failed" || len(kubeClient.deleted) != 1 {
		t.Fatalf("status=%#v deleted=%#v", stored.Status, kubeClient.deleted)
	}
}

type contextExecKube struct {
	fakeKube
	exec func(context.Context, kube.ExecOptions) error
}

func (k *contextExecKube) Exec(ctx context.Context, _, _ string, opts kube.ExecOptions) error {
	return k.exec(ctx, opts)
}

func TestHungVerificationLeavesReconcileCapacityForOtherCompletionsAndAdmission(t *testing.T) {
	first := lifecycleBuild("hung-peer", kovav1.PhaseVerifying)
	second := lifecycleBuild("other-completion", kovav1.PhaseVerifying)
	queued := lifecycleBuild("queued-peer", kovav1.PhaseQueued)
	now := metav1.Now()
	deadline := metav1.NewTime(now.Add(time.Minute))
	for _, build := range []*kovav1.KovaBuild{first, second} {
		build.Status.VerificationStartedAt = &now
		build.Status.VerificationDeadlineAt = &deadline
		build.Status.VerificationResults = []kovav1.BuildVerificationResult{{Format: "oci", Image: build.Spec.Targets[0].Target, Platform: "linux/amd64", State: "pending"}}
		build.Status.AllocatedConcurrency = 1
	}
	client := crfake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(first, second, queued).Build()
	started := make(chan struct{})
	kubeClient := &contextExecKube{exec: func(ctx context.Context, opts kube.ExecOptions) error {
		if !strings.Contains(strings.Join(opts.Command, " "), "/api/v1/export") {
			return fmt.Errorf("unexpected runner command: %v", opts.Command)
		}
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}}
	r := KovaBuildReconciler{Client: client, Scheme: testScheme(t), Kube: kubeClient, Cfg: config.Config{
		RunnerImage: "runner:test", BuildkitPlatformAddrs: map[string]string{"linux/amd64": "worker:1234"},
		VerificationAttemptTimeout: 2 * time.Second, VerificationWindow: time.Minute,
		ControllerConcurrency: 2, MaxActiveJobs: 3, MaxActiveJobsPerRequester: 3, WorkerSlots: 3,
	}}
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	defer cancelFirst()
	firstDone := make(chan error, 1)
	go func() {
		_, err := r.Reconcile(firstCtx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: first.Namespace, Name: first.Name}})
		firstDone <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first verification did not start")
	}
	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: second.Namespace, Name: second.Name}})
	if err != nil || result.RequeueAfter <= 0 {
		t.Fatalf("other completion result=%#v err=%v", result, err)
	}
	admissionDone := make(chan error, 1)
	go func() {
		_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: queued.Namespace, Name: queued.Name}})
		admissionDone <- err
	}()
	select {
	case err := <-admissionDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("hung verification starved admission")
	}
	if got := storedLifecycleBuild(t, client, queued.Name).Status.Phase; got != kovav1.PhaseStarting {
		t.Fatalf("queued job phase=%s, want Starting", got)
	}
	cancelFirst()
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("hung verification ignored cancellation")
	}
	if got := storedLifecycleBuild(t, client, first.Name).Status.Phase; got != kovav1.PhaseVerifying {
		t.Fatalf("hung job phase=%s, want durable Verifying", got)
	}
}
