package buildcontroller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/kube"
	"github.com/cofy-x/kova/internal/runner"
	"github.com/cofy-x/kova/internal/service/config"
	"github.com/cofy-x/kova/internal/service/runnerexec"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func failedVerificationRequest(build *kovav1.KovaBuild) ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Namespace: build.Namespace, Name: build.Name}}
}

func TestFailedRunnerWithNoPushedOutputEndsFailed(t *testing.T) {
	build := lifecycleBuild("no-output", kovav1.PhaseRunning)
	crClient := crfake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build, testRunnerPod(build)).Build()
	exports := 0
	kubeClient := &fakeKube{podClient: crClient, execFn: func(opts kube.ExecOptions) error {
		if !strings.Contains(strings.Join(opts.Command, " "), "/api/v1/export") {
			t.Fatal("failed runner must not be submitted or polled again")
		}
		exports++
		_, _ = fmt.Fprintf(opts.Stdout, "{\"target\":%q,\"success\":false,\"reason\":\"build failed\"}\n", build.Spec.Targets[0].Target)
		return nil
	}}
	r := KovaBuildReconciler{Client: crClient, Scheme: testScheme(t), Kube: kubeClient, Cfg: config.Config{VerificationWindow: time.Hour}}
	initializeAdmissionForTest(t, &r)
	if _, err := r.finishObservedBuild(context.Background(), build, runnerexec.Client{Kube: kubeClient}, runner.BuildState{Status: "failed", Error: "buildkit failed"}); err != nil {
		t.Fatal(err)
	}
	stored := storedLifecycleBuild(t, crClient, build.Name)
	if stored.Status.Phase != kovav1.PhaseFailedVerifying || stored.Status.Reason != "BuildFailed" ||
		stored.Status.VerificationDeadlineAt == nil || time.Until(stored.Status.VerificationDeadlineAt.Time) > 5*time.Minute || len(kubeClient.deleted) != 0 {
		t.Fatalf("failure intent was not durably bounded: %#v", stored.Status)
	}
	if _, err := r.Reconcile(context.Background(), failedVerificationRequest(build)); err != nil {
		t.Fatal(err)
	}
	stored = storedLifecycleBuild(t, crClient, build.Name)
	if stored.Status.Phase != kovav1.PhaseFailed || stored.Status.Reason != "BuildFailed" || stored.Status.Message != "buildkit failed" ||
		stored.Status.FinishedAt == nil || len(stored.Status.Outputs) != 0 || exports != 1 || len(kubeClient.deleted) != 0 {
		t.Fatalf("terminal failure status=%#v exports=%d deleted=%v", stored.Status, exports, kubeClient.deleted)
	}
	if _, err := r.Reconcile(context.Background(), failedVerificationRequest(build)); err != nil {
		t.Fatal(err)
	}
	if len(kubeClient.deleted) != 1 {
		t.Fatalf("terminal cleanup deleted=%v", kubeClient.deleted)
	}
}

func TestFailedRunnerPartialReceiptRetriesRegistryAcrossLeaderAndNeverSucceeds(t *testing.T) {
	host, ref, digest, mode := faultableVerifiedImage(t)
	build := lifecycleBuild("partial-retry", kovav1.PhaseRunning)
	missing := host + "/demo:missing"
	build.Spec.Targets = buildTargets(ref, missing)
	crClient := crfake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build, testRunnerPod(build)).Build()
	exports := 0
	kubeClient := &fakeKube{podClient: crClient, execFn: func(opts kube.ExecOptions) error {
		if !strings.Contains(strings.Join(opts.Command, " "), "/api/v1/export") {
			t.Fatal("failed runner must not be submitted or polled again")
		}
		exports++
		_, _ = fmt.Fprintf(opts.Stdout, "{\"target\":%q,\"success\":true,\"manifest_digest\":%q}\n{\"target\":%q,\"success\":false,\"reason\":\"build failed\"}\n", ref, digest, missing)
		return nil
	}}
	cfg := config.Config{RegistryPlainHTTP: []string{host}, VerificationAttemptTimeout: 150 * time.Millisecond, VerificationWindow: time.Minute}
	r := KovaBuildReconciler{Client: crClient, Scheme: testScheme(t), Kube: kubeClient, Cfg: cfg}
	initializeAdmissionForTest(t, &r)
	if _, err := r.finishObservedBuild(context.Background(), build, runnerexec.Client{Kube: kubeClient}, runner.BuildState{Status: "failed", Error: "one target failed"}); err != nil {
		t.Fatal(err)
	}
	mode.Store(registryUnavailable)
	if _, err := r.Reconcile(context.Background(), failedVerificationRequest(build)); err != nil {
		t.Fatal(err)
	}
	stored := storedLifecycleBuild(t, crClient, build.Name)
	if stored.Status.Phase != kovav1.PhaseFailedVerifying || len(stored.Status.VerificationResults) != 2 ||
		stored.Status.VerificationResults[0].PushedDigest != digest || stored.Status.VerificationResults[0].State != "pending" ||
		stored.Status.VerificationResults[1].State != "failed" || len(stored.Status.Outputs) != 0 || len(kubeClient.deleted) != 0 || exports != 1 {
		t.Fatalf("partial receipt was not retained for retry: %#v", stored.Status)
	}
	mode.Store(registryHealthy)
	past := metav1.NewTime(time.Now().Add(-time.Second))
	stored.Status.VerificationNextAttemptAt = &past
	if err := crClient.Status().Update(context.Background(), stored); err != nil {
		t.Fatal(err)
	}
	newLeader := KovaBuildReconciler{Client: crClient, Scheme: testScheme(t), Kube: kubeClient, Cfg: cfg}
	if _, err := newLeader.Reconcile(context.Background(), failedVerificationRequest(build)); err != nil {
		t.Fatal(err)
	}
	stored = storedLifecycleBuild(t, crClient, build.Name)
	if stored.Status.Phase != kovav1.PhaseFailed || stored.Status.Reason != "BuildFailed" || len(stored.Status.Outputs) != 1 ||
		stored.Status.Outputs[0].ManifestDigest != digest || exports != 1 || len(kubeClient.deleted) != 0 {
		t.Fatalf("recovered partial failure status=%#v exports=%d deleted=%v", stored.Status, exports, kubeClient.deleted)
	}
	if _, err := newLeader.Reconcile(context.Background(), failedVerificationRequest(build)); err != nil {
		t.Fatal(err)
	}
	if len(kubeClient.deleted) != 1 {
		t.Fatalf("terminal cleanup deleted=%v", kubeClient.deleted)
	}
}

func TestFailedRunnerRetriesTransientExportButNeverReportsSuccess(t *testing.T) {
	host, ref, digest := testVerifiedImage(t, "amd64")
	build := lifecycleBuild("failed-export-retry", kovav1.PhaseRunning)
	build.Spec.Targets = buildTargets(ref)
	crClient := crfake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build, testRunnerPod(build)).Build()
	exports := 0
	kubeClient := &fakeKube{podClient: crClient, execFn: func(opts kube.ExecOptions) error {
		if !strings.Contains(strings.Join(opts.Command, " "), "/api/v1/export") {
			t.Fatal("failed runner must not be submitted or polled again")
		}
		exports++
		if exports == 1 {
			return errors.New("temporary exec disconnect")
		}
		_, _ = fmt.Fprintf(opts.Stdout, "{\"target\":%q,\"success\":true,\"manifest_digest\":%q}\n", ref, digest)
		return nil
	}}
	cfg := config.Config{RegistryPlainHTTP: []string{host}, VerificationWindow: time.Minute}
	r := KovaBuildReconciler{Client: crClient, Scheme: testScheme(t), Kube: kubeClient, Cfg: cfg}
	initializeAdmissionForTest(t, &r)
	if _, err := r.finishObservedBuild(context.Background(), build, runnerexec.Client{Kube: kubeClient}, runner.BuildState{Status: "failed", Error: "later step failed"}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), failedVerificationRequest(build)); err != nil {
		t.Fatal(err)
	}
	stored := storedLifecycleBuild(t, crClient, build.Name)
	if stored.Status.Phase != kovav1.PhaseFailedVerifying || stored.Status.VerificationResults[0].State != "pending" ||
		stored.Status.VerificationLastError != "runner export unavailable" || len(kubeClient.deleted) != 0 {
		t.Fatalf("transient export status=%#v", stored.Status)
	}
	past := metav1.NewTime(time.Now().Add(-time.Second))
	stored.Status.VerificationNextAttemptAt = &past
	if err := crClient.Status().Update(context.Background(), stored); err != nil {
		t.Fatal(err)
	}
	newLeader := KovaBuildReconciler{Client: crClient, Scheme: testScheme(t), Kube: kubeClient, Cfg: cfg}
	if _, err := newLeader.Reconcile(context.Background(), failedVerificationRequest(build)); err != nil {
		t.Fatal(err)
	}
	stored = storedLifecycleBuild(t, crClient, build.Name)
	if stored.Status.Phase != kovav1.PhaseFailed || len(stored.Status.Outputs) != 1 ||
		stored.Status.Outputs[0].ManifestDigest != digest || exports != 2 {
		t.Fatalf("recovered export status=%#v exports=%d", stored.Status, exports)
	}
}

func TestFailedVerificationDeadlineTerminatesAndReleasesActiveSlot(t *testing.T) {
	build := lifecycleBuild("failed-deadline", kovav1.PhaseFailedVerifying)
	build.Status.Reason, build.Status.Message = "BuildFailed", "runner failed"
	build.Annotations = map[string]string{kovav1.CancellationRequestedAnnotation: "2026-01-01T00:00:00Z"}
	started := metav1.NewTime(time.Now().Add(-6 * time.Minute))
	deadline := metav1.NewTime(time.Now().Add(-time.Second))
	build.Status.VerificationStartedAt, build.Status.VerificationDeadlineAt = &started, &deadline
	build.Status.VerificationResults = []kovav1.BuildVerificationResult{{Format: "oci", Image: build.Spec.Targets[0].Target, Platform: "linux/amd64", State: "pending"}}
	crClient := crfake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build, testRunnerPod(build)).Build()
	kubeClient := &fakeKube{podClient: crClient, execFn: func(kube.ExecOptions) error {
		t.Fatal("expired failed verification must not call runner")
		return nil
	}}
	r := KovaBuildReconciler{Client: crClient, Scheme: testScheme(t), Kube: kubeClient, Cfg: config.Config{MaxActiveJobs: 1, WorkerSlots: 1}}
	initializeAdmissionForTest(t, &r)
	if _, err := r.Reconcile(context.Background(), failedVerificationRequest(build)); err != nil {
		t.Fatal(err)
	}
	stored := storedLifecycleBuild(t, crClient, build.Name)
	if stored.Status.Phase != kovav1.PhaseFailed || stored.Status.Reason != "BuildFailed" ||
		stored.Status.VerificationResults[0].State != "failed" || !strings.Contains(stored.Status.VerificationLastError, "deadline") ||
		len(kubeClient.deleted) != 0 {
		t.Fatalf("expired failed status=%#v deleted=%v", stored.Status, kubeClient.deleted)
	}
	if _, err := r.Reconcile(context.Background(), failedVerificationRequest(build)); err != nil {
		t.Fatal(err)
	}
	_, ledger, err := r.readReservations(context.Background(), build.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	if len(kubeClient.deleted) != 1 || len(ledger.Active) != 0 {
		t.Fatalf("failed verification retained active capacity: deleted=%v ledger=%#v", kubeClient.deleted, ledger.Active)
	}
}

type failFailedStatusClient struct {
	client.Client
	failures int
}

func (c *failFailedStatusClient) Status() client.SubResourceWriter {
	return &failFailedStatusWriter{SubResourceWriter: c.Client.Status(), parent: c}
}

type failFailedStatusWriter struct {
	client.SubResourceWriter
	parent *failFailedStatusClient
}

func (w *failFailedStatusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if build, ok := obj.(*kovav1.KovaBuild); ok && build.Status.Phase == kovav1.PhaseFailed && w.parent.failures > 0 {
		w.parent.failures--
		return errors.New("injected failed terminal write rejection")
	}
	return w.SubResourceWriter.Update(ctx, obj, opts...)
}

func TestFailedRunnerTerminalWriteFaultKeepsReceiptAndRetriesPastDeadline(t *testing.T) {
	host, ref, digest := testVerifiedImage(t, "amd64")
	build := lifecycleBuild("failed-write", kovav1.PhaseFailedVerifying)
	build.Spec.Targets = buildTargets(ref)
	build.Status.Reason, build.Status.Message = "BuildFailed", "runner failed after push"
	now := metav1.Now()
	deadline := metav1.NewTime(now.Add(time.Minute))
	build.Status.VerificationStartedAt, build.Status.VerificationDeadlineAt = &now, &deadline
	build.Status.VerificationResults = []kovav1.BuildVerificationResult{{Format: "oci", Image: ref, Platform: "linux/amd64", PushedDigest: digest, State: "pending"}}
	crClient := crfake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build, testRunnerPod(build)).Build()
	statusClient := &failFailedStatusClient{Client: crClient, failures: 1}
	kubeClient := &fakeKube{podClient: crClient, execFn: func(kube.ExecOptions) error {
		t.Fatal("persisted pushed digest must not be re-exported")
		return nil
	}}
	cfg := config.Config{RegistryPlainHTTP: []string{host}, VerificationAttemptTimeout: time.Second}
	r := KovaBuildReconciler{Client: statusClient, Scheme: testScheme(t), Kube: kubeClient, Cfg: cfg}
	initializeAdmissionForTest(t, &r)
	if _, err := r.Reconcile(context.Background(), failedVerificationRequest(build)); err == nil || !strings.Contains(err.Error(), "injected failed terminal write rejection") {
		t.Fatalf("terminal write fault = %v", err)
	}
	stored := storedLifecycleBuild(t, crClient, build.Name)
	if stored.Status.Phase != kovav1.PhaseFailedVerifying || stored.Status.VerificationResults[0].State != "succeeded" ||
		len(stored.Status.Outputs) != 1 || stored.Status.Outputs[0].ManifestDigest != digest || len(kubeClient.deleted) != 0 {
		t.Fatalf("receipt was lost before terminal status: %#v", stored.Status)
	}
	deadline = metav1.NewTime(time.Now().Add(-time.Second))
	stored.Status.VerificationDeadlineAt = &deadline
	if err := crClient.Status().Update(context.Background(), stored); err != nil {
		t.Fatal(err)
	}
	newLeader := KovaBuildReconciler{Client: crClient, Scheme: testScheme(t), Kube: kubeClient, Cfg: cfg}
	if _, err := newLeader.Reconcile(context.Background(), failedVerificationRequest(build)); err != nil {
		t.Fatal(err)
	}
	stored = storedLifecycleBuild(t, crClient, build.Name)
	if stored.Status.Phase != kovav1.PhaseFailed || len(stored.Status.Outputs) != 1 || stored.Status.Outputs[0].ManifestDigest != digest || len(kubeClient.deleted) != 0 {
		t.Fatalf("restarted terminal status=%#v", stored.Status)
	}
}

func TestUnknownFutureFailurePhaseFailsClosedWithoutRunnerCleanup(t *testing.T) {
	build := lifecycleBuild("future-failure-phase", "FutureFailureVerification")
	crClient := crfake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build, testRunnerPod(build)).Build()
	kubeClient := &fakeKube{podClient: crClient, execFn: func(kube.ExecOptions) error { t.Fatal("unknown phase must not touch runner"); return nil }}
	r := KovaBuildReconciler{Client: crClient, Scheme: testScheme(t), Kube: kubeClient, Cfg: config.Config{PollInterval: time.Second}}
	result, err := r.Reconcile(context.Background(), failedVerificationRequest(build))
	if err != nil || result.RequeueAfter <= 0 || len(kubeClient.deleted) != 0 || storedLifecycleBuild(t, crClient, build.Name).Status.Phase != build.Status.Phase {
		t.Fatalf("unknown phase was not fail-closed: result=%#v err=%v", result, err)
	}
}
