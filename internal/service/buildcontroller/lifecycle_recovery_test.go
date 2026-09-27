package buildcontroller

import (
	"context"
	"errors"
	"fmt"
	"io"
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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type failRunningStatusClient struct {
	client.Client
	failures int
}

func (c *failRunningStatusClient) Status() client.SubResourceWriter {
	return &failRunningStatusWriter{SubResourceWriter: c.Client.Status(), parent: c}
}

type failRunningStatusWriter struct {
	client.SubResourceWriter
	parent *failRunningStatusClient
}

func (w *failRunningStatusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if build, ok := obj.(*kovav1.KovaBuild); ok && build.Status.Phase == kovav1.PhaseRunning && w.parent.failures > 0 {
		w.parent.failures--
		return errors.New("injected status update failure")
	}
	return w.SubResourceWriter.Update(ctx, obj, opts...)
}

func readyLifecyclePod(name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "jobs"},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{
			Type: corev1.PodReady, Status: corev1.ConditionTrue,
		}}},
	}
}

func lifecycleBuild(name, phase string) *kovav1.KovaBuild {
	return &kovav1.KovaBuild{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "jobs", UID: types.UID("uid-" + name), Finalizers: []string{cleanupFinalizer}},
		Spec:       kovav1.KovaBuildSpec{Targets: buildTargets("registry.example/demo:dev"), Build: kovav1.KovaBuildOptions{Format: "oci"}},
		Status:     kovav1.KovaBuildStatus{Phase: phase, RunnerPodName: buildPodName(name)},
	}
}

func storedLifecycleBuild(t *testing.T, c client.Client, name string) *kovav1.KovaBuild {
	t.Helper()
	var build kovav1.KovaBuild
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "jobs", Name: name}, &build); err != nil {
		t.Fatal(err)
	}
	return &build
}

func TestSubmitRejectsLegacyIdleRunnerBeforePost(t *testing.T) {
	scheme := testScheme(t)
	build := lifecycleBuild("legacy-idle", kovav1.PhaseStarting)
	crClient := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build, readyLifecyclePod(build.Status.RunnerPodName)).Build()
	inspects, submits := 0, 0
	kubeClient := &fakeKube{execFn: func(opts kube.ExecOptions) error {
		command := strings.Join(opts.Command, " ")
		switch {
		case strings.Contains(command, "--method GET"):
			_, _ = io.WriteString(opts.Stdout, `{"status":"idle"}`)
		case strings.Contains(command, "source inspect"):
			inspects++
		case strings.Contains(command, "--method POST"):
			submits++
		}
		return nil
	}}
	r := KovaBuildReconciler{Client: crClient, Scheme: scheme, Kube: kubeClient, Cfg: config.Config{PollInterval: time.Second}}
	if _, err := r.submitWhenReady(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	stored := storedLifecycleBuild(t, crClient, "legacy-idle")
	if stored.Status.Phase != kovav1.PhaseFailed || stored.Status.Reason != "RunnerProtocolIncompatible" || !strings.Contains(stored.Status.Message, "idempotent") {
		t.Fatalf("status=%#v", stored.Status)
	}
	if inspects != 0 || submits != 0 {
		t.Fatalf("legacy runner was touched after capability rejection: inspects=%d submits=%d", inspects, submits)
	}
}

func TestSubmitObservesAlreadyRunningLegacyRunner(t *testing.T) {
	scheme := testScheme(t)
	build := lifecycleBuild("legacy-running", kovav1.PhaseStarting)
	crClient := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build, readyLifecyclePod(build.Status.RunnerPodName)).Build()
	status := "running"
	buildPosts := 0
	kubeClient := &fakeKube{execFn: func(opts kube.ExecOptions) error {
		command := strings.Join(opts.Command, " ")
		switch {
		case strings.Contains(command, "--method GET"):
			_, _ = fmt.Fprintf(opts.Stdout, `{"status":%q,"error":"legacy build failed"}`, status)
		case strings.Contains(command, "--method POST") && strings.Contains(command, "--path /api/v1/build "):
			buildPosts++
		case strings.Contains(command, "/api/v1/export"):
			return errors.New("legacy result unavailable")
		}
		return nil
	}}
	r := KovaBuildReconciler{Client: crClient, Scheme: scheme, Kube: kubeClient, Cfg: config.Config{PollInterval: time.Second}}
	if _, err := r.submitWhenReady(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	if got := storedLifecycleBuild(t, crClient, "legacy-running").Status.Phase; got != kovav1.PhaseRunning {
		t.Fatalf("phase after observing legacy running = %s", got)
	}
	status = "failed"
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "legacy-running"}}); err != nil {
		t.Fatal(err)
	}
	stored := storedLifecycleBuild(t, crClient, "legacy-running")
	if stored.Status.Phase != kovav1.PhaseFailed || stored.Status.Reason != "BuildFailed" || stored.Status.Message != "legacy build failed" || buildPosts != 0 {
		t.Fatalf("status=%#v buildPosts=%d", stored.Status, buildPosts)
	}
}

func TestSubmitRecoversAfterAcceptedPostAndStatusWriteFailure(t *testing.T) {
	scheme := testScheme(t)
	build := lifecycleBuild("ambiguous", kovav1.PhaseStarting)
	pod := readyLifecyclePod(build.Status.RunnerPodName)
	base := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build, pod).Build()
	crClient := &failRunningStatusClient{Client: base, failures: 1}
	state := "idle"
	submits := 0
	kubeClient := &fakeKube{execFn: func(opts kube.ExecOptions) error {
		command := strings.Join(opts.Command, " ")
		switch {
		case strings.Contains(command, "source inspect"):
			_, _ = io.WriteString(opts.Stdout, `{"targets":[{"target":"registry.example/demo:dev","platform":"linux/amd64"}]}`)
		case strings.Contains(command, "--method GET"):
			if state == "idle" {
				_, _ = io.WriteString(opts.Stdout, `{"status":"idle","capabilities":["idempotent-build-request-v1"]}`)
			} else {
				_, _ = io.WriteString(opts.Stdout, `{"status":"running","requestId":"uid-ambiguous"}`)
			}
		case strings.Contains(command, "--method POST"):
			submits++
			state = "running"
			_, _ = io.WriteString(opts.Stdout, `{"status":"running","requestId":"uid-ambiguous"}`)
		}
		return nil
	}}
	r := KovaBuildReconciler{Client: crClient, Scheme: scheme, Kube: kubeClient, Cfg: config.Config{PollInterval: time.Second}}
	if _, err := r.submitWhenReady(context.Background(), build); err == nil {
		t.Fatal("expected injected status update failure")
	}
	if got := storedLifecycleBuild(t, crClient, "ambiguous").Status.Phase; got != kovav1.PhaseStarting {
		t.Fatalf("stored phase after failed update = %s", got)
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "ambiguous"}}); err != nil {
		t.Fatal(err)
	}
	if got := storedLifecycleBuild(t, crClient, "ambiguous").Status.Phase; got != kovav1.PhaseRunning {
		t.Fatalf("stored phase after recovery = %s", got)
	}
	if submits != 1 {
		t.Fatalf("submit calls = %d, want 1", submits)
	}
}

func TestSubmitTreatsLostPostResponseAsUncertainUntilRunnerIsObserved(t *testing.T) {
	scheme := testScheme(t)
	build := lifecycleBuild("lost-response", kovav1.PhaseStarting)
	crClient := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build, readyLifecyclePod(build.Status.RunnerPodName)).Build()
	accepted := false
	submits := 0
	kubeClient := &fakeKube{execFn: func(opts kube.ExecOptions) error {
		command := strings.Join(opts.Command, " ")
		switch {
		case strings.Contains(command, "source inspect"):
			_, _ = io.WriteString(opts.Stdout, `{"targets":[{"target":"registry.example/demo:dev","platform":"linux/amd64"}]}`)
		case strings.Contains(command, "--method GET"):
			if accepted {
				_, _ = io.WriteString(opts.Stdout, `{"status":"running","requestId":"uid-lost-response"}`)
			} else {
				_, _ = io.WriteString(opts.Stdout, `{"status":"idle","capabilities":["idempotent-build-request-v1"]}`)
			}
		case strings.Contains(command, "--method POST"):
			submits++
			accepted = true
			return errors.New("exec response lost after accept")
		}
		return nil
	}}
	r := KovaBuildReconciler{Client: crClient, Scheme: scheme, Kube: kubeClient, Cfg: config.Config{PollInterval: time.Second}}
	if _, err := r.submitWhenReady(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	stored := storedLifecycleBuild(t, crClient, "lost-response")
	if stored.Status.Phase != kovav1.PhaseRunning || submits != 1 || len(kubeClient.deleted) != 0 {
		t.Fatalf("status=%#v submits=%d deleted=%#v", stored.Status, submits, kubeClient.deleted)
	}
}

func TestSubmitRetriesSourceInspectTransportFailure(t *testing.T) {
	scheme := testScheme(t)
	build := lifecycleBuild("inspect-retry", kovav1.PhaseStarting)
	started := metav1.Now()
	build.Status.StartedAt = &started
	crClient := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build, readyLifecyclePod(build.Status.RunnerPodName)).Build()
	inspects := 0
	submits := 0
	kubeClient := &fakeKube{execFn: func(opts kube.ExecOptions) error {
		command := strings.Join(opts.Command, " ")
		switch {
		case strings.Contains(command, "--method GET"):
			_, _ = io.WriteString(opts.Stdout, `{"status":"idle","capabilities":["idempotent-build-request-v1"]}`)
		case strings.Contains(command, "source inspect"):
			inspects++
			if inspects == 1 {
				return errors.New("temporary SPDY disconnect")
			}
			_, _ = io.WriteString(opts.Stdout, `{"targets":[{"target":"registry.example/demo:dev","platform":"linux/amd64"}]}`)
		case strings.Contains(command, "--method POST"):
			submits++
			_, _ = io.WriteString(opts.Stdout, `{"status":"running","requestId":"uid-inspect-retry"}`)
		}
		return nil
	}}
	r := KovaBuildReconciler{Client: crClient, Scheme: scheme, Kube: kubeClient, Cfg: config.Config{PollRetryWindow: time.Minute, MaxBuildDuration: time.Hour}}
	first, err := r.submitWhenReady(context.Background(), build)
	if err != nil || first.RequeueAfter <= 0 {
		t.Fatalf("first attempt result=%#v err=%v", first, err)
	}
	stored := storedLifecycleBuild(t, crClient, "inspect-retry")
	if stored.Status.Phase != kovav1.PhaseStarting || stored.Status.PollFailureSince == nil || submits != 0 {
		t.Fatalf("first attempt status=%#v submits=%d", stored.Status, submits)
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "inspect-retry"}}); err != nil {
		t.Fatal(err)
	}
	stored = storedLifecycleBuild(t, crClient, "inspect-retry")
	if stored.Status.Phase != kovav1.PhaseRunning || stored.Status.PollFailureSince != nil || inspects != 2 || submits != 1 {
		t.Fatalf("recovered status=%#v inspects=%d submits=%d", stored.Status, inspects, submits)
	}
}

func TestPollRetriesTransientExecFailureWhilePodLives(t *testing.T) {
	scheme := testScheme(t)
	build := lifecycleBuild("transient", kovav1.PhaseRunning)
	pod := readyLifecyclePod(build.Status.RunnerPodName)
	crClient := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build, pod).Build()
	calls := 0
	kubeClient := &fakeKube{execFn: func(opts kube.ExecOptions) error {
		calls++
		if calls == 1 {
			return errors.New("temporary SPDY disconnect")
		}
		_, _ = io.WriteString(opts.Stdout, `{"status":"running","requestId":"uid-transient"}`)
		return nil
	}}
	r := KovaBuildReconciler{Client: crClient, Scheme: scheme, Kube: kubeClient, Cfg: config.Config{PollInterval: time.Second, PollRetryWindow: time.Minute}}
	first, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "transient"}})
	if err != nil || first.RequeueAfter <= 0 {
		t.Fatalf("first poll result=%#v err=%v", first, err)
	}
	stored := storedLifecycleBuild(t, crClient, "transient")
	if stored.Status.Phase != kovav1.PhaseRunning || stored.Status.PollFailureSince == nil || stored.Status.PollFailureCount != 1 || len(kubeClient.deleted) != 0 {
		t.Fatalf("first poll status=%#v deleted=%#v", stored.Status, kubeClient.deleted)
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "transient"}}); err != nil {
		t.Fatal(err)
	}
	stored = storedLifecycleBuild(t, crClient, "transient")
	if stored.Status.Phase != kovav1.PhaseRunning || stored.Status.PollFailureSince != nil || stored.Status.PollFailureCount != 0 {
		t.Fatalf("status after recovery=%#v", stored.Status)
	}
}

func TestPollChecksLiveAPIWhenCachedPodIsMissing(t *testing.T) {
	scheme := testScheme(t)
	build := lifecycleBuild("stale-cache", kovav1.PhaseRunning)
	cache := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build).Build()
	apiReader := crfake.NewClientBuilder().WithScheme(scheme).WithObjects(readyLifecyclePod(build.Status.RunnerPodName)).Build()
	kubeClient := &fakeKube{execFn: func(kube.ExecOptions) error { return errors.New("temporary exec failure") }}
	r := KovaBuildReconciler{Client: cache, APIReader: apiReader, Scheme: scheme, Kube: kubeClient, Cfg: config.Config{PollRetryWindow: time.Minute}}
	result, err := r.pollBuild(context.Background(), build)
	if err != nil || result.RequeueAfter <= 0 {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	stored := storedLifecycleBuild(t, cache, "stale-cache")
	if stored.Status.Phase != kovav1.PhaseRunning || stored.Status.PollFailureSince == nil {
		t.Fatalf("status=%#v", stored.Status)
	}
}

func TestPollFailsWhenRunnerPodIsLost(t *testing.T) {
	scheme := testScheme(t)
	build := lifecycleBuild("lost", kovav1.PhaseRunning)
	crClient := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build).Build()
	kubeClient := &fakeKube{execFn: func(kube.ExecOptions) error { return errors.New("exec unavailable") }}
	r := KovaBuildReconciler{Client: crClient, Scheme: scheme, Kube: kubeClient, Cfg: config.Config{PollRetryWindow: time.Minute}}
	if _, err := r.pollBuild(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	stored := storedLifecycleBuild(t, crClient, "lost")
	if stored.Status.Phase != kovav1.PhaseFailed || stored.Status.Reason != "RunnerUnavailable" {
		t.Fatalf("status=%#v", stored.Status)
	}
}

func TestPollFailsWhenRunnerPodTerminates(t *testing.T) {
	scheme := testScheme(t)
	build := lifecycleBuild("terminated", kovav1.PhaseRunning)
	pod := readyLifecyclePod(build.Status.RunnerPodName)
	pod.Status.Phase = corev1.PodFailed
	crClient := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build, pod).Build()
	kubeClient := &fakeKube{execFn: func(kube.ExecOptions) error { return errors.New("exec unavailable") }}
	r := KovaBuildReconciler{Client: crClient, Scheme: scheme, Kube: kubeClient, Cfg: config.Config{PollRetryWindow: time.Minute}}
	if _, err := r.pollBuild(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	stored := storedLifecycleBuild(t, crClient, "terminated")
	if stored.Status.Phase != kovav1.PhaseFailed || stored.Status.Reason != "RunnerUnavailable" {
		t.Fatalf("status=%#v", stored.Status)
	}
}

func TestPollFailureWindowStopsRunner(t *testing.T) {
	scheme := testScheme(t)
	build := lifecycleBuild("unobservable", kovav1.PhaseRunning)
	since := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	build.Status.PollFailureSince = &since
	build.Status.PollFailureCount = 3
	crClient := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build, readyLifecyclePod(build.Status.RunnerPodName)).Build()
	kubeClient := &fakeKube{execFn: func(kube.ExecOptions) error { return errors.New("exec unavailable") }}
	r := KovaBuildReconciler{Client: crClient, Scheme: scheme, Kube: kubeClient, Cfg: config.Config{PollRetryWindow: time.Minute}}
	if _, err := r.pollBuild(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	stored := storedLifecycleBuild(t, crClient, "unobservable")
	if stored.Status.Phase != kovav1.PhaseFailed || stored.Status.Reason != "RunnerUnavailable" || len(kubeClient.deleted) != 1 {
		t.Fatalf("status=%#v deleted=%#v", stored.Status, kubeClient.deleted)
	}
}

func TestPollProtocolErrorFailsImmediately(t *testing.T) {
	scheme := testScheme(t)
	build := lifecycleBuild("protocol", kovav1.PhaseRunning)
	crClient := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build).Build()
	kubeClient := &fakeKube{execFn: func(opts kube.ExecOptions) error {
		_, _ = io.WriteString(opts.Stdout, "not-json")
		return nil
	}}
	r := KovaBuildReconciler{Client: crClient, Scheme: scheme, Kube: kubeClient, Cfg: config.Config{PollRetryWindow: time.Minute}}
	if _, err := r.pollBuild(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	stored := storedLifecycleBuild(t, crClient, "protocol")
	if stored.Status.Phase != kovav1.PhaseFailed || stored.Status.Reason != "RunnerProtocolError" {
		t.Fatalf("status=%#v", stored.Status)
	}
}

func TestPollTreatsReportedBuildFailureAsTerminal(t *testing.T) {
	scheme := testScheme(t)
	build := lifecycleBuild("build-failed", kovav1.PhaseRunning)
	crClient := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build).Build()
	kubeClient := &fakeKube{execFn: func(opts kube.ExecOptions) error {
		if strings.Contains(strings.Join(opts.Command, " "), "--method GET") {
			_, _ = io.WriteString(opts.Stdout, `{"status":"failed","error":"buildkit rejected build","requestId":"uid-build-failed"}`)
			return nil
		}
		return errors.New("result export unavailable")
	}}
	r := KovaBuildReconciler{Client: crClient, Scheme: scheme, Kube: kubeClient, Cfg: config.Config{PollRetryWindow: time.Minute}}
	result, err := r.pollBuild(context.Background(), build)
	if err != nil || result.RequeueAfter != 0 {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	stored := storedLifecycleBuild(t, crClient, "build-failed")
	if stored.Status.Phase != kovav1.PhaseFailed || stored.Status.Reason != "BuildFailed" || stored.Status.Message != "buildkit rejected build" {
		t.Fatalf("status=%#v", stored.Status)
	}
}

func TestBuildDurationExpiryDeletesPodAndReleasesSlot(t *testing.T) {
	scheme := testScheme(t)
	build := lifecycleBuild("expired", kovav1.PhaseRunning)
	started := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	build.Status.StartedAt = &started
	queued := lifecycleBuild("next", kovav1.PhaseQueued)
	queued.Status.RunnerPodName = ""
	crClient := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build, queued).Build()
	kubeClient := &fakeKube{}
	r := KovaBuildReconciler{Client: crClient, Scheme: scheme, Kube: kubeClient, Cfg: config.Config{MaxBuildDuration: time.Minute, MaxActiveJobs: 1, WorkerSlots: 1}}
	initializeAdmissionForTest(t, &r)
	if _, err := r.pollBuild(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	stored := storedLifecycleBuild(t, crClient, "expired")
	if stored.Status.Phase != kovav1.PhaseFailed || stored.Status.Reason != "BuildTimedOut" || len(kubeClient.deleted) != 1 {
		t.Fatalf("status=%#v deleted=%#v", stored.Status, kubeClient.deleted)
	}
	// A terminal reconcile confirms cleanup before releasing the active grant.
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "expired"}}); err != nil {
		t.Fatal(err)
	}
	decision, err := r.admission(context.Background(), queued)
	if err != nil || !decision.Admitted {
		t.Fatalf("next job admission=%#v err=%v", decision, err)
	}
}

func TestBuildDurationExpiryKeepsSlotUntilPodCleanupSucceeds(t *testing.T) {
	scheme := testScheme(t)
	build := lifecycleBuild("cleanup-failed", kovav1.PhaseRunning)
	started := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	build.Status.StartedAt = &started
	crClient := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build).Build()
	kubeClient := &fakeKube{deleteErr: errors.New("delete temporarily unavailable")}
	r := KovaBuildReconciler{Client: crClient, Scheme: scheme, Kube: kubeClient, Cfg: config.Config{MaxBuildDuration: time.Minute}}
	initializeAdmissionForTest(t, &r)
	if _, err := r.pollBuild(context.Background(), build); err == nil {
		t.Fatal("expected cleanup failure to retry reconciliation")
	}
	if got := storedLifecycleBuild(t, crClient, "cleanup-failed").Status.Phase; got != kovav1.PhaseRunning {
		t.Fatalf("phase before cleanup = %s", got)
	}
}

func TestDelayedDeadlineReconcilePreservesCompletedBuild(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	registryServer := httptest.NewServer(registry.New())
	defer registryServer.Close()
	host := strings.TrimPrefix(registryServer.URL, "http://")
	ref, err := name.NewTag(host+"/demo:dev", name.Insecure)
	if err != nil {
		t.Fatal(err)
	}
	imageConfig, err := empty.Image.ConfigFile()
	if err != nil {
		t.Fatal(err)
	}
	imageConfig.OS = "linux"
	imageConfig.Architecture = "amd64"
	image, err := mutate.ConfigFile(empty.Image, imageConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(ref, image); err != nil {
		t.Fatal(err)
	}
	wantDigest, err := image.Digest()
	if err != nil {
		t.Fatal(err)
	}

	for _, phase := range []string{kovav1.PhaseStarting, kovav1.PhaseRunning} {
		t.Run(phase, func(t *testing.T) {
			scheme := testScheme(t)
			build := lifecycleBuild("late-complete", phase)
			build.Spec.Targets = buildTargets(ref.Name())
			started := metav1.NewTime(time.Now().Add(-2 * time.Minute))
			build.Status.StartedAt = &started
			crClient := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build, testRunnerPod(build)).Build()
			kubeClient := &fakeKube{podClient: crClient, execFn: func(opts kube.ExecOptions) error {
				command := strings.Join(opts.Command, " ")
				switch {
				case strings.Contains(command, "--method GET"):
					_, _ = fmt.Fprintf(opts.Stdout, `{"status":"completed","requestId":%q}`, string(build.UID))
				case strings.Contains(command, "/api/v1/export"):
					_, _ = fmt.Fprintf(opts.Stdout, "{\"target\":%q,\"manifest_digest\":%q,\"success\":true}\n", ref.Name(), wantDigest.String())
				default:
					t.Fatalf("unexpected runner command: %s", command)
				}
				return nil
			}}
			r := KovaBuildReconciler{Client: crClient, Scheme: scheme, Kube: kubeClient, Cfg: config.Config{MaxBuildDuration: time.Minute, RegistryPlainHTTP: []string{host}}}
			initializeAdmissionForTest(t, &r)
			var reconcileErr error
			if phase == kovav1.PhaseStarting {
				_, reconcileErr = r.submitWhenReady(context.Background(), build)
			} else {
				_, reconcileErr = r.pollBuild(context.Background(), build)
			}
			if reconcileErr != nil {
				t.Fatal(reconcileErr)
			}
			verifying := storedLifecycleBuild(t, crClient, "late-complete")
			if verifying.Status.Phase != kovav1.PhaseVerifying {
				t.Fatalf("phase after runner completion = %s", verifying.Status.Phase)
			}
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "late-complete"}}); err != nil {
				t.Fatal(err)
			}
			stored := storedLifecycleBuild(t, crClient, "late-complete")
			if stored.Status.Phase != kovav1.PhaseSucceeded || stored.Status.Reason != "Completed" || len(stored.Status.Outputs) != 1 || stored.Status.Outputs[0].ManifestDigest != wantDigest.String() || len(kubeClient.deleted) != 0 {
				t.Fatalf("status=%#v deleted=%#v", stored.Status, kubeClient.deleted)
			}
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "late-complete"}}); err != nil {
				t.Fatal(err)
			}
			if len(kubeClient.deleted) != 1 {
				t.Fatalf("terminal cleanup deleted=%#v, want runner Pod", kubeClient.deleted)
			}
		})
	}
}
