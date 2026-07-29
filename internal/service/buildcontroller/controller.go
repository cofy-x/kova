package buildcontroller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/artifactstore"
	"github.com/cofy-x/kova/internal/kube"
	"github.com/cofy-x/kova/internal/observability"
	"github.com/cofy-x/kova/internal/runner"
	"github.com/cofy-x/kova/internal/service/buildresult"
	"github.com/cofy-x/kova/internal/service/config"
	"github.com/cofy-x/kova/internal/service/runnerexec"

	"go.opentelemetry.io/otel/attribute"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	controllerOptions "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const cleanupFinalizer = "kova.cofy.dev/cleanup"

var (
	jobQueueLatency = observability.DurationHistogram("kova.service.job.queue.duration", "Time from job creation to runner admission")
	jobDuration     = observability.DurationHistogram("kova.service.job.duration", "Time from runner admission to terminal state")
	jobCompletions  = observability.Int64Counter("kova.service.job.completions", "Terminal service jobs")
	capacityWaits   = observability.Int64Counter("kova.service.capacity.waits", "Reconciliations delayed by capacity admission")
)

type KovaBuildReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Kube     kube.API
	Store    artifactstore.Store
	Cfg      config.Config
	Recorder record.EventRecorder
}

func (r *KovaBuildReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var build kovav1.KovaBuild
	if err := r.Get(ctx, req.NamespacedName, &build); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !build.ObjectMeta.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &build)
	}
	if controllerutil.AddFinalizer(&build, cleanupFinalizer) {
		if err := r.Update(ctx, &build); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}
	if cancellationRequested(&build) && !isTerminalPhase(build.Status.Phase) {
		return r.cancelBuild(ctx, &build)
	}
	if isTerminalPhase(build.Status.Phase) {
		return r.reconcileTerminal(ctx, &build)
	}
	switch build.Status.Phase {
	case "", kovav1.PhaseQueued:
		return r.startBuild(ctx, &build)
	case kovav1.PhaseStarting:
		return r.submitWhenReady(ctx, &build)
	case kovav1.PhaseRunning:
		return r.pollBuild(ctx, &build)
	default:
		return ctrl.Result{RequeueAfter: r.Cfg.PollInterval}, nil
	}
}

func (r *KovaBuildReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&kovav1.KovaBuild{}).
		Owns(&corev1.Pod{}).
		WithOptions(controllerOptions.Options{MaxConcurrentReconciles: 1}).
		Complete(r)
}

func (r *KovaBuildReconciler) startBuild(ctx context.Context, build *kovav1.KovaBuild) (ctrl.Result, error) {
	if r.Cfg.RunnerImage == "" {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "ConfigurationInvalid", "runner image is required")
	}
	decision, err := r.admission(ctx, build)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !decision.Admitted {
		capacityWaits.Add(ctx, 1)
		build.Status.Phase = kovav1.PhaseQueued
		setPhaseCondition(build, kovav1.PhaseQueued, "WaitingForCapacity", decision.Message)
		build.Status.ObservedGeneration = build.Generation
		if err := r.Status().Update(ctx, build); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: r.Cfg.PollInterval}, nil
	}
	source, err := url.Parse(build.Spec.Source.URI)
	if err != nil {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "SourceInvalid", err.Error())
	}
	if r.Store != nil {
		reader, err := r.Store.Open(ctx, build.Spec.Source.URI)
		if err != nil {
			return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "ArtifactUnavailable", fmt.Sprintf("open source artifact: %v", err))
		}
		if err := reader.Close(); err != nil {
			return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "ArtifactUnavailable", fmt.Sprintf("close source artifact: %v", err))
		}
	}
	if source.Scheme == "file" && r.Cfg.SourcePVCClaim == "" {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "ConfigurationInvalid", "filesystem artifacts require --source-pvc-claim")
	}
	podName := buildPodName(build.Name)
	pod := runner.PreparePod(runner.ManifestOptions{
		PodName:         podName,
		Namespace:       build.Namespace,
		Image:           r.Cfg.RunnerImage,
		ImagePullPolicy: r.Cfg.RunnerImagePullPolicy,
		ImagePullSecret: r.Cfg.RunnerImagePullSecret,
		BuildkitAddr:    r.Cfg.BuildkitAddr,
		NodeSelector:    r.Cfg.RunnerNodeSelector,
		Env:             r.Cfg.RunnerEnv,
		SourcePVCClaim:  filesystemPVC(source, r.Cfg.SourcePVCClaim),
		SourceMountPath: filesystemMount(source, r.Cfg.ArtifactRoot),
		SourceReadOnly:  true,
		SourceURI:       build.Spec.Source.URI,
		SourceDigest:    build.Spec.Source.Digest,
		ArtifactRoot:    r.Cfg.ArtifactRoot,
		ArtifactSecret:  r.Cfg.ArtifactSecret,
		S3Endpoint:      r.Cfg.S3Endpoint,
		S3Bucket:        r.Cfg.S3Bucket,
		S3Region:        r.Cfg.S3Region,
		S3Secure:        r.Cfg.S3Secure,
		Labels: map[string]string{
			"kova.cofy.dev/build-id": build.Name,
		},
	})
	if err := ctrl.SetControllerReference(build, &pod, r.Scheme); err != nil {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "RunnerCreateFailed", err.Error())
	}
	if err := r.Create(ctx, &pod); err != nil && !apierrors.IsAlreadyExists(err) {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "RunnerCreateFailed", err.Error())
	}
	now := metav1.Now()
	jobQueueLatency.RecordDuration(ctx, time.Since(build.CreationTimestamp.Time))
	build.Status.Phase = kovav1.PhaseStarting
	build.Status.ObservedGeneration = build.Generation
	build.Status.AllocatedConcurrency = int32(decision.Allocation)
	build.Status.RunnerPodName = podName
	build.Status.StartedAt = &now
	build.Status.Message = ""
	setPhaseCondition(build, kovav1.PhaseStarting, "RunnerCreated", "runner Pod was created")
	if err := r.Status().Update(ctx, build); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: time.Second}, nil
}

func (r *KovaBuildReconciler) cancelBuild(ctx context.Context, build *kovav1.KovaBuild) (ctrl.Result, error) {
	if build.Status.Phase == kovav1.PhaseRunning && build.Status.RunnerPodName != "" {
		// Cancellation remains effective when the daemon is already unavailable;
		// deleting the runner Pod is the authoritative stop operation.
		_ = (runnerexec.Client{Kube: r.Kube, BuildkitAddr: r.Cfg.BuildkitAddr}).CancelBuild(ctx, build)
	}
	r.persistLogs(ctx, build)
	if build.Status.RunnerPodName != "" {
		if err := r.Kube.DeletePod(ctx, build.Namespace, build.Status.RunnerPodName); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseCancelled, "Cancelled", "build was cancelled")
}

func (r *KovaBuildReconciler) submitWhenReady(ctx context.Context, build *kovav1.KovaBuild) (ctrl.Result, error) {
	var pod corev1.Pod
	if err := r.Get(ctx, types.NamespacedName{Namespace: build.Namespace, Name: build.Status.RunnerPodName}, &pod); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "RunnerUnavailable", "runner Pod disappeared before the build was submitted")
		}
		return ctrl.Result{}, err
	}
	if !podReady(&pod) {
		if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
			return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "RunnerUnavailable", fmt.Sprintf("runner Pod terminated before becoming ready: %s", pod.Status.Phase))
		}
		if build.Status.StartedAt != nil && r.Cfg.WaitTimeout > 0 && time.Since(build.Status.StartedAt.Time) >= r.Cfg.WaitTimeout {
			return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "RunnerUnavailable", fmt.Sprintf("runner Pod did not become ready within %s", r.Cfg.WaitTimeout))
		}
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	if err := (runnerexec.Client{Kube: r.Kube, BuildkitAddr: r.Cfg.BuildkitAddr}).SubmitBuild(ctx, build, sourcePath(build)); err != nil {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "BuildSubmissionFailed", err.Error())
	}
	build.Status.Phase = kovav1.PhaseRunning
	build.Status.ObservedGeneration = build.Generation
	build.Status.Message = ""
	setPhaseCondition(build, kovav1.PhaseRunning, "BuildSubmitted", "build was submitted to the runner")
	if err := r.Status().Update(ctx, build); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: r.Cfg.PollInterval}, nil
}

func (r *KovaBuildReconciler) pollBuild(ctx context.Context, build *kovav1.KovaBuild) (ctrl.Result, error) {
	state, err := (runnerexec.Client{Kube: r.Kube, BuildkitAddr: r.Cfg.BuildkitAddr}).BuildStatus(ctx, build)
	if err != nil {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "RunnerUnavailable", err.Error())
	}
	done, success, err := runner.WaitDecision(state.Status)
	if err != nil {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "RunnerProtocolError", err.Error())
	}
	if !done {
		return ctrl.Result{RequeueAfter: r.Cfg.PollInterval}, nil
	}
	if success {
		build.Status.Phase = kovav1.PhaseSucceeded
		build.Status.Results = buildresult.Resolve(ctx, runnerexec.Client{Kube: r.Kube, BuildkitAddr: r.Cfg.BuildkitAddr}, build, r.Cfg.RegistryPlainHTTP)
		if !buildresult.AllSucceeded(build.Status.Results) {
			return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "ResultVerificationFailed", "one or more build results could not be verified")
		}
		if err := r.persistResults(ctx, build); err != nil {
			return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "ArtifactStoreUnavailable", err.Error())
		}
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseSucceeded, "Completed", "")
	}
	if state.Status == "cancelled" {
		build.Status.Phase = kovav1.PhaseCancelled
		build.Status.Results = buildresult.Resolve(ctx, runnerexec.Client{Kube: r.Kube, BuildkitAddr: r.Cfg.BuildkitAddr}, build, r.Cfg.RegistryPlainHTTP)
		if err := r.persistResults(ctx, build); err != nil {
			return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "ArtifactStoreUnavailable", err.Error())
		}
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseCancelled, "Cancelled", state.Error)
	}
	build.Status.Phase = kovav1.PhaseFailed
	build.Status.Results = buildresult.Resolve(ctx, runnerexec.Client{Kube: r.Kube, BuildkitAddr: r.Cfg.BuildkitAddr}, build, r.Cfg.RegistryPlainHTTP)
	if err := r.persistResults(ctx, build); err != nil {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "ArtifactStoreUnavailable", err.Error())
	}
	return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "BuildFailed", state.Error)
}

func (r *KovaBuildReconciler) reconcileTerminal(ctx context.Context, build *kovav1.KovaBuild) (ctrl.Result, error) {
	if build.Status.FinishedAt == nil || r.Cfg.JobTTL <= 0 {
		return ctrl.Result{}, nil
	}
	remaining := time.Until(build.Status.FinishedAt.Add(r.Cfg.JobTTL))
	if remaining > 0 {
		return ctrl.Result{RequeueAfter: remaining}, nil
	}
	if err := r.Delete(ctx, build); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *KovaBuildReconciler) reconcileDelete(ctx context.Context, build *kovav1.KovaBuild) (ctrl.Result, error) {
	if build.Status.RunnerPodName != "" {
		if err := r.Kube.DeletePod(ctx, build.Namespace, build.Status.RunnerPodName); err != nil {
			return ctrl.Result{}, err
		}
	}
	if r.Store != nil {
		if build.Status.ResultArtifactURI != "" {
			if err := r.Store.Delete(ctx, build.Status.ResultArtifactURI); err != nil {
				return ctrl.Result{}, err
			}
		}
		if build.Status.LogArtifactURI != "" {
			if err := r.Store.Delete(ctx, build.Status.LogArtifactURI); err != nil {
				return ctrl.Result{}, err
			}
		}
		if err := r.Store.Delete(ctx, build.Spec.Source.URI); err != nil {
			return ctrl.Result{}, err
		}
	}
	if controllerutil.RemoveFinalizer(build, cleanupFinalizer) {
		if err := r.Update(ctx, build); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

func (r *KovaBuildReconciler) finish(ctx context.Context, build *kovav1.KovaBuild, phase string, reason string, message string) error {
	r.persistLogs(ctx, build)
	now := metav1.Now()
	build.Status.Phase = phase
	build.Status.ObservedGeneration = build.Generation
	build.Status.Reason = reason
	build.Status.Message = message
	build.Status.FinishedAt = &now
	if build.Status.ResultSummary.Total == 0 {
		build.Status.ResultSummary = summarize(build.Status.Results)
	}
	setPhaseCondition(build, phase, reason, message)
	if r.Recorder != nil {
		eventType := corev1.EventTypeNormal
		if phase == kovav1.PhaseFailed {
			eventType = corev1.EventTypeWarning
		}
		r.Recorder.Event(build, eventType, reason, defaultMessage(message, phase))
	}
	jobCompletions.Add(ctx, 1, attribute.String("kova.phase", phase))
	if build.Status.StartedAt != nil {
		jobDuration.RecordDuration(ctx, time.Since(build.Status.StartedAt.Time), attribute.String("kova.phase", phase))
	}
	return r.Status().Update(ctx, build)
}

func (r *KovaBuildReconciler) persistLogs(ctx context.Context, build *kovav1.KovaBuild) {
	if r.Store == nil || r.Kube == nil || build.Status.RunnerPodName == "" || build.Status.LogArtifactURI != "" {
		return
	}
	logs := newTailBuffer(r.Cfg.MaxLogBytes)
	if err := r.Kube.WritePodLogsTail(ctx, build.Namespace, build.Status.RunnerPodName, -1, logs); err != nil || logs.Len() == 0 {
		return
	}
	raw := logs.Bytes()
	uri, err := r.Store.Put(ctx, "builds/"+build.Name+"/runner.log", bytes.NewReader(raw), int64(len(raw)), "text/plain; charset=utf-8")
	if err != nil {
		return
	}
	digest := sha256.Sum256(raw)
	build.Status.LogArtifactURI = uri
	build.Status.LogArtifactDigest = fmt.Sprintf("sha256:%x", digest)
}

type tailBuffer struct {
	limit int
	data  []byte
}

func newTailBuffer(limit int64) *tailBuffer {
	if limit <= 0 {
		limit = 16 << 20
	}
	return &tailBuffer{limit: int(limit)}
}

func (b *tailBuffer) Write(p []byte) (int, error) {
	written := len(p)
	if len(p) >= b.limit {
		b.data = append(b.data[:0], p[len(p)-b.limit:]...)
		return written, nil
	}
	overflow := len(b.data) + len(p) - b.limit
	if overflow > 0 {
		copy(b.data, b.data[overflow:])
		b.data = b.data[:len(b.data)-overflow]
	}
	b.data = append(b.data, p...)
	return written, nil
}

func (b *tailBuffer) Len() int { return len(b.data) }

func (b *tailBuffer) Bytes() []byte { return append([]byte(nil), b.data...) }

func (r *KovaBuildReconciler) persistResults(ctx context.Context, build *kovav1.KovaBuild) error {
	build.Status.ResultSummary = summarize(build.Status.Results)
	if r.Store == nil || len(build.Status.Results) == 0 {
		return nil
	}
	raw, err := json.Marshal(build.Status.Results)
	if err != nil {
		return fmt.Errorf("encode build results: %w", err)
	}
	uri, err := r.Store.Put(ctx, "builds/"+build.Name+"/results.json", bytes.NewReader(raw), int64(len(raw)), "application/json")
	if err != nil {
		return fmt.Errorf("persist build results: %w", err)
	}
	build.Status.ResultArtifactURI = uri
	digest := sha256.Sum256(raw)
	build.Status.ResultArtifactDigest = fmt.Sprintf("sha256:%x", digest)
	if len(build.Status.Results) > 100 {
		build.Status.Results = build.Status.Results[:100]
	}
	return nil
}

func defaultMessage(message, fallback string) string {
	if message != "" {
		return message
	}
	return fallback
}

type admissionDecision struct {
	Admitted   bool
	Allocation int
	Message    string
}

func (r *KovaBuildReconciler) admission(ctx context.Context, build *kovav1.KovaBuild) (admissionDecision, error) {
	var builds kovav1.KovaBuildList
	if err := r.List(ctx, &builds, client.InNamespace(build.Namespace)); err != nil {
		return admissionDecision{}, err
	}
	active := 0
	usedSlots := 0
	activeByRequester := map[string]int{}
	queued := make([]*kovav1.KovaBuild, 0, len(builds.Items))
	for i := range builds.Items {
		item := &builds.Items[i]
		switch item.Status.Phase {
		case kovav1.PhaseStarting, kovav1.PhaseRunning:
			active++
			activeByRequester[requesterKey(item)]++
			usedSlots += allocatedConcurrency(item)
		case "", kovav1.PhaseQueued:
			queued = append(queued, item)
		}
	}
	jobCapacity := len(queued)
	if r.Cfg.MaxActiveJobs > 0 {
		jobCapacity = r.Cfg.MaxActiveJobs - active
		if jobCapacity <= 0 {
			return admissionDecision{Message: "waiting for an active job slot"}, nil
		}
	}
	slotCapacity := 0
	boundedSlots := r.Cfg.WorkerSlots > 0
	if boundedSlots {
		slotCapacity = r.Cfg.WorkerSlots - usedSlots
		if slotCapacity <= 0 {
			return admissionDecision{Message: "waiting for worker capacity"}, nil
		}
	}
	for _, candidate := range fairQueue(queued) {
		requester := requesterKey(candidate)
		if r.Cfg.MaxActiveJobsPerRequester > 0 && activeByRequester[requester] >= r.Cfg.MaxActiveJobsPerRequester {
			continue
		}
		allocation := requestedConcurrency(candidate)
		if boundedSlots && allocation > slotCapacity {
			allocation = slotCapacity
		}
		if allocation <= 0 || jobCapacity <= 0 {
			break
		}
		if candidate.Name == build.Name {
			return admissionDecision{Admitted: true, Allocation: allocation}, nil
		}
		activeByRequester[requester]++
		jobCapacity--
		if boundedSlots {
			slotCapacity -= allocation
		}
	}
	return admissionDecision{Message: "waiting for fair-share capacity"}, nil
}

func fairQueue(builds []*kovav1.KovaBuild) []*kovav1.KovaBuild {
	groups := map[string][]*kovav1.KovaBuild{}
	for _, build := range builds {
		key := requesterKey(build)
		groups[key] = append(groups[key], build)
	}
	for _, group := range groups {
		sort.Slice(group, func(i, j int) bool { return buildLess(group[i], group[j]) })
	}
	requesters := make([]string, 0, len(groups))
	for requester := range groups {
		requesters = append(requesters, requester)
	}
	sort.Slice(requesters, func(i, j int) bool {
		left, right := groups[requesters[i]][0], groups[requesters[j]][0]
		if buildLess(left, right) {
			return true
		}
		if buildLess(right, left) {
			return false
		}
		return requesters[i] < requesters[j]
	})
	ordered := make([]*kovav1.KovaBuild, 0, len(builds))
	for round := 0; len(ordered) < len(builds); round++ {
		for _, requester := range requesters {
			if round < len(groups[requester]) {
				ordered = append(ordered, groups[requester][round])
			}
		}
	}
	return ordered
}

func buildLess(left, right *kovav1.KovaBuild) bool {
	if left.CreationTimestamp.Equal(&right.CreationTimestamp) {
		return left.Name < right.Name
	}
	return left.CreationTimestamp.Before(&right.CreationTimestamp)
}

func requesterKey(build *kovav1.KovaBuild) string {
	if build.Spec.Requester.Username != "" {
		return build.Spec.Requester.Username
	}
	return "unknown/" + build.Name
}

func requestedConcurrency(build *kovav1.KovaBuild) int {
	if build.Spec.Build.Concurrency > 0 {
		return build.Spec.Build.Concurrency
	}
	return 1
}

func allocatedConcurrency(build *kovav1.KovaBuild) int {
	if build.Status.AllocatedConcurrency > 0 {
		return int(build.Status.AllocatedConcurrency)
	}
	return requestedConcurrency(build)
}

func cancellationRequested(build *kovav1.KovaBuild) bool {
	return build.Annotations[kovav1.CancellationRequestedAnnotation] != ""
}

func sourcePath(build *kovav1.KovaBuild) string {
	u, err := url.Parse(build.Spec.Source.URI)
	if err == nil && u.Scheme == "file" {
		return u.Path
	}
	return runner.MaterializedSourcePath
}

func filesystemPVC(source *url.URL, claim string) string {
	if source.Scheme == "file" {
		return claim
	}
	return ""
}

func filesystemMount(source *url.URL, root string) string {
	if source.Scheme == "file" {
		return root
	}
	return ""
}

func setPhaseCondition(build *kovav1.KovaBuild, phase, reason, message string) {
	status := metav1.ConditionUnknown
	if phase == kovav1.PhaseSucceeded {
		status = metav1.ConditionTrue
	} else if phase == kovav1.PhaseFailed || phase == kovav1.PhaseCancelled {
		status = metav1.ConditionFalse
	}
	if message == "" {
		message = phase
	}
	apiMeta.SetStatusCondition(&build.Status.Conditions, metav1.Condition{
		Type: "Ready", Status: status, Reason: reason, Message: message,
		ObservedGeneration: build.Generation,
	})
}

func summarize(results []kovav1.BuildResult) kovav1.BuildResultSummary {
	summary := kovav1.BuildResultSummary{Total: int32(len(results))}
	for _, result := range results {
		if result.Status == "succeeded" {
			summary.Succeeded++
		} else {
			summary.Failed++
		}
	}
	return summary
}

func buildPodName(name string) string {
	return "kova-job-" + name
}

func isTerminalPhase(phase string) bool {
	switch phase {
	case kovav1.PhaseSucceeded, kovav1.PhaseFailed, kovav1.PhaseCancelled:
		return true
	default:
		return false
	}
}

func podReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}
