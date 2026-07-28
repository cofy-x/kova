package buildcontroller

import (
	"bytes"
	"context"
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
	Scheme *runtime.Scheme
	Kube   kube.API
	Store  artifactstore.Store
	Cfg    config.Config
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
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "runner image is required")
	}
	admitted, active, err := r.admitted(ctx, build)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !admitted {
		capacityWaits.Add(ctx, 1)
		build.Status.Phase = kovav1.PhaseQueued
		setPhaseCondition(build, kovav1.PhaseQueued, "WaitingForCapacity", "waiting for fair-share capacity")
		build.Status.ObservedGeneration = build.Generation
		if err := r.Status().Update(ctx, build); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: r.Cfg.PollInterval}, nil
	}
	source, err := url.Parse(build.Spec.Source.URI)
	if err != nil {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, err.Error())
	}
	if r.Store != nil {
		reader, err := r.Store.Open(ctx, build.Spec.Source.URI)
		if err != nil {
			return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, fmt.Sprintf("open source artifact: %v", err))
		}
		if err := reader.Close(); err != nil {
			return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, fmt.Sprintf("close source artifact: %v", err))
		}
	}
	if source.Scheme == "file" && r.Cfg.SourcePVCClaim == "" {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "filesystem artifacts require --source-pvc-claim")
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
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, err.Error())
	}
	if err := r.Create(ctx, &pod); err != nil && !apierrors.IsAlreadyExists(err) {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, err.Error())
	}
	now := metav1.Now()
	jobQueueLatency.RecordDuration(ctx, time.Since(build.CreationTimestamp.Time))
	build.Status.Phase = kovav1.PhaseStarting
	build.Status.ObservedGeneration = build.Generation
	build.Status.AllocatedConcurrency = int32(r.allocatedConcurrency(build, active+1))
	build.Status.RunnerPodName = podName
	build.Status.StartedAt = &now
	build.Status.Message = ""
	setPhaseCondition(build, kovav1.PhaseStarting, "RunnerCreated", "runner Pod was created")
	if err := r.Status().Update(ctx, build); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: time.Second}, nil
}

func (r *KovaBuildReconciler) submitWhenReady(ctx context.Context, build *kovav1.KovaBuild) (ctrl.Result, error) {
	var pod corev1.Pod
	if err := r.Get(ctx, types.NamespacedName{Namespace: build.Namespace, Name: build.Status.RunnerPodName}, &pod); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "runner Pod disappeared before the build was submitted")
		}
		return ctrl.Result{}, err
	}
	if !podReady(&pod) {
		if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
			return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, fmt.Sprintf("runner Pod terminated before becoming ready: %s", pod.Status.Phase))
		}
		if build.Status.StartedAt != nil && r.Cfg.WaitTimeout > 0 && time.Since(build.Status.StartedAt.Time) >= r.Cfg.WaitTimeout {
			return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, fmt.Sprintf("runner Pod did not become ready within %s", r.Cfg.WaitTimeout))
		}
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	if err := (runnerexec.Client{Kube: r.Kube, BuildkitAddr: r.Cfg.BuildkitAddr}).SubmitBuild(ctx, build, sourcePath(build)); err != nil {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, err.Error())
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
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, err.Error())
	}
	done, success, err := runner.WaitDecision(state.Status)
	if err != nil {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, err.Error())
	}
	if !done {
		return ctrl.Result{RequeueAfter: r.Cfg.PollInterval}, nil
	}
	if success {
		build.Status.Phase = kovav1.PhaseSucceeded
		build.Status.Results = buildresult.Resolve(ctx, runnerexec.Client{Kube: r.Kube, BuildkitAddr: r.Cfg.BuildkitAddr}, build)
		if !buildresult.AllSucceeded(build.Status.Results) {
			return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "one or more build results could not be verified")
		}
		if err := r.persistResults(ctx, build); err != nil {
			return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, err.Error())
		}
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseSucceeded, "")
	}
	if state.Status == "cancelled" {
		build.Status.Phase = kovav1.PhaseCancelled
		build.Status.Results = buildresult.Resolve(ctx, runnerexec.Client{Kube: r.Kube, BuildkitAddr: r.Cfg.BuildkitAddr}, build)
		if err := r.persistResults(ctx, build); err != nil {
			return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, err.Error())
		}
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseCancelled, state.Error)
	}
	build.Status.Phase = kovav1.PhaseFailed
	build.Status.Results = buildresult.Resolve(ctx, runnerexec.Client{Kube: r.Kube, BuildkitAddr: r.Cfg.BuildkitAddr}, build)
	if err := r.persistResults(ctx, build); err != nil {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, err.Error())
	}
	return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, state.Error)
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

func (r *KovaBuildReconciler) finish(ctx context.Context, build *kovav1.KovaBuild, phase string, message string) error {
	now := metav1.Now()
	build.Status.Phase = phase
	build.Status.ObservedGeneration = build.Generation
	build.Status.Message = message
	build.Status.FinishedAt = &now
	if build.Status.ResultSummary.Total == 0 {
		build.Status.ResultSummary = summarize(build.Status.Results)
	}
	setPhaseCondition(build, phase, phase, message)
	jobCompletions.Add(ctx, 1, attribute.String("kova.phase", phase))
	if build.Status.StartedAt != nil {
		jobDuration.RecordDuration(ctx, time.Since(build.Status.StartedAt.Time), attribute.String("kova.phase", phase))
	}
	return r.Status().Update(ctx, build)
}

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
	if len(build.Status.Results) > 100 {
		build.Status.Results = build.Status.Results[:100]
	}
	return nil
}

func (r *KovaBuildReconciler) admitted(ctx context.Context, build *kovav1.KovaBuild) (bool, int, error) {
	limit := r.activeJobLimit()
	if limit <= 0 {
		return true, 0, nil
	}
	var builds kovav1.KovaBuildList
	if err := r.List(ctx, &builds, client.InNamespace(build.Namespace)); err != nil {
		return false, 0, err
	}
	active := 0
	queued := make([]*kovav1.KovaBuild, 0, len(builds.Items))
	for i := range builds.Items {
		item := &builds.Items[i]
		switch item.Status.Phase {
		case kovav1.PhaseStarting, kovav1.PhaseRunning:
			active++
		case "", kovav1.PhaseQueued:
			queued = append(queued, item)
		}
	}
	available := limit - active
	if available <= 0 {
		return false, active, nil
	}
	sort.Slice(queued, func(i, j int) bool {
		if queued[i].CreationTimestamp.Equal(&queued[j].CreationTimestamp) {
			return queued[i].Name < queued[j].Name
		}
		return queued[i].CreationTimestamp.Before(&queued[j].CreationTimestamp)
	})
	for i := 0; i < len(queued) && i < available; i++ {
		if queued[i].Name == build.Name {
			return true, active, nil
		}
	}
	return false, active, nil
}

func (r *KovaBuildReconciler) allocatedConcurrency(build *kovav1.KovaBuild, active int) int {
	requested := build.Spec.Build.Concurrency
	if requested <= 0 {
		requested = 1
	}
	if r.Cfg.WorkerSlots <= 0 {
		return requested
	}
	jobs := r.activeJobLimit()
	if jobs <= 0 {
		jobs = active
	}
	share := r.Cfg.WorkerSlots / jobs
	if share < 1 {
		share = 1
	}
	if requested < share {
		return requested
	}
	return share
}

func (r *KovaBuildReconciler) activeJobLimit() int {
	limit := r.Cfg.MaxActiveJobs
	if r.Cfg.WorkerSlots > 0 && (limit <= 0 || r.Cfg.WorkerSlots < limit) {
		limit = r.Cfg.WorkerSlots
	}
	return limit
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
