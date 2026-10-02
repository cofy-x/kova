package buildcontroller

import (
	"context"
	"fmt"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/daemonclient"
	"github.com/cofy-x/kova/internal/service/runnerexec"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	genesisNamespaceUIDKey = "kova.cofy.dev/runner-namespace-uid"
	genesisUIDKey          = "kova.cofy.dev/runner-genesis-uid"
	genesisGenerationKey   = "kova.cofy.dev/runner-generation"
	genesisActiveUIDKey    = "kova.cofy.dev/runner-active-ledger-uid"
	genesisQueueUIDKey     = "kova.cofy.dev/runner-queue-ledger-uid"
	genesisBuildUIDKey     = "kova.cofy.dev/runner-build-uid"
	genesisRequestIDKey    = "kova.cofy.dev/runner-request-id"
)

// stampGenesisPod records the original installation before the one Pod
// Create. Pod annotations are mutable in Kubernetes: every later use must
// directly revalidate them together with the Pod UID and owner reference.
func (r *KovaBuildReconciler) stampGenesisPod(ctx context.Context, build *kovav1.KovaBuild, pod *corev1.Pod, attempt string) error {
	if r.Genesis == nil {
		return nil
	}
	if err := r.Genesis.Check(ctx); err != nil {
		return err
	}
	if build.UID == "" || len(build.UID) > 256 || !validReservationHex(attempt, 32) ||
		pod == nil || pod.Namespace != build.Namespace || pod.Name != buildPodName(build.Name) ||
		!podOwnedByBuild(pod, build) {
		return fmt.Errorf("Genesis runner Pod lacks exact CR ownership or create attempt")
	}
	runnerContainer, err := genesisRunnerContainer(pod)
	if err != nil {
		return err
	}
	for _, env := range runnerContainer.Env {
		if env.Name == daemonclient.RunnerPodUIDEnv {
			return fmt.Errorf("Genesis runner Pod UID env collides with configured runner env")
		}
	}
	runnerContainer.Env = append(runnerContainer.Env, corev1.EnvVar{
		Name: daemonclient.RunnerPodUIDEnv,
		ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{
			APIVersion: "v1", FieldPath: "metadata.uid",
		}},
	})
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	w := r.Genesis.Original
	pod.Annotations[genesisNamespaceUIDKey] = w.NamespaceUID
	pod.Annotations[genesisUIDKey] = w.GenesisUID
	pod.Annotations[genesisGenerationKey] = r.Genesis.Bootstrap.Receipt.Contract.Generation
	pod.Annotations[genesisActiveUIDKey] = w.ActiveLedgerUID
	pod.Annotations[genesisQueueUIDKey] = w.QueueLedgerUID
	pod.Annotations[genesisBuildUIDKey] = string(build.UID)
	pod.Annotations[genesisRequestIDKey] = runnerexec.RequestID(build)
	pod.Annotations[podCreateAttemptKey] = attempt
	return nil
}

// witnessFromPod binds a directly observed Pod UID to its write-on-create
// stamps. It does not inspect admission ledgers and therefore cannot grant
// new work after those ledgers disappear.
func (r *KovaBuildReconciler) witnessFromPod(build *kovav1.KovaBuild, pod *corev1.Pod) (*kovav1.AdmissionGenesisWitness, error) {
	return r.witnessFromPodMode(build, pod, false)
}

func (r *KovaBuildReconciler) witnessFromPodMode(build *kovav1.KovaBuild, pod *corev1.Pod, allowTerminating bool) (*kovav1.AdmissionGenesisWitness, error) {
	if r.Genesis == nil || build == nil || pod == nil || build.UID == "" || len(build.UID) > 256 ||
		pod.UID == "" || len(pod.UID) > 256 || pod.Namespace != build.Namespace ||
		pod.Name != buildPodName(build.Name) || pod.Labels["kova.cofy.dev/build-id"] != build.Name ||
		!podOwnedByBuild(pod, build) || (!allowTerminating && pod.DeletionTimestamp != nil) {
		return nil, fmt.Errorf("Genesis runner Pod lacks a live exact UID and CR owner")
	}
	runnerContainer, err := genesisRunnerContainer(pod)
	if err != nil {
		return nil, err
	}
	fenceCount := 0
	for _, env := range runnerContainer.Env {
		if env.Name != daemonclient.RunnerPodUIDEnv {
			continue
		}
		fenceCount++
		if env.Value != "" || env.ValueFrom == nil || env.ValueFrom.FieldRef == nil ||
			env.ValueFrom.FieldRef.APIVersion != "v1" || env.ValueFrom.FieldRef.FieldPath != "metadata.uid" {
			return nil, fmt.Errorf("Genesis runner Pod has no immutable Downward API UID fence")
		}
	}
	if fenceCount != 1 {
		return nil, fmt.Errorf("Genesis runner Pod UID env must have one Downward API source")
	}
	w := r.Genesis.Original
	annotations := pod.Annotations
	attempt := annotations[podCreateAttemptKey]
	requestID := runnerexec.RequestID(build)
	if !validReservationHex(attempt, 32) || len(requestID) > 256 ||
		annotations[genesisNamespaceUIDKey] != w.NamespaceUID ||
		annotations[genesisUIDKey] != w.GenesisUID ||
		annotations[genesisGenerationKey] != r.Genesis.Bootstrap.Receipt.Contract.Generation ||
		annotations[genesisActiveUIDKey] != w.ActiveLedgerUID ||
		annotations[genesisQueueUIDKey] != w.QueueLedgerUID ||
		annotations[genesisBuildUIDKey] != string(build.UID) ||
		annotations[genesisRequestIDKey] != requestID {
		return nil, fmt.Errorf("Genesis runner Pod stamps differ from original admission receipt")
	}
	return &kovav1.AdmissionGenesisWitness{
		NamespaceUID: w.NamespaceUID, GenesisUID: w.GenesisUID,
		Generation:      r.Genesis.Bootstrap.Receipt.Contract.Generation,
		ActiveLedgerUID: w.ActiveLedgerUID, QueueLedgerUID: w.QueueLedgerUID,
		BuildUID: string(build.UID), PodName: pod.Name, PodUID: string(pod.UID),
		PodCreateAttempt: attempt, RunnerRequestID: requestID,
	}, nil
}

func genesisRunnerContainer(pod *corev1.Pod) (*corev1.Container, error) {
	var runner *corev1.Container
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == "runner" {
			if runner != nil {
				return nil, fmt.Errorf("Genesis runner Pod has duplicate runner containers")
			}
			runner = &pod.Spec.Containers[i]
		}
	}
	if runner == nil {
		return nil, fmt.Errorf("Genesis runner Pod has no runner container")
	}
	return runner, nil
}

// directGenesisWitness requires both independently persisted halves: the
// CR status and a direct Pod read. A forged direct/admin Starting CR with no
// controller-issued grant/Pod cannot manufacture runner authority.
func (r *KovaBuildReconciler) directGenesisWitness(ctx context.Context, build *kovav1.KovaBuild) (*kovav1.KovaBuild, *corev1.Pod, error) {
	return r.directGenesisWitnessMode(ctx, build, false, false)
}

// Cleanup may retry after a UID-preconditioned delete has made the original
// Pod Terminating or after the CR has a deletion timestamp. Neither state can
// authorize another runner operation; only the exact stored witness is read.
func (r *KovaBuildReconciler) directGenesisCleanupWitness(ctx context.Context, build *kovav1.KovaBuild) (*kovav1.KovaBuild, *corev1.Pod, error) {
	return r.directGenesisWitnessMode(ctx, build, true, true)
}

func (r *KovaBuildReconciler) directGenesisWitnessMode(ctx context.Context, build *kovav1.KovaBuild, allowDeleting, allowTerminating bool) (*kovav1.KovaBuild, *corev1.Pod, error) {
	if r.Genesis == nil || r.APIReader == nil || build == nil || build.UID == "" {
		return nil, nil, fmt.Errorf("Genesis witness requires an original CR and direct API reader")
	}
	var current kovav1.KovaBuild
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
		return nil, nil, err
	}
	if current.UID != build.UID || (!allowDeleting && current.DeletionTimestamp != nil) ||
		current.Status.AdmissionGenesisWitness == nil || build.Status.AdmissionGenesisWitness == nil ||
		*current.Status.AdmissionGenesisWitness != *build.Status.AdmissionGenesisWitness ||
		current.Status.RunnerPodName != current.Status.AdmissionGenesisWitness.PodName ||
		current.Status.RunnerPodName != buildPodName(build.Name) {
		return nil, nil, fmt.Errorf("Genesis CR status has no matching durable runner witness")
	}
	pod, err := r.getOwnedPod(ctx, &current)
	if err != nil {
		return nil, nil, err
	}
	witness, err := r.witnessFromPodMode(&current, pod, allowTerminating)
	if err != nil {
		return nil, nil, err
	}
	if *witness != *current.Status.AdmissionGenesisWitness {
		return nil, nil, fmt.Errorf("Genesis CR and Pod witnesses disagree")
	}
	return &current, pod, nil
}

// directGenesisGrant is required immediately before a new runner submission.
// It checks the original pair, the live nonclosing CR-UID grant, and the
// separately persisted Pod/CR witness. It is not a lease across a later Exec;
// the caller must revalidate around that operation.
func (r *KovaBuildReconciler) directGenesisGrant(ctx context.Context, build *kovav1.KovaBuild) (*kovav1.KovaBuild, *corev1.Pod, error) {
	if r.Genesis == nil {
		return nil, nil, fmt.Errorf("Genesis guard is not enabled")
	}
	current, pod, err := r.directGenesisWitness(ctx, build)
	if err != nil {
		return nil, nil, err
	}
	if current.Status.Phase != kovav1.PhaseStarting || cancellationRequested(current) {
		return nil, nil, fmt.Errorf("Genesis runner submission requires a live Starting CR")
	}
	entry, err := r.directGenesisCharge(ctx, current)
	if err != nil {
		return nil, nil, err
	}
	if entry.Closing || len(entry.InFlight) != 0 {
		return nil, nil, fmt.Errorf("Genesis runner submission lacks an open active grant")
	}
	return current, pod, nil
}

// A healthy original pair can prove the exact active charge even when the
// entry is Closing during cancellation. Evidence-only observation after pair
// loss instead requires the pre-loss independent Pod/CR witness.
func (r *KovaBuildReconciler) directGenesisCharge(ctx context.Context, build *kovav1.KovaBuild) (activeReservation, error) {
	_, state, err := r.readReservations(ctx, build.Namespace)
	if err != nil {
		return activeReservation{}, err
	}
	entry, found := state.Active[reservationKey(build)]
	if !found || entry.BuildName != build.Name || entry.Requester != requesterKey(build) ||
		entry.Slots != int(build.Status.AllocatedConcurrency) {
		return activeReservation{}, fmt.Errorf("Genesis runner has no exact active charge")
	}
	return entry, nil
}

func samePodUID(left, right *corev1.Pod) bool {
	return left != nil && right != nil && left.UID != types.UID("") && left.UID == right.UID
}

// The protected base includes #61 UID-preconditioned Pod deletion. Until the
// Genesis terminal-first stop path is used, reject legacy stop entry points
// rather than treating UID-safe deletion alone as stop authority.
func (r *KovaBuildReconciler) requireUIDSafePodStop() error {
	if r.Genesis != nil {
		return fmt.Errorf("Genesis runner stop requires the terminal-first stop path")
	}
	return nil
}
