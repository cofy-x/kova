package buildcontroller

import (
	"context"
	"fmt"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/runnerexec"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Forced stops have a separate, sticky status intent. The controller may
// record it only while the original pair, active charge, CR, and Pod are
// directly witnessed. It cannot infer a stop merely because a Pod is absent.
func genesisStopOutcome(reason string) (phase, message string, err error) {
	switch reason {
	case "Cancelled":
		return kovav1.PhaseCancelled, "build was cancelled", nil
	case "Deleted":
		return kovav1.PhaseCancelled, "build was deleted", nil
	case "BuildTimedOut":
		return kovav1.PhaseFailed, "build exceeded the service maximum duration", nil
	default:
		return "", "", fmt.Errorf("unsupported Genesis stop reason %q", reason)
	}
}

func genesisStopIntent(w *kovav1.AdmissionGenesisWitness, reason string) *kovav1.AdmissionGenesisStopIntent {
	return &kovav1.AdmissionGenesisStopIntent{
		BuildUID: w.BuildUID, PodUID: w.PodUID,
		PodCreateAttempt: w.PodCreateAttempt, RunnerRequestID: w.RunnerRequestID,
		Reason: reason,
	}
}

func (r *KovaBuildReconciler) validGenesisStatusWitness(build *kovav1.KovaBuild) bool {
	if r.Genesis == nil || build == nil || build.UID == "" || build.Status.AdmissionGenesisWitness == nil {
		return false
	}
	w := build.Status.AdmissionGenesisWitness
	original := r.Genesis.Original
	return w.NamespaceUID == original.NamespaceUID && w.GenesisUID == original.GenesisUID &&
		w.Generation == r.Genesis.Bootstrap.Receipt.Contract.Generation &&
		w.ActiveLedgerUID == original.ActiveLedgerUID && w.QueueLedgerUID == original.QueueLedgerUID &&
		w.BuildUID == string(build.UID) && w.PodName == buildPodName(build.Name) &&
		w.PodUID != "" && len(w.PodUID) <= 256 && validReservationHex(w.PodCreateAttempt, 32) &&
		w.RunnerRequestID == runnerexec.RequestID(build) &&
		build.Status.RunnerPodName == w.PodName
}

func validGenesisStopIntent(build *kovav1.KovaBuild) bool {
	if build == nil || build.Status.AdmissionGenesisWitness == nil || build.Status.AdmissionGenesisStopIntent == nil {
		return false
	}
	i := build.Status.AdmissionGenesisStopIntent
	w := build.Status.AdmissionGenesisWitness
	_, _, err := genesisStopOutcome(i.Reason)
	return err == nil && i.BuildUID == w.BuildUID && i.PodUID == w.PodUID &&
		i.PodCreateAttempt == w.PodCreateAttempt && i.RunnerRequestID == w.RunnerRequestID
}

// A cleanup read accepts an original Pod already Terminating and a deleting
// CR, but never a changed same-name Pod. Pod absence is reported separately
// and is useful only after a durable stop intent or terminal result exists.
func (r *KovaBuildReconciler) directGenesisStopState(ctx context.Context, build *kovav1.KovaBuild) (*kovav1.KovaBuild, *corev1.Pod, error) {
	if r.Genesis == nil || r.APIReader == nil || build == nil || build.UID == "" ||
		build.Status.AdmissionGenesisWitness == nil {
		return nil, nil, fmt.Errorf("Genesis stop requires an original durable runner witness")
	}
	var current kovav1.KovaBuild
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
		return nil, nil, err
	}
	if current.UID != build.UID || !r.validGenesisStatusWitness(&current) ||
		*current.Status.AdmissionGenesisWitness != *build.Status.AdmissionGenesisWitness ||
		(build.Status.AdmissionGenesisStopIntent != nil &&
			(current.Status.AdmissionGenesisStopIntent == nil ||
				*current.Status.AdmissionGenesisStopIntent != *build.Status.AdmissionGenesisStopIntent)) ||
		(current.Status.AdmissionGenesisStopIntent != nil && !validGenesisStopIntent(&current)) {
		return nil, nil, fmt.Errorf("Genesis stop CR identity, witness, or intent changed")
	}
	var pod corev1.Pod
	err := r.APIReader.Get(ctx, types.NamespacedName{Namespace: current.Namespace, Name: current.Status.RunnerPodName}, &pod)
	if apierrors.IsNotFound(err) {
		return &current, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	witness, err := r.witnessFromPodMode(&current, &pod, true)
	if err != nil {
		return nil, nil, err
	}
	if *witness != *current.Status.AdmissionGenesisWitness {
		return nil, nil, fmt.Errorf("Genesis stop Pod UID or stamps differ from the durable CR witness")
	}
	return &current, &pod, nil
}

// Cancellation and timeout callers first observe a nonterminal runner status
// under the exact live witness. Explicit CR deletion itself is an irrevocable
// request to discard the result. Once an intent is durable, retries resume
// that same decision rather than re-contacting the runner.
func (r *KovaBuildReconciler) stopGenesisRunner(ctx context.Context, build *kovav1.KovaBuild, reason string) (ctrl.Result, error) {
	if _, _, err := genesisStopOutcome(reason); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.Genesis.Check(ctx); err != nil {
		return ctrl.Result{}, err
	}
	current, pod, err := r.directGenesisStopState(ctx, build)
	if err != nil {
		return ctrl.Result{}, err
	}
	if current.Status.AdmissionGenesisStopIntent == nil {
		if pod == nil || pod.DeletionTimestamp != nil || isTerminalPhase(current.Status.Phase) {
			return ctrl.Result{}, fmt.Errorf("Genesis stop has no live original Pod or active phase before intent")
		}
		if current.Status.Phase != build.Status.Phase {
			return ctrl.Result{}, fmt.Errorf("Genesis stop phase changed after the runner observation")
		}
		switch reason {
		case "Cancelled":
			if !cancellationRequested(current) ||
				(current.Status.Phase != kovav1.PhaseStarting && current.Status.Phase != kovav1.PhaseRunning) {
				return ctrl.Result{}, fmt.Errorf("Genesis cancellation is no longer requested for an active runner")
			}
		case "BuildTimedOut":
			if (current.Status.Phase != kovav1.PhaseStarting && current.Status.Phase != kovav1.PhaseRunning) ||
				r.Cfg.MaxBuildDuration <= 0 || current.Status.StartedAt == nil ||
				time.Since(current.Status.StartedAt.Time) < r.Cfg.MaxBuildDuration {
				return ctrl.Result{}, fmt.Errorf("Genesis build timeout is no longer effective")
			}
		case "Deleted":
			if current.DeletionTimestamp == nil {
				return ctrl.Result{}, fmt.Errorf("Genesis CR deletion is not durable")
			}
		}
		if _, err := r.directGenesisCharge(ctx, current); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.fenceReservation(ctx, current); err != nil {
			return ctrl.Result{}, err
		}
		entry, err := r.directGenesisCharge(ctx, current)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !entry.Closing {
			return ctrl.Result{}, fmt.Errorf("Genesis stop lacks a fenced exact active charge")
		}
		current.Status.AdmissionGenesisStopIntent = genesisStopIntent(current.Status.AdmissionGenesisWitness, reason)
		// An unknown Update result is not permission to stop. The next
		// reconcile reads the status directly and resumes only if it exists.
		if err := r.Status().Update(ctx, current); err != nil {
			return ctrl.Result{}, err
		}
	}
	if current.Status.AdmissionGenesisStopIntent.Reason != reason {
		return ctrl.Result{}, fmt.Errorf("Genesis stop already committed a different outcome")
	}
	return r.resumeGenesisStop(ctx, current)
}

func (r *KovaBuildReconciler) resumeGenesisStop(ctx context.Context, build *kovav1.KovaBuild) (ctrl.Result, error) {
	if err := r.Genesis.Check(ctx); err != nil {
		return ctrl.Result{}, err
	}
	current, pod, err := r.directGenesisStopState(ctx, build)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !validGenesisStopIntent(current) || isTerminalPhase(current.Status.Phase) {
		return ctrl.Result{}, fmt.Errorf("Genesis stop has no valid nonterminal intent")
	}
	entry, err := r.directGenesisCharge(ctx, current)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !entry.Closing {
		return ctrl.Result{}, fmt.Errorf("Genesis stop active charge is not fenced")
	}
	if pod != nil {
		if err := r.deleteOwnedRunnerPodAndConfirm(ctx, current, pod); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, r.finishGenesisStopped(ctx, current)
}

func (r *KovaBuildReconciler) finishGenesisStopped(ctx context.Context, build *kovav1.KovaBuild) error {
	if err := r.Genesis.Check(ctx); err != nil {
		return err
	}
	current, pod, err := r.directGenesisStopState(ctx, build)
	if err != nil {
		return err
	}
	if !validGenesisStopIntent(current) || pod != nil || isTerminalPhase(current.Status.Phase) {
		return fmt.Errorf("Genesis stopped outcome lacks durable intent or direct Pod absence")
	}
	entry, err := r.directGenesisCharge(ctx, current)
	if err != nil {
		return err
	}
	if !entry.Closing || len(entry.InFlight) != 0 {
		return fmt.Errorf("Genesis stopped outcome still has an open or in-flight active charge")
	}
	phase, message, err := genesisStopOutcome(current.Status.AdmissionGenesisStopIntent.Reason)
	if err != nil {
		return err
	}
	return r.writeTerminalStatus(ctx, current, phase, current.Status.AdmissionGenesisStopIntent.Reason, message)
}
