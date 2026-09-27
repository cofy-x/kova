package buildcontroller

import (
	"context"
	"fmt"
	"time"
	"unicode/utf8"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"

	"go.opentelemetry.io/otel/attribute"
	corev1 "k8s.io/api/core/v1"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const admissionRecoveryCondition = "AdmissionRecoveryRequired"

func (r *KovaBuildReconciler) markAdmissionRecovery(ctx context.Context, build *kovav1.KovaBuild, pending int) error {
	message := fmt.Sprintf("%d runner Pod create attempt(s) have an unknown outcome; capacity remains reserved", pending)
	return r.markAdmissionRecoveryReason(ctx, build, "PodCreateOutcomeUnknown", message)
}

func (r *KovaBuildReconciler) markAdmissionRecoveryReason(ctx context.Context, build *kovav1.KovaBuild, reason, message string) error {
	if current := apiMeta.FindStatusCondition(build.Status.Conditions, admissionRecoveryCondition); current != nil && current.Status == metav1.ConditionTrue && current.Reason == reason && current.Message == message {
		return nil
	}
	apiMeta.SetStatusCondition(&build.Status.Conditions, metav1.Condition{
		Type: admissionRecoveryCondition, Status: metav1.ConditionTrue,
		Reason: reason, Message: message,
		ObservedGeneration: build.Generation,
	})
	if r.Recorder != nil {
		r.Recorder.Event(build, corev1.EventTypeWarning, "AdmissionRecoveryRequired", message)
	}
	return r.Status().Update(ctx, build)
}

func (r *KovaBuildReconciler) clearAdmissionRecovery(ctx context.Context, build *kovav1.KovaBuild) error {
	if !apiMeta.RemoveStatusCondition(&build.Status.Conditions, admissionRecoveryCondition) {
		return nil
	}
	return r.Status().Update(ctx, build)
}

func (r *KovaBuildReconciler) finish(ctx context.Context, build *kovav1.KovaBuild, phase string, reason string, message string) error {
	now := metav1.Now()
	build.Status.Phase = phase
	build.Status.ObservedGeneration = build.Generation
	build.Status.Reason = truncate(reason, 128)
	build.Status.Message = truncate(message, 2048)
	build.Status.FinishedAt = &now
	build.Status.PollFailureSince = nil
	build.Status.PollFailureCount = 0
	setPhaseCondition(build, phase, build.Status.Reason, build.Status.Message)
	// A failed status write is retried, possibly by another leader. Publish
	// completion signals only after the durable terminal receipt exists.
	if err := r.Status().Update(ctx, build); err != nil {
		return err
	}
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
	return nil
}

func defaultMessage(message, fallback string) string {
	if message != "" {
		return message
	}
	return fallback
}

func setPhaseCondition(build *kovav1.KovaBuild, phase, reason, message string) {
	reason = truncate(reason, 128)
	message = truncate(message, 2048)
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

func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	value = value[:limit]
	for !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}

func cancellationRequested(build *kovav1.KovaBuild) bool {
	return build.Annotations[kovav1.CancellationRequestedAnnotation] != ""
}

func isTerminalPhase(phase string) bool {
	switch phase {
	case kovav1.PhaseSucceeded, kovav1.PhaseFailed, kovav1.PhaseCancelled:
		return true
	default:
		return false
	}
}
