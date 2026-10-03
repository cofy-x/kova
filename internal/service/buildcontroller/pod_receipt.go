package buildcontroller

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/cofy-x/kova/internal/admissioncontract"
	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/runner"
	"github.com/cofy-x/kova/internal/service/recoveryreceipt"
	"github.com/cofy-x/kova/internal/service/runnerexec"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var errPodReceiptUnpinned = errors.New("Pod Create receipt is not pinned; recovery observation only")

// A Genesis Pod explicitly chooses defaults that would otherwise be added
// during admission. In particular, disabling ServiceAccount token automount
// prevents a random-name projected token volume and container mounts from
// changing the pre-Create template. A ServiceAccount that injects image pull
// secrets still changes the full digest and fails closed on direct readback.
func prepareGenesisPodDefaults(pod *corev1.Pod) {
	if pod == nil {
		return
	}
	no := false
	pod.Spec.AutomountServiceAccountToken = &no
	pod.Spec.EnableServiceLinks = &no
	pod.Spec.ServiceAccountName = "default"
	pod.Spec.DNSPolicy = corev1.DNSClusterFirst
	pod.Spec.SchedulerName = "default-scheduler"
	grace := int64(corev1.DefaultTerminationGracePeriodSeconds)
	pod.Spec.TerminationGracePeriodSeconds = &grace
	seconds := int64(300)
	pod.Spec.Tolerations = []corev1.Toleration{
		{Key: "node.kubernetes.io/not-ready", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: &seconds},
		{Key: "node.kubernetes.io/unreachable", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: &seconds},
	}
	for i := range pod.Spec.Containers {
		prepareGenesisContainerDefaults(&pod.Spec.Containers[i])
	}
	for i := range pod.Spec.InitContainers {
		prepareGenesisContainerDefaults(&pod.Spec.InitContainers[i])
	}
	for i := range pod.Spec.Volumes {
		if secret := pod.Spec.Volumes[i].Secret; secret != nil && secret.DefaultMode == nil {
			mode := int32(0644)
			secret.DefaultMode = &mode
		}
	}
}

func prepareGenesisContainerDefaults(container *corev1.Container) {
	// Pod admission fills each missing request from its matching limit for
	// regular and init containers. Do it before the receipt digest so a
	// legitimate limit-only configuration survives direct API readback.
	if container.Resources.Limits != nil {
		if container.Resources.Requests == nil {
			container.Resources.Requests = make(corev1.ResourceList)
		}
		for name, limit := range container.Resources.Limits {
			if _, exists := container.Resources.Requests[name]; !exists {
				container.Resources.Requests[name] = limit.DeepCopy()
			}
		}
	}
	if container.ImagePullPolicy == "" {
		container.ImagePullPolicy = corev1.PullIfNotPresent
	}
	container.TerminationMessagePath = corev1.TerminationMessagePathDefault
	if container.TerminationMessagePolicy == "" {
		container.TerminationMessagePolicy = corev1.TerminationMessageReadFile
	}
	for _, probe := range []*corev1.Probe{container.LivenessProbe, container.ReadinessProbe, container.StartupProbe} {
		if probe != nil && probe.SuccessThreshold == 0 {
			probe.SuccessThreshold = 1
		}
	}
}

func samePodAttempt(left, right activeReservation) bool {
	return sameGrant(left, right) && left.GrantObservedRV == right.GrantObservedRV &&
		left.GrantReceiptUID == right.GrantReceiptUID && left.GrantReceiptDigest == right.GrantReceiptDigest &&
		left.PodAttemptNonce == right.PodAttemptNonce
}

func (r *KovaBuildReconciler) validateGenesisPodRuntimeIdentity(build *kovav1.KovaBuild, pod *corev1.Pod) error {
	if build == nil || pod == nil || len(pod.Spec.Containers) != 1 ||
		pod.Spec.Containers[0].Name != "runner" || pod.Spec.Containers[0].Image != r.Cfg.RunnerImage ||
		!slices.Equal(pod.Spec.Containers[0].Command, []string{"kovad", "daemon"}) ||
		pod.Spec.RestartPolicy != corev1.RestartPolicyNever {
		return fmt.Errorf("Genesis Pod runner differs from pinned image and command")
	}
	if build.Spec.Source.URI == "" {
		if len(pod.Spec.InitContainers) != 0 {
			return fmt.Errorf("Genesis Pod has unexpected source-fetch container")
		}
		return nil
	}
	if len(pod.Spec.InitContainers) != 1 || pod.Spec.InitContainers[0].Name != "source-fetch" ||
		pod.Spec.InitContainers[0].Image != r.Cfg.RunnerImage {
		return fmt.Errorf("Genesis Pod source-fetch image differs from pinned runner image")
	}
	command := pod.Spec.InitContainers[0].Command
	want := []string{"kovad", "source", "fetch", "--uri", build.Spec.Source.URI,
		"--digest", build.Spec.Source.Digest, "--output", runner.MaterializedSourcePath}
	if len(command) < len(want) || !slices.Equal(command[:len(want)], want) {
		return fmt.Errorf("Genesis Pod source-fetch command differs from original source")
	}
	return nil
}

// podIntent derives all fields from the original epoch and the persistent
// active charge. The Pod template digest is the second-CAS arm, not a value
// reconstructed from an observed Pod or its mutable annotation.
func (r *KovaBuildReconciler) podIntent(ctx context.Context, build *kovav1.KovaBuild, entry activeReservation) (recoveryreceipt.PodCreateIntent, error) {
	if !validReservationHex(entry.PodAttemptNonce, 32) || !validGrantReceiptDigest(entry.PodTemplateDigest) ||
		!validGrantReceiptUID(entry.GrantReceiptUID) || !validGrantReceiptDigest(entry.GrantReceiptDigest) {
		return recoveryreceipt.PodCreateIntent{}, fmt.Errorf("Genesis Pod attempt has no reconstructible pre-effect identity")
	}
	grant, err := r.grantIntent(ctx, build, entry)
	if err != nil {
		return recoveryreceipt.PodCreateIntent{}, err
	}
	grantCM, err := recoveryreceipt.NewGrantConfigMap(grant)
	if err != nil {
		return recoveryreceipt.PodCreateIntent{}, err
	}
	if r.Cfg.RunnerImage != r.Genesis.Bootstrap.Receipt.Contract.RunnerImage {
		return recoveryreceipt.PodCreateIntent{}, fmt.Errorf("runner image differs from immutable admission Genesis")
	}
	digest, err := admissioncontract.RunnerManifestDigest(r.Cfg.RunnerImage)
	if err != nil || digest != r.Cfg.RunnerImageDigest {
		return recoveryreceipt.PodCreateIntent{}, fmt.Errorf("runner image digest is not derived from the original manifest reference")
	}
	return recoveryreceipt.PodCreateIntent{
		Build: grant.Build,
		GrantReceipt: recoveryreceipt.ReceiptLink{
			Name: grantCM.Name, UID: entry.GrantReceiptUID, DataDigest: entry.GrantReceiptDigest,
		},
		PodName: buildPodName(build.Name), PodAttemptNonce: entry.PodAttemptNonce,
		PodTemplateDigest: entry.PodTemplateDigest, RunnerImageDigest: digest,
		BuildRequestID: runnerexec.RequestID(build),
	}, nil
}

// Only the caller that committed beginPodCreate's nonce may arm its receipt
// Create. A terminal cleanup CAS can close/remove an unarmed attempt; this
// second CAS then conflicts. Losing this arm's response never authorizes a
// receipt Create, even if a later observation finds the same digest.
func (r *KovaBuildReconciler) armFreshPodAttempt(ctx context.Context, build *kovav1.KovaBuild, attempt, digest string) (activeReservation, error) {
	if !validReservationHex(attempt, 32) || !validGrantReceiptDigest(digest) {
		return activeReservation{}, fmt.Errorf("invalid Pod attempt or canonical template digest")
	}
	for retry := 0; retry < maxReservationCASAttempts; retry++ {
		cm, state, err := r.readReservations(ctx, build.Namespace)
		if err != nil {
			return activeReservation{}, err
		}
		entry, ok := state.Active[reservationKey(build)]
		if !ok || entry.BuildName != build.Name || entry.PodAttemptNonce != attempt ||
			entry.PodTemplateDigest != "" || entry.PodReceiptUID != "" || entry.Closing || len(entry.InFlight) != 0 {
			return activeReservation{}, fmt.Errorf("fresh Pod attempt lost its unarmed active charge")
		}
		if err := r.ensureLiveAdmissionBuild(ctx, build); err != nil {
			return activeReservation{}, err
		}
		if err := r.verifyPinnedGrant(ctx, build, entry); err != nil {
			return activeReservation{}, err
		}
		entry.PodTemplateDigest = digest
		state.Active[reservationKey(build)] = entry
		if err := r.writeReservations(ctx, cm, state); err != nil {
			if apierrors.IsConflict(err) {
				if err := waitReservationCAS(ctx, retry); err != nil {
					return activeReservation{}, err
				}
				continue
			}
			return activeReservation{}, err
		}
		_, observed, err := r.readReservations(ctx, build.Namespace)
		if err != nil {
			return activeReservation{}, err
		}
		armed, ok := observed.Active[reservationKey(build)]
		if !ok || !samePodAttempt(armed, entry) || armed.PodTemplateDigest != digest ||
			armed.PodReceiptUID != "" || armed.Closing || len(armed.InFlight) != 0 {
			return activeReservation{}, fmt.Errorf("armed Pod attempt changed before receipt Create")
		}
		return armed, nil
	}
	return activeReservation{}, fmt.Errorf("active ledger is busy arming Pod attempt for %s/%s", build.Namespace, build.Name)
}

func (r *KovaBuildReconciler) observePodReceipt(ctx context.Context, build *kovav1.KovaBuild, entry activeReservation) (recoveryreceipt.EffectWitness, error) {
	if err := r.checkGrantReceiptNamespace(ctx, build.Namespace); err != nil {
		return recoveryreceipt.EffectWitness{}, err
	}
	intent, err := r.podIntent(ctx, build, entry)
	if err != nil {
		return recoveryreceipt.EffectWitness{}, err
	}
	witness, err := recoveryreceipt.ObservePodCreate(ctx, r.RecoveryReceipts, intent)
	if err != nil {
		return recoveryreceipt.EffectWitness{}, err
	}
	if err := r.checkGrantReceiptNamespace(ctx, build.Namespace); err != nil {
		return recoveryreceipt.EffectWitness{}, err
	}
	return witness, r.Genesis.Check(ctx)
}

func (r *KovaBuildReconciler) verifyPinnedPodReceipt(ctx context.Context, build *kovav1.KovaBuild, entry activeReservation, allowClosing bool) error {
	if !allowClosing && entry.Closing {
		return fmt.Errorf("%w: %s/%s Pod attempt is closing", errAdmissionClosed, build.Namespace, build.Name)
	}
	if entry.PodReceiptUID == "" || entry.PodReceiptDigest == "" {
		if entry.PodTemplateDigest != "" {
			_, _ = r.observePodReceipt(ctx, build, entry)
		}
		return errPodReceiptUnpinned
	}
	witness, err := r.observePodReceipt(ctx, build, entry)
	if err != nil {
		return err
	}
	if witness.ReceiptUID != entry.PodReceiptUID || witness.DataDigest != entry.PodReceiptDigest {
		return recoveryreceipt.ErrChanged
	}
	return nil
}

func (r *KovaBuildReconciler) pinPodReceipt(ctx context.Context, build *kovav1.KovaBuild, armed activeReservation, witness recoveryreceipt.EffectWitness) (activeReservation, error) {
	if !validGrantReceiptUID(witness.ReceiptUID) || !validGrantReceiptDigest(witness.DataDigest) {
		return activeReservation{}, recoveryreceipt.ErrChanged
	}
	for retry := 0; retry < maxReservationCASAttempts; retry++ {
		cm, state, err := r.readReservations(ctx, build.Namespace)
		if err != nil {
			return activeReservation{}, err
		}
		entry, ok := state.Active[reservationKey(build)]
		if !ok || !samePodAttempt(entry, armed) || entry.PodTemplateDigest != armed.PodTemplateDigest || entry.PodCleanupReady {
			return activeReservation{}, fmt.Errorf("Pod attempt changed before receipt pin")
		}
		if entry.PodReceiptUID != "" {
			if entry.PodReceiptUID != witness.ReceiptUID || entry.PodReceiptDigest != witness.DataDigest {
				return activeReservation{}, recoveryreceipt.ErrChanged
			}
			return entry, nil
		}
		entry.PodReceiptUID, entry.PodReceiptDigest = witness.ReceiptUID, witness.DataDigest
		state.Active[reservationKey(build)] = entry
		if err := r.writeReservations(ctx, cm, state); err != nil {
			if apierrors.IsConflict(err) {
				if err := waitReservationCAS(ctx, retry); err != nil {
					return activeReservation{}, err
				}
				continue
			}
			_, observed, readErr := r.readReservations(ctx, build.Namespace)
			if readErr == nil {
				pinned, ok := observed.Active[reservationKey(build)]
				if ok && samePodAttempt(pinned, armed) && pinned.PodTemplateDigest == armed.PodTemplateDigest &&
					pinned.PodReceiptUID == witness.ReceiptUID && pinned.PodReceiptDigest == witness.DataDigest {
					return pinned, nil
				}
			}
			return activeReservation{}, err
		}
		_, observed, err := r.readReservations(ctx, build.Namespace)
		if err != nil {
			return activeReservation{}, err
		}
		pinned, ok := observed.Active[reservationKey(build)]
		if !ok || !samePodAttempt(pinned, armed) || pinned.PodTemplateDigest != armed.PodTemplateDigest ||
			pinned.PodReceiptUID != witness.ReceiptUID || pinned.PodReceiptDigest != witness.DataDigest {
			return activeReservation{}, recoveryreceipt.ErrChanged
		}
		return pinned, nil
	}
	return activeReservation{}, fmt.Errorf("active ledger is busy pinning Pod receipt for %s/%s", build.Namespace, build.Name)
}

// The final CAS is the sole Pod-Create issue fence. A cleanup CAS that closes
// a pinned-but-unissued attempt makes this conflict; a successful issue keeps
// InFlight charged until definitive rejection or exact Pod observation.
func (r *KovaBuildReconciler) authorizeFreshPodCreate(ctx context.Context, build *kovav1.KovaBuild, pinned activeReservation) error {
	for retry := 0; retry < maxReservationCASAttempts; retry++ {
		cm, state, err := r.readReservations(ctx, build.Namespace)
		if err != nil {
			return err
		}
		entry, ok := state.Active[reservationKey(build)]
		if !ok || !samePodAttempt(entry, pinned) || entry.PodTemplateDigest != pinned.PodTemplateDigest ||
			entry.PodReceiptUID != pinned.PodReceiptUID || entry.PodReceiptDigest != pinned.PodReceiptDigest ||
			entry.Closing || entry.PodCleanupReady || len(entry.InFlight) != 0 {
			return fmt.Errorf("pinned Pod attempt changed before Create issue")
		}
		if err := r.ensureLiveAdmissionBuild(ctx, build); err != nil {
			return err
		}
		if err := r.verifyPinnedGrant(ctx, build, entry); err != nil {
			return err
		}
		if err := r.verifyPinnedPodReceipt(ctx, build, entry, false); err != nil {
			return err
		}
		entry.InFlight = []string{pinned.PodAttemptNonce}
		state.Active[reservationKey(build)] = entry
		if err := r.writeReservations(ctx, cm, state); err != nil {
			if apierrors.IsConflict(err) {
				if err := waitReservationCAS(ctx, retry); err != nil {
					return err
				}
				continue
			}
			// A lost issue response may be resolved for this same fresh owner
			// only by an authoritative read of its exact persistent nonce.
			if r.freshIssueRecorded(ctx, build, entry) == nil {
				return nil
			}
			return err
		}
		if err := r.freshIssueRecorded(ctx, build, entry); err != nil {
			return err
		}
		return nil
	}
	return fmt.Errorf("active ledger is busy issuing Pod Create for %s/%s", build.Namespace, build.Name)
}

func (r *KovaBuildReconciler) freshIssueRecorded(ctx context.Context, build *kovav1.KovaBuild, issued activeReservation) error {
	_, state, err := r.readReservations(ctx, build.Namespace)
	if err != nil {
		return err
	}
	entry, ok := state.Active[reservationKey(build)]
	if !ok || !samePodAttempt(entry, issued) || entry.Closing || entry.PodCleanupReady ||
		entry.PodTemplateDigest != issued.PodTemplateDigest ||
		entry.PodReceiptUID != issued.PodReceiptUID || entry.PodReceiptDigest != issued.PodReceiptDigest ||
		len(entry.InFlight) != 1 || entry.InFlight[0] != issued.PodAttemptNonce {
		return fmt.Errorf("Pod Create issue fence is not durably recorded")
	}
	return nil
}

func (r *KovaBuildReconciler) finishFreshPodAttempt(ctx context.Context, build *kovav1.KovaBuild, pod *corev1.Pod, attempt string) error {
	if pod == nil || pod.Namespace != build.Namespace || pod.Name != buildPodName(build.Name) ||
		pod.Annotations[podCreateAttemptKey] != attempt || !podOwnedByBuild(pod, build) {
		return fmt.Errorf("Pod template differs from fresh attempt or CR owner")
	}
	if err := r.validateGenesisPodRuntimeIdentity(build, pod); err != nil {
		return err
	}
	digest, err := recoveryreceipt.CanonicalPodTemplateDigest(pod)
	if err != nil {
		return err
	}
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[recoveryreceipt.PodTemplateDigestAnnotation] = digest
	armed, err := r.armFreshPodAttempt(ctx, build, attempt, digest)
	if err != nil {
		return err
	}
	if err := r.ensureLiveAdmissionBuild(ctx, build); err != nil {
		return err
	}
	if err := r.checkGrantReceiptNamespace(ctx, build.Namespace); err != nil {
		return err
	}
	intent, err := r.podIntent(ctx, build, armed)
	if err != nil {
		return err
	}
	witness, err := recoveryreceipt.RecordPodCreateOnce(ctx, r.RecoveryReceipts, intent)
	if err != nil {
		return err
	}
	if err := r.checkGrantReceiptNamespace(ctx, build.Namespace); err != nil {
		return err
	}
	pinned, err := r.pinPodReceipt(ctx, build, armed, witness)
	if err != nil {
		return err
	}
	return r.authorizeFreshPodCreate(ctx, build, pinned)
}

// qualifyGenesisPod checks the direct Pod's full canonical template against
// the separately pinned immutable pre-Create receipt. Its annotation is only
// one equality check; it cannot certify a changed image, command or volume.
func (r *KovaBuildReconciler) qualifyGenesisPod(ctx context.Context, build *kovav1.KovaBuild, pod *corev1.Pod, allowClosing bool) error {
	if pod == nil || pod.UID == "" || pod.Namespace != build.Namespace || pod.Name != buildPodName(build.Name) ||
		!podOwnedByBuild(pod, build) {
		return fmt.Errorf("Genesis Pod has no exact original UID and CR owner")
	}
	if err := r.validateGenesisPodRuntimeIdentity(build, pod); err != nil {
		return err
	}
	_, state, err := r.readReservations(ctx, build.Namespace)
	if err != nil {
		return err
	}
	entry, ok := state.Active[reservationKey(build)]
	if !ok || entry.BuildName != build.Name || entry.PodAttemptNonce == "" || entry.PodReceiptUID == "" ||
		pod.Annotations[podCreateAttemptKey] != entry.PodAttemptNonce ||
		pod.Annotations[recoveryreceipt.PodTemplateDigestAnnotation] != entry.PodTemplateDigest {
		return fmt.Errorf("Genesis Pod lacks the original receipted attempt")
	}
	actual, err := recoveryreceipt.CanonicalPodTemplateDigest(pod)
	if err != nil || actual != entry.PodTemplateDigest {
		return fmt.Errorf("Genesis Pod execution template differs from immutable pre-Create receipt")
	}
	return r.verifyPinnedPodReceipt(ctx, build, entry, allowClosing)
}

func (r *KovaBuildReconciler) completeGenesisPodCreate(ctx context.Context, build *kovav1.KovaBuild, attempt string) error {
	for retry := 0; retry < maxReservationCASAttempts; retry++ {
		cm, state, err := r.readReservations(ctx, build.Namespace)
		if err != nil {
			return err
		}
		entry, ok := state.Active[reservationKey(build)]
		if !ok || entry.BuildName != build.Name || entry.PodAttemptNonce != attempt || entry.PodReceiptUID == "" {
			return fmt.Errorf("Genesis Pod completion lacks exact spent and receipted attempt")
		}
		if len(entry.InFlight) == 0 {
			return nil
		}
		if len(entry.InFlight) != 1 || entry.InFlight[0] != attempt {
			return fmt.Errorf("Genesis Pod completion differs from in-flight issue fence")
		}
		if err := r.verifyPinnedPodReceipt(ctx, build, entry, true); err != nil {
			return err
		}
		entry.InFlight = nil
		state.Active[reservationKey(build)] = entry
		if err := r.writeReservations(ctx, cm, state); err != nil {
			if apierrors.IsConflict(err) {
				if err := waitReservationCAS(ctx, retry); err != nil {
					return err
				}
				continue
			}
			_, observed, readErr := r.readReservations(ctx, build.Namespace)
			if readErr == nil {
				complete, ok := observed.Active[reservationKey(build)]
				if ok && samePodAttempt(complete, entry) && complete.PodReceiptUID == entry.PodReceiptUID && len(complete.InFlight) == 0 {
					return nil
				}
			}
			return err
		}
		return nil
	}
	return fmt.Errorf("active ledger is busy completing Pod Create for %s/%s", build.Namespace, build.Name)
}

// A spent but unarmed attempt is removable by the closing CAS: its paused
// owner must arm first. An armed missing receipt is never treated as absent
// effect because its one Create may arrive later. A pinned receipt needs a
// durable cleanup marker before UID-preconditioned deletion.
func (r *KovaBuildReconciler) releaseGenesisPodReceipt(ctx context.Context, build *kovav1.KovaBuild, cm *corev1.ConfigMap, state reservationState, entry activeReservation) (bool, error) {
	if entry.PodTemplateDigest == "" {
		return true, nil
	}
	if entry.PodReceiptUID == "" {
		witness, err := r.observePodReceipt(ctx, build, entry)
		if err != nil {
			return false, fmt.Errorf("armed Pod receipt outcome is unknown: %w", err)
		}
		if _, err := r.pinPodReceipt(ctx, build, entry, witness); err != nil {
			return false, err
		}
		return false, nil
	}
	if !entry.PodCleanupReady {
		if err := r.verifyPinnedPodReceipt(ctx, build, entry, true); err != nil {
			return false, err
		}
		entry.PodCleanupReady = true
		state.Active[reservationKey(build)] = entry
		if err := r.writeReservations(ctx, cm, state); err != nil {
			return false, err
		}
		return false, nil
	}
	if err := r.deletePinnedPodReceipt(ctx, build, entry); err != nil {
		return false, err
	}
	return true, nil
}

func (r *KovaBuildReconciler) deletePinnedPodReceipt(ctx context.Context, build *kovav1.KovaBuild, entry activeReservation) error {
	if !entry.PodCleanupReady || !entry.Closing || entry.PodReceiptUID == "" {
		return fmt.Errorf("Pod receipt lacks durable cleanup disposition")
	}
	if err := r.checkGrantReceiptNamespace(ctx, build.Namespace); err != nil {
		return err
	}
	intent, err := r.podIntent(ctx, build, entry)
	if err != nil {
		return err
	}
	expected, err := recoveryreceipt.NewPodCreateConfigMap(intent)
	if err != nil {
		return err
	}
	observed, err := r.RecoveryReceipts.Get(ctx, expected.Name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if err == nil {
		witness, err := recoveryreceipt.QualifyPodCreate(intent, observed)
		if err != nil || witness.ReceiptUID != entry.PodReceiptUID || witness.DataDigest != entry.PodReceiptDigest {
			return recoveryreceipt.ErrChanged
		}
		uid := types.UID(entry.PodReceiptUID)
		if err := r.RecoveryReceipts.Delete(ctx, expected.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	if _, err := r.RecoveryReceipts.Get(ctx, expected.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		if err == nil {
			return fmt.Errorf("Pod receipt still exists after UID-preconditioned Delete")
		}
		return err
	}
	if err := r.checkGrantReceiptNamespace(ctx, build.Namespace); err != nil {
		return err
	}
	return r.Genesis.Check(ctx)
}

func (r *KovaBuildReconciler) pinnedGenesisPodForBuild(ctx context.Context, build *kovav1.KovaBuild, allowClosing bool) (*corev1.Pod, error) {
	var pod corev1.Pod
	err := r.reader().Get(ctx, client.ObjectKey{Namespace: build.Namespace, Name: buildPodName(build.Name)}, &pod)
	if err != nil {
		return nil, err
	}
	if err := r.qualifyGenesisPod(ctx, build, &pod, allowClosing); err != nil {
		return nil, err
	}
	return &pod, nil
}
