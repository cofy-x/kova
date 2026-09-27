package buildcontroller

import (
	"context"
	"encoding/json"
	"fmt"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	reservationConfigMap = "kova-service-admission"
	reservationDataKey   = "reservations.json"
)

type activeReservation struct {
	BuildName string `json:"buildName"`
	Requester string `json:"requester"`
	Slots     int    `json:"slots"`
}

type reservationState struct {
	Version int                          `json:"version"`
	Active  map[string]activeReservation `json:"active"`
}

func reservationKey(build *kovav1.KovaBuild) string {
	if build.UID != "" {
		return string(build.UID)
	}
	// The API server always sets UID. The name fallback is for fake clients.
	return "name:" + build.Name
}

func (r *KovaBuildReconciler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func (r *KovaBuildReconciler) readReservations(ctx context.Context, namespace string) (*corev1.ConfigMap, reservationState, error) {
	key := client.ObjectKey{Namespace: namespace, Name: reservationConfigMap}
	for {
		var cm corev1.ConfigMap
		err := r.reader().Get(ctx, key, &cm)
		if err == nil {
			state, err := decodeReservations(&cm)
			return &cm, state, err
		}
		if !apierrors.IsNotFound(err) {
			return nil, reservationState{}, err
		}
		state, err := r.seedReservations(ctx, namespace)
		if err != nil {
			return nil, reservationState{}, err
		}
		data, err := json.Marshal(state)
		if err != nil {
			return nil, reservationState{}, err
		}
		cm = corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: reservationConfigMap, Namespace: namespace},
			Data:       map[string]string{reservationDataKey: string(data)},
		}
		if err := r.Create(ctx, &cm); err != nil {
			if apierrors.IsAlreadyExists(err) {
				continue
			}
			return nil, reservationState{}, err
		}
		return &cm, state, nil
	}
}

func decodeReservations(cm *corev1.ConfigMap) (reservationState, error) {
	var state reservationState
	if err := json.Unmarshal([]byte(cm.Data[reservationDataKey]), &state); err != nil {
		return reservationState{}, fmt.Errorf("admission ledger %s/%s is invalid: %w", cm.Namespace, cm.Name, err)
	}
	if state.Version != 1 || state.Active == nil {
		return reservationState{}, fmt.Errorf("admission ledger %s/%s has an unsupported state", cm.Namespace, cm.Name)
	}
	for key, entry := range state.Active {
		if key == "" || entry.BuildName == "" || entry.Requester == "" || entry.Slots < 1 {
			return reservationState{}, fmt.Errorf("admission ledger %s/%s has an invalid active reservation", cm.Namespace, cm.Name)
		}
	}
	return state, nil
}

func (r *KovaBuildReconciler) seedReservations(ctx context.Context, namespace string) (reservationState, error) {
	state := reservationState{Version: 1, Active: map[string]activeReservation{}}
	var builds kovav1.KovaBuildList
	if err := r.reader().List(ctx, &builds, client.InNamespace(namespace)); err != nil {
		return reservationState{}, err
	}
	byName := make(map[string]*kovav1.KovaBuild, len(builds.Items))
	for i := range builds.Items {
		build := &builds.Items[i]
		byName[build.Name] = build
		if build.Status.Phase == kovav1.PhaseStarting || build.Status.Phase == kovav1.PhaseRunning {
			state.Active[reservationKey(build)] = activeReservation{BuildName: build.Name, Requester: requesterKey(build), Slots: allocatedConcurrency(build)}
		}
	}
	var pods corev1.PodList
	if err := r.reader().List(ctx, &pods, client.InNamespace(namespace)); err != nil {
		return reservationState{}, err
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		name := pod.Labels["kova.cofy.dev/build-id"]
		if name == "" {
			continue
		}
		build := byName[name]
		if build != nil && podOwnedByBuild(pod, build) {
			key := reservationKey(build)
			if _, ok := state.Active[key]; !ok {
				state.Active[key] = activeReservation{BuildName: name, Requester: requesterKey(build), Slots: allocatedConcurrency(build)}
			}
			continue
		}
		// An unowned/unknown runner cannot be safely ignored or reclaimed.
		// Charge the whole pool until an operator resolves the mismatch.
		key := "orphan:" + string(pod.UID)
		if pod.UID == "" {
			key = "orphan:" + pod.Name
		}
		slots := r.Cfg.WorkerSlots
		if slots < 1 {
			slots = 1
		}
		state.Active[key] = activeReservation{BuildName: name, Requester: "unknown/" + name, Slots: slots}
	}
	return state, nil
}

func (r *KovaBuildReconciler) writeReservations(ctx context.Context, cm *corev1.ConfigMap, state reservationState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	copy := cm.DeepCopy()
	copy.Data = map[string]string{reservationDataKey: string(data)}
	return r.Update(ctx, copy)
}

func (r *KovaBuildReconciler) releaseReservation(ctx context.Context, build *kovav1.KovaBuild) error {
	key := client.ObjectKey{Namespace: build.Namespace, Name: reservationConfigMap}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var cm corev1.ConfigMap
		if err := r.reader().Get(ctx, key, &cm); err != nil {
			return client.IgnoreNotFound(err)
		}
		state, err := decodeReservations(&cm)
		if err != nil {
			return err
		}
		entry, ok := state.Active[reservationKey(build)]
		if !ok {
			return nil
		}
		if entry.BuildName != build.Name {
			return fmt.Errorf("admission reservation for %s/%s has mismatched build name %q", build.Namespace, build.Name, entry.BuildName)
		}
		delete(state.Active, reservationKey(build))
		if err := r.writeReservations(ctx, &cm, state); err != nil {
			if apierrors.IsConflict(err) {
				continue
			}
			return err
		}
		return nil
	}
}

func podOwnedByBuild(pod *corev1.Pod, build *kovav1.KovaBuild) bool {
	for _, owner := range pod.OwnerReferences {
		if owner.APIVersion == kovav1.Group+"/"+kovav1.Version && owner.Kind == "KovaBuild" && owner.Name == build.Name && owner.UID == build.UID {
			return true
		}
	}
	return false
}

func (r *KovaBuildReconciler) ensureNoUnreservedRunner(ctx context.Context, namespace string, state reservationState, builds []kovav1.KovaBuild) error {
	byName := make(map[string]*kovav1.KovaBuild, len(builds))
	for i := range builds {
		build := &builds[i]
		byName[build.Name] = build
		if build.Status.Phase == kovav1.PhaseStarting || build.Status.Phase == kovav1.PhaseRunning {
			covered, err := r.reservationCovered(ctx, namespace, state, build)
			if err != nil {
				return err
			}
			if !covered {
				return fmt.Errorf("active KovaBuild %s/%s has no admission reservation", namespace, build.Name)
			}
		}
	}
	var pods corev1.PodList
	if err := r.reader().List(ctx, &pods, client.InNamespace(namespace)); err != nil {
		return err
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		name := pod.Labels["kova.cofy.dev/build-id"]
		if name == "" {
			continue
		}
		build := byName[name]
		if build == nil || !podOwnedByBuild(pod, build) {
			return fmt.Errorf("runner Pod %s/%s has no matching KovaBuild owner", namespace, pod.Name)
		}
		covered, err := r.reservationCovered(ctx, namespace, state, build)
		if err != nil {
			return err
		}
		if !covered {
			return fmt.Errorf("runner Pod %s/%s has no admission reservation", namespace, pod.Name)
		}
	}
	return nil
}

func (r *KovaBuildReconciler) reservationCovered(ctx context.Context, namespace string, state reservationState, build *kovav1.KovaBuild) (bool, error) {
	if entry, ok := state.Active[reservationKey(build)]; ok {
		return entry.BuildName == build.Name, nil
	}
	// The Pod/status LIST may be newer than our ledger GET. Re-read the
	// authoritative ledger before declaring drift; a concurrent grant must
	// have committed before the Pod/status became visible.
	var latest corev1.ConfigMap
	if err := r.reader().Get(ctx, client.ObjectKey{Namespace: namespace, Name: reservationConfigMap}, &latest); err != nil {
		return false, err
	}
	current, err := decodeReservations(&latest)
	if err != nil {
		return false, err
	}
	entry, ok := current.Active[reservationKey(build)]
	return ok && entry.BuildName == build.Name, nil
}

func (r *KovaBuildReconciler) getOwnedPod(ctx context.Context, build *kovav1.KovaBuild) (bool, error) {
	var pod corev1.Pod
	err := r.reader().Get(ctx, types.NamespacedName{Namespace: build.Namespace, Name: buildPodName(build.Name)}, &pod)
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !podOwnedByBuild(&pod, build) {
		return false, fmt.Errorf("runner Pod %s/%s is not owned by KovaBuild UID %s", pod.Namespace, pod.Name, build.UID)
	}
	return true, nil
}
