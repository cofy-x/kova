package buildcontroller

import (
	"context"
	"strings"
	"testing"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestRecoveryTombstoneIsNeitherReconciledNorQueued(t *testing.T) {
	build := &kovav1.KovaBuild{ObjectMeta: metav1.ObjectMeta{Namespace: "old", Name: "build-old",
		Labels:      map[string]string{"kova.cofy.dev/recovery-tombstone": "v1"},
		Annotations: map[string]string{"kova.cofy.dev/recovery-tombstone": "incident-one", "kova.cofy.dev/recovery-epoch-uid": "namespace-old-uid"},
		Finalizers:  []string{"kova.cofy.dev/recovery-hold"}},
		Spec: kovav1.KovaBuildSpec{Requester: kovav1.KovaBuildRequester{Username: "recovery-tombstone"},
			Targets: []kovav1.KovaBuildTargetSpec{{Target: "recovery.invalid/tombstone:never", Platform: "linux/amd64"}},
			Source:  kovav1.KovaBuildSourceSpec{URI: "https://recovery.invalid/inert", Digest: "sha256:" + strings.Repeat("0", 64)},
			Build:   kovav1.KovaBuildOptions{Format: "oci", Concurrency: 1}},
		Status: kovav1.KovaBuildStatus{Phase: kovav1.PhaseQueued}}
	scheme := runtime.NewScheme()
	if err := kovav1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	api := fake.NewClientBuilder().WithScheme(scheme).WithObjects(build).Build()
	r := &KovaBuildReconciler{Client: api}
	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: build.Namespace, Name: build.Name}})
	if err != nil || result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("tombstone reconciled: %#v, %v", result, err)
	}
	var unchanged kovav1.KovaBuild
	if err := api.Get(context.Background(), client.ObjectKeyFromObject(build), &unchanged); err != nil {
		t.Fatal(err)
	}
	if len(unchanged.Finalizers) != 1 || unchanged.Status.Phase != kovav1.PhaseQueued {
		t.Fatalf("tombstone mutated: %#v", unchanged)
	}
	if candidates := queuedAdmissionCandidates([]kovav1.KovaBuild{unchanged}, map[string]activeReservation{}); len(candidates) != 0 {
		t.Fatalf("tombstone entered admission queue: %#v", candidates)
	}
}

func TestMarkerOnRealBuildDoesNotSuppressCleanup(t *testing.T) {
	build := &kovav1.KovaBuild{ObjectMeta: metav1.ObjectMeta{Namespace: "old", Name: "real",
		Labels: map[string]string{"kova.cofy.dev/recovery-tombstone": "v1"}},
		Status: kovav1.KovaBuildStatus{Phase: kovav1.PhaseQueued}}
	scheme := runtime.NewScheme()
	if err := kovav1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	api := fake.NewClientBuilder().WithScheme(scheme).WithObjects(build).Build()
	r := &KovaBuildReconciler{Client: api}
	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: build.Namespace, Name: build.Name}})
	if err != nil || !result.Requeue {
		t.Fatalf("real build marker skipped reconcile: %#v, %v", result, err)
	}
	var current kovav1.KovaBuild
	if err := api.Get(context.Background(), client.ObjectKeyFromObject(build), &current); err != nil {
		t.Fatal(err)
	}
	if len(current.Finalizers) != 1 || current.Finalizers[0] != kovav1.CleanupFinalizer {
		t.Fatalf("normal cleanup finalizer missing: %#v", current.Finalizers)
	}
	if candidates := queuedAdmissionCandidates([]kovav1.KovaBuild{current}, map[string]activeReservation{}); len(candidates) != 1 {
		t.Fatalf("marker-only real build suppressed admission: %#v", candidates)
	}
}
