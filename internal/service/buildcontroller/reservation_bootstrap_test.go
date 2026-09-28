package buildcontroller

import (
	"context"
	"testing"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestActiveBootstrapRejectsLegacyObjectsWithoutSeeding(t *testing.T) {
	for _, obj := range []client.Object{
		&kovav1.KovaBuild{ObjectMeta: metav1.ObjectMeta{Name: "queued", Namespace: "jobs"}, Status: kovav1.KovaBuildStatus{Phase: kovav1.PhaseQueued}},
		&kovav1.KovaBuild{ObjectMeta: metav1.ObjectMeta{Name: "running", Namespace: "jobs"}, Status: kovav1.KovaBuildStatus{Phase: kovav1.PhaseRunning}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "orphan", Namespace: "jobs", Labels: map[string]string{"kova.cofy.dev/build-id": "missing"}}},
	} {
		t.Run(obj.GetName(), func(t *testing.T) {
			ctx := context.Background()
			base := crfake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(obj).Build()
			if created, err := EnsureAdmissionLedger(ctx, base, base, "jobs", admissionConfig()); err == nil || created {
				t.Fatalf("legacy object seeded a ledger: created=%t err=%v", created, err)
			}
			var cm corev1.ConfigMap
			if err := base.Get(ctx, client.ObjectKey{Namespace: "jobs", Name: AdmissionLedgerName}, &cm); !apierrors.IsNotFound(err) {
				t.Fatalf("rejected bootstrap left active ledger: %v", err)
			}
		})
	}
}

func TestActiveBootstrapCreationAuthorityIsOnlyForInitialCaller(t *testing.T) {
	ctx := context.Background()
	base := crfake.NewClientBuilder().WithScheme(testScheme(t)).Build()
	created, err := EnsureAdmissionLedger(ctx, base, base, "jobs", admissionConfig())
	if err != nil || !created {
		t.Fatalf("initial bootstrap authority: created=%t err=%v", created, err)
	}
	var before corev1.ConfigMap
	key := client.ObjectKey{Namespace: "jobs", Name: AdmissionLedgerName}
	if err := base.Get(ctx, key, &before); err != nil || len(before.Annotations[bootstrapAttemptKey]) != 32 {
		t.Fatalf("bootstrap identity missing: annotations=%v err=%v", before.Annotations, err)
	}
	created, err = EnsureAdmissionLedger(ctx, base, base, "jobs", admissionConfig())
	if err != nil || created {
		t.Fatalf("subsequent observer got queue creation authority: created=%t err=%v", created, err)
	}
	var after corev1.ConfigMap
	if err := base.Get(ctx, key, &after); err != nil || after.ResourceVersion != before.ResourceVersion || after.Annotations[bootstrapAttemptKey] != before.Annotations[bootstrapAttemptKey] {
		t.Fatalf("observer rewrote bootstrap identity: before=%#v after=%#v err=%v", before.ObjectMeta, after.ObjectMeta, err)
	}
}
