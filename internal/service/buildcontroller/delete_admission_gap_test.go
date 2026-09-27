package buildcontroller

import (
	"context"
	"errors"
	"fmt"
	"testing"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/queueadmission"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type observeAdmissionReleaseClient struct {
	client.Client
	onRelease func(context.Context) error
	released  bool
}

func (c *observeAdmissionReleaseClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if cm, ok := obj.(*corev1.ConfigMap); ok && cm.Name == reservationConfigMap {
		state, err := decodeReservations(cm)
		if err != nil {
			return err
		}
		if len(state.Active) == 0 {
			if err := c.Client.Update(ctx, obj, opts...); err != nil {
				return err
			}
			c.released = true
			return c.onRelease(ctx)
		}
	}
	return c.Client.Update(ctx, obj, opts...)
}

type failDeleteCancellationClient struct{ client.Client }

func (c *failDeleteCancellationClient) Status() client.SubResourceWriter {
	return &failDeleteCancellationWriter{SubResourceWriter: c.Client.Status()}
}

type failDeleteCancellationWriter struct{ client.SubResourceWriter }

func (w *failDeleteCancellationWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if build, ok := obj.(*kovav1.KovaBuild); ok && build.Status.Phase == kovav1.PhaseCancelled {
		return errors.New("injected deletion status failure")
	}
	return w.SubResourceWriter.Update(ctx, obj, opts...)
}

func deletingActiveFixture(t *testing.T) (*KovaBuildReconciler, client.Client, *kovav1.KovaBuild) {
	t.Helper()
	ctx := context.Background()
	build := queuedBuild("deleting", "alice", 1, 1)
	scheme := testScheme(t)
	base := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).Build()
	cfg := admissionConfig()
	cfg.MaxActiveJobs, cfg.WorkerSlots = 1, 1
	r := &KovaBuildReconciler{Client: base, APIReader: base, Scheme: scheme, Kube: &fakeKube{podClient: base}, Cfg: cfg}
	initializeAdmissionForTest(t, r)
	queue := queueadmission.Store{Client: base, Reader: base, Namespace: "jobs", GlobalLimit: cfg.MaxQueuedJobs, RequesterLimit: cfg.MaxQueuedJobsPerRequester}
	if err := queue.EnsureInitialized(ctx); err != nil {
		t.Fatal(err)
	}
	if err := base.Create(ctx, build); err != nil {
		t.Fatal(err)
	}
	build.Status.Phase = kovav1.PhaseStarting
	build.Status.RunnerPodName = buildPodName(build.Name)
	if err := base.Status().Update(ctx, build); err != nil {
		t.Fatal(err)
	}
	if err := base.Create(ctx, testRunnerPod(build)); err != nil {
		t.Fatal(err)
	}
	cm, state, err := r.readReservations(ctx, "jobs")
	if err != nil {
		t.Fatal(err)
	}
	state.Active[reservationKey(build)] = activeReservation{BuildName: build.Name, Requester: requesterKey(build), Slots: 1}
	if err := r.writeReservations(ctx, cm, state); err != nil {
		t.Fatal(err)
	}
	if err := base.Delete(ctx, build); err != nil {
		t.Fatal(err)
	}
	return r, base, build
}

func TestDeleteTerminatesActiveStatusBeforeReleasingReservation(t *testing.T) {
	ctx := context.Background()
	r, base, build := deletingActiveFixture(t)
	pump := NewAdmissionPump(base, r.Cfg)
	writer := &observeAdmissionReleaseClient{Client: base}
	writer.onRelease = func(ctx context.Context) error {
		var current kovav1.KovaBuild
		if err := base.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
			return fmt.Errorf("build disappeared before finalizer release: %w", err)
		}
		if current.Status.Phase != kovav1.PhaseCancelled || current.Status.Reason != "Deleted" || current.DeletionTimestamp.IsZero() {
			return fmt.Errorf("deleting build was not terminal before admission release: phase=%q reason=%q deleting=%t", current.Status.Phase, current.Status.Reason, !current.DeletionTimestamp.IsZero())
		}
		if _, err := pump.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: reservationConfigMap}}); err != nil {
			return fmt.Errorf("admission pump blocked on the deletion gap: %w", err)
		}
		return nil
	}
	r.Client = writer
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: build.Name}}); err != nil {
		t.Fatal(err)
	}
	if !writer.released {
		t.Fatal("active reservation was not released")
	}
}

func TestDeleteKeepsReservationWhenTerminalStatusWriteFails(t *testing.T) {
	ctx := context.Background()
	r, base, build := deletingActiveFixture(t)
	r.Client = &failDeleteCancellationClient{Client: base}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: build.Name}}); err == nil {
		t.Fatal("expected deletion status write failure")
	}
	_, state, err := r.readReservations(ctx, "jobs")
	if err != nil {
		t.Fatal(err)
	}
	reservation, ok := state.Active[reservationKey(build)]
	if !ok || !reservation.Closing {
		t.Fatalf("capacity was released before terminal status was durable: present=%t closing=%t", ok, reservation.Closing)
	}
	var pod corev1.Pod
	if err := base.Get(ctx, client.ObjectKey{Namespace: "jobs", Name: buildPodName(build.Name)}, &pod); !apierrors.IsNotFound(err) {
		t.Fatalf("runner Pod still exists after confirmed cleanup: %v", err)
	}
	var current kovav1.KovaBuild
	if err := base.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != kovav1.PhaseStarting || current.DeletionTimestamp.IsZero() {
		t.Fatalf("build unexpectedly changed after failed status write: phase=%q deleting=%t", current.Status.Phase, !current.DeletionTimestamp.IsZero())
	}
}
