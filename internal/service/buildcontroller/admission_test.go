package buildcontroller

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/config"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type emptyCachedBuildList struct{ client.Client }

func (c emptyCachedBuildList) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if builds, ok := list.(*kovav1.KovaBuildList); ok {
		builds.Items = nil
		return nil
	}
	return c.Client.List(ctx, list, opts...)
}

type uncertainConfigMapUpdate struct {
	client.Client
	mu     sync.Mutex
	failed bool
}

func (c *uncertainConfigMapUpdate) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if _, ok := obj.(*corev1.ConfigMap); ok {
		c.mu.Lock()
		defer c.mu.Unlock()
		if !c.failed {
			c.failed = true
			if err := c.Client.Update(ctx, obj, opts...); err != nil {
				return err
			}
			return context.DeadlineExceeded
		}
	}
	return c.Client.Update(ctx, obj, opts...)
}

type uncertainPodCreate struct {
	client.Client
	failed bool
}

func (c *uncertainPodCreate) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if _, ok := obj.(*corev1.Pod); ok && !c.failed {
		c.failed = true
		if err := c.Client.Create(ctx, obj, opts...); err != nil {
			return err
		}
		return context.DeadlineExceeded
	}
	return c.Client.Create(ctx, obj, opts...)
}

type failedStartingStatus struct {
	client.SubResourceWriter
	failed bool
}

func (s *failedStartingStatus) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if build, ok := obj.(*kovav1.KovaBuild); ok && build.Status.Phase == kovav1.PhaseStarting && !s.failed {
		s.failed = true
		return errors.New("injected status failure")
	}
	return s.SubResourceWriter.Update(ctx, obj, opts...)
}

type statusFailureClient struct {
	client.Client
	writer *failedStartingStatus
}

func (c *statusFailureClient) Status() client.SubResourceWriter { return c.writer }

func queuedBuild(name, requester string, order int64, slots int) *kovav1.KovaBuild {
	targets := buildTargets("registry.local/example:" + name)
	if slots > 1 {
		targets = buildTargets("registry.local/example:"+name+"-1", "registry.local/example:"+name+"-2")
	}
	return &kovav1.KovaBuild{
		TypeMeta: metav1.TypeMeta{APIVersion: kovav1.Group + "/" + kovav1.Version, Kind: "KovaBuild"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "jobs", UID: types.UID("uid-" + name),
			CreationTimestamp: metav1.NewTime(time.Unix(order, 0)), Finalizers: []string{cleanupFinalizer}},
		Spec: kovav1.KovaBuildSpec{
			Requester: kovav1.KovaBuildRequester{Username: requester},
			Targets:   targets,
			Source: kovav1.KovaBuildSourceSpec{URI: "https://sources.example.com/build.zip",
				Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
			Build: kovav1.KovaBuildOptions{Format: "oci", Concurrency: slots},
		},
	}
}

func admissionConfig() config.Config {
	return config.Config{
		RunnerImage: "registry.local/kova:dev", BuildkitPlatformAddrs: map[string]string{"linux/amd64": "tcp://buildkit:9094"},
		MaxActiveJobs: 2, MaxActiveJobsPerRequester: 1, WorkerSlots: 3, PollInterval: time.Millisecond,
	}
}

func TestConcurrentReconcilesReserveBeforePodWithStaleCache(t *testing.T) {
	builds := []*kovav1.KovaBuild{
		queuedBuild("alice-1", "alice", 1, 2), queuedBuild("alice-2", "alice", 2, 2),
		queuedBuild("bob-1", "bob", 3, 2), queuedBuild("bob-2", "bob", 4, 2),
		queuedBuild("charlie-1", "charlie", 5, 2), queuedBuild("charlie-2", "charlie", 6, 2),
	}
	objects := make([]client.Object, 0, len(builds))
	for _, build := range builds {
		objects = append(objects, build)
	}
	scheme := testScheme(t)
	base := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(objects...).Build()
	// A manager cache stuck at an empty List cannot affect admission or counts.
	r1 := KovaBuildReconciler{Client: emptyCachedBuildList{base}, APIReader: base, Scheme: scheme, Kube: &fakeKube{}, Cfg: admissionConfig()}
	r2 := KovaBuildReconciler{Client: emptyCachedBuildList{base}, APIReader: base, Scheme: scheme, Kube: &fakeKube{}, Cfg: admissionConfig()}
	var wg sync.WaitGroup
	errCh := make(chan error, len(builds))
	for i, build := range builds {
		reconciler := &r1
		if i%2 == 1 {
			reconciler = &r2
		}
		wg.Add(1)
		go func(name string, reconciler *KovaBuildReconciler) {
			defer wg.Done()
			_, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: name}})
			errCh <- err
		}(build.Name, reconciler)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent reconcile: %v", err)
		}
	}
	var pods corev1.PodList
	if err := base.List(context.Background(), &pods, client.InNamespace("jobs")); err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 2 {
		t.Fatalf("runner Pods = %d, want 2", len(pods.Items))
	}
	admitted := map[string]bool{}
	for _, pod := range pods.Items {
		admitted[pod.Labels["kova.cofy.dev/build-id"]] = true
	}
	if !admitted["alice-1"] || !admitted["bob-1"] {
		t.Fatalf("fair queue admitted %#v", admitted)
	}
	_, ledger, err := r1.readReservations(context.Background(), "jobs")
	if err != nil {
		t.Fatal(err)
	}
	used := 0
	for _, entry := range ledger.Active {
		used += entry.Slots
	}
	if len(ledger.Active) != 2 || used != 3 {
		t.Fatalf("active reservations=%d worker slots=%d", len(ledger.Active), used)
	}
}

func TestReservationSurvivesUnknownWriteAndRestart(t *testing.T) {
	scheme := testScheme(t)
	a := queuedBuild("a", "alice", 1, 1)
	b := queuedBuild("b", "bob", 2, 1)
	base := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(a, b).Build()
	cfg := admissionConfig()
	cfg.MaxActiveJobs, cfg.WorkerSlots = 1, 1
	uncertain := &uncertainConfigMapUpdate{Client: base}
	r1 := KovaBuildReconciler{Client: uncertain, APIReader: base, Scheme: scheme, Kube: &fakeKube{}, Cfg: cfg}
	if _, err := r1.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "a"}}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first reconcile error = %v", err)
	}
	if !uncertain.failed {
		t.Fatal("did not inject an uncertain ConfigMap update")
	}
	r2 := KovaBuildReconciler{Client: base, APIReader: base, Scheme: scheme, Kube: &fakeKube{podClient: base}, Cfg: cfg}
	if _, err := r2.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "b"}}); err != nil {
		t.Fatal(err)
	}
	var pods corev1.PodList
	if err := base.List(context.Background(), &pods); err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 0 {
		t.Fatalf("Pod created without a reservation recovery: %d", len(pods.Items))
	}
	if _, err := r2.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "a"}}); err != nil {
		t.Fatal(err)
	}
	if err := base.List(context.Background(), &pods); err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 1 || pods.Items[0].Labels["kova.cofy.dev/build-id"] != "a" {
		t.Fatalf("recovered Pods = %#v", pods.Items)
	}
	var finished kovav1.KovaBuild
	if err := base.Get(context.Background(), types.NamespacedName{Namespace: "jobs", Name: "a"}, &finished); err != nil {
		t.Fatal(err)
	}
	finished.Status.Phase = kovav1.PhaseFailed
	if err := base.Status().Update(context.Background(), &finished); err != nil {
		t.Fatal(err)
	}
	if _, err := r2.reconcileTerminal(context.Background(), &finished); err != nil {
		t.Fatal(err)
	}
	if _, err := r2.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "b"}}); err != nil {
		t.Fatal(err)
	}
	if err := base.List(context.Background(), &pods); err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 1 || pods.Items[0].Labels["kova.cofy.dev/build-id"] != "b" {
		t.Fatalf("capacity did not transfer after verified cleanup: %#v", pods.Items)
	}
}

func TestPodCreateUnknownResultAndStatusFailureHoldCapacity(t *testing.T) {
	for _, mode := range []string{"pod-create", "status-update"} {
		t.Run(mode, func(t *testing.T) {
			scheme := testScheme(t)
			a := queuedBuild("a", "alice", 1, 1)
			b := queuedBuild("b", "bob", 2, 1)
			base := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(a, b).Build()
			cfg := admissionConfig()
			cfg.MaxActiveJobs, cfg.WorkerSlots = 1, 1
			var writer client.Client
			if mode == "pod-create" {
				writer = &uncertainPodCreate{Client: base}
			} else {
				writer = &statusFailureClient{Client: base, writer: &failedStartingStatus{SubResourceWriter: base.Status()}}
			}
			r1 := KovaBuildReconciler{Client: writer, APIReader: base, Scheme: scheme, Kube: &fakeKube{}, Cfg: cfg}
			if _, err := r1.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "a"}}); mode == "status-update" && err == nil {
				t.Fatal("injected status failure did not reach reconcile")
			}
			r2 := KovaBuildReconciler{Client: base, APIReader: base, Scheme: scheme, Kube: &fakeKube{podClient: base}, Cfg: cfg}
			if _, err := r2.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "b"}}); err != nil {
				t.Fatal(err)
			}
			var pods corev1.PodList
			if err := base.List(context.Background(), &pods); err != nil {
				t.Fatal(err)
			}
			if len(pods.Items) != 1 || pods.Items[0].Labels["kova.cofy.dev/build-id"] != "a" {
				t.Fatalf("capacity escaped after %s: %#v", mode, pods.Items)
			}
			if mode == "status-update" {
				if _, err := r2.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "a"}}); err != nil {
					t.Fatal(err)
				}
			}
			_, ledger, err := r2.readReservations(context.Background(), "jobs")
			if err != nil || len(ledger.Active) != 1 {
				t.Fatalf("ledger after %s: %#v, err=%v", mode, ledger, err)
			}
			var finished kovav1.KovaBuild
			if err := base.Get(context.Background(), types.NamespacedName{Namespace: "jobs", Name: "a"}, &finished); err != nil {
				t.Fatal(err)
			}
			finished.Status.Phase = kovav1.PhaseFailed
			if err := base.Status().Update(context.Background(), &finished); err != nil {
				t.Fatal(err)
			}
			if _, err := r2.reconcileTerminal(context.Background(), &finished); err != nil {
				t.Fatal(err)
			}
			if _, err := r2.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "b"}}); err != nil {
				t.Fatal(err)
			}
			if err := base.List(context.Background(), &pods); err != nil {
				t.Fatal(err)
			}
			if len(pods.Items) != 1 || pods.Items[0].Labels["kova.cofy.dev/build-id"] != "b" {
				t.Fatalf("capacity did not recover after %s cleanup: %#v", mode, pods.Items)
			}
		})
	}
}

func TestMissingReservationForLivePodFailsClosed(t *testing.T) {
	scheme := testScheme(t)
	a := queuedBuild("a", "alice", 1, 1)
	b := queuedBuild("b", "bob", 2, 1)
	base := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(a, b).Build()
	r := KovaBuildReconciler{Client: base, APIReader: base, Scheme: scheme, Kube: &fakeKube{}, Cfg: admissionConfig()}
	if _, _, err := r.readReservations(context.Background(), "jobs"); err != nil {
		t.Fatal(err)
	}
	if err := base.Create(context.Background(), testRunnerPod(a)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.admission(context.Background(), b); err == nil {
		t.Fatal("unreserved runner Pod did not block admission")
	} else if got := err.Error(); got == "" {
		t.Fatal(fmt.Errorf("empty drift error"))
	}
}

func TestFailedPodCleanupRetainsReservation(t *testing.T) {
	build := queuedBuild("a", "alice", 1, 1)
	build.Status.Phase = kovav1.PhaseRunning
	build.Status.RunnerPodName = buildPodName(build.Name)
	base := crfake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build, testRunnerPod(build)).Build()
	kube := &fakeKube{podClient: base, deleteErr: errors.New("injected delete failure")}
	r := KovaBuildReconciler{Client: base, APIReader: base, Kube: kube, Cfg: admissionConfig()}
	if _, state, err := r.readReservations(context.Background(), "jobs"); err != nil || len(state.Active) != 1 {
		t.Fatalf("initial ledger = %#v, err=%v", state, err)
	}
	build.Status.Phase = kovav1.PhaseFailed
	if err := base.Status().Update(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	if _, err := r.reconcileTerminal(context.Background(), build); err == nil {
		t.Fatal("expected cleanup failure")
	}
	if _, state, err := r.readReservations(context.Background(), "jobs"); err != nil || len(state.Active) != 1 {
		t.Fatalf("ledger released before Pod cleanup: %#v, err=%v", state, err)
	}
	kube.deleteErr = nil
	if _, err := r.reconcileTerminal(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	if _, state, err := r.readReservations(context.Background(), "jobs"); err != nil || len(state.Active) != 0 {
		t.Fatalf("ledger retained after confirmed cleanup: %#v, err=%v", state, err)
	}
}
