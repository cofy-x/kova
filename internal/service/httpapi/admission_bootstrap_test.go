package httpapi

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/buildcontroller"
	"github.com/cofy-x/kova/internal/service/config"
	"github.com/cofy-x/kova/internal/service/queueadmission"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type bootstrapWriter struct {
	client.Client
	attempts atomic.Int32
	writes   atomic.Int32
	before   func(context.Context, client.Object) error
	after    func(client.Object) error
}

func (c *bootstrapWriter) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	c.attempts.Add(1)
	if c.before != nil {
		if err := c.before(ctx, obj); err != nil {
			return err
		}
	}
	if err := c.Client.Create(ctx, obj, opts...); err != nil {
		return err
	}
	c.writes.Add(1)
	if c.after != nil {
		return c.after(obj)
	}
	return nil
}

func newBootstrapServer(t *testing.T, cfg config.Config, objects ...client.Object) (*Server, *bootstrapWriter, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := kovav1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	base := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(objects...).Build()
	writer := &bootstrapWriter{Client: base}
	return NewServer(cfg, &fakeKube{}, writer, base, nil, nil, nil), writer, base
}

func assertBootstrapLedgers(t *testing.T, srv *Server, base client.Client, want int) {
	t.Helper()
	var cms corev1.ConfigMapList
	if err := base.List(context.Background(), &cms, client.InNamespace(srv.cfg.Namespace)); err != nil {
		t.Fatal(err)
	}
	if len(cms.Items) != want {
		t.Fatalf("ledger count = %d, want %d", len(cms.Items), want)
	}
	if want == 2 {
		if err := srv.checkAdmissionLedgers(context.Background()); err != nil {
			t.Fatalf("bootstrap contracts invalid: %v", err)
		}
	}
}

func TestAdmissionBootstrapRejectsLegacyStateBeforeAnyWrite(t *testing.T) {
	for _, phase := range []string{"", kovav1.PhaseQueued, kovav1.PhaseRunning, kovav1.PhaseSucceeded} {
		t.Run("build-"+string(phase), func(t *testing.T) {
			build := &kovav1.KovaBuild{ObjectMeta: metav1.ObjectMeta{Name: "legacy", Namespace: "jobs"}, Status: kovav1.KovaBuildStatus{Phase: phase}}
			srv, writer, base := newBootstrapServer(t, testConfig(""), build)
			for attempt := 0; attempt < 2; attempt++ {
				if err := srv.initializeAdmission(context.Background()); err == nil {
					t.Fatal("legacy KovaBuild admitted initial bootstrap")
				}
				assertBootstrapLedgers(t, srv, base, 0)
			}
			if writer.attempts.Load() != 0 {
				t.Fatalf("rejected bootstrap attempted %d writes", writer.attempts.Load())
			}
		})
	}
	t.Run("orphan-runner", func(t *testing.T) {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "legacy-runner", Namespace: "jobs", Labels: map[string]string{"kova.cofy.dev/build-id": "legacy"}}}
		srv, writer, base := newBootstrapServer(t, testConfig(""), pod)
		if err := srv.initializeAdmission(context.Background()); err == nil {
			t.Fatal("orphan runner admitted initial bootstrap")
		}
		assertBootstrapLedgers(t, srv, base, 0)
		if writer.attempts.Load() != 0 {
			t.Fatal("orphan runner preflight wrote a partial ledger")
		}
	})
}

type bootstrapListFailure struct {
	client.Reader
	failPod bool
}

func (r bootstrapListFailure) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	_, isPod := list.(*corev1.PodList)
	if isPod == r.failPod {
		return errors.New("bootstrap namespace observation unavailable")
	}
	return r.Reader.List(ctx, list, opts...)
}

func TestAdmissionBootstrapPreflightsBothContractsWithoutMutation(t *testing.T) {
	for _, mode := range []string{"queue-limits", "active-limits", "build-list", "pod-list"} {
		t.Run(mode, func(t *testing.T) {
			cfg := testConfig("")
			if mode == "queue-limits" {
				cfg.MaxQueuedJobs = 0
			}
			if mode == "active-limits" {
				cfg.MaxActiveJobs = 0
			}
			srv, writer, base := newBootstrapServer(t, cfg)
			if mode == "build-list" || mode == "pod-list" {
				srv.reader = bootstrapListFailure{Reader: base, failPod: mode == "pod-list"}
			}
			if err := srv.initializeAdmission(context.Background()); err == nil {
				t.Fatalf("%s did not reject initial bootstrap", mode)
			}
			assertBootstrapLedgers(t, srv, base, 0)
			if writer.attempts.Load() != 0 {
				t.Fatalf("%s rejection attempted %d writes", mode, writer.attempts.Load())
			}
		})
	}
}

func TestAdmissionBootstrapConcurrentFreshReplicas(t *testing.T) {
	srv, writer, base := newBootstrapServer(t, testConfig(""))
	start := make(chan struct{})
	var wg sync.WaitGroup
	errors := make(chan error, 16)
	for replica := 0; replica < cap(errors); replica++ {
		other := NewServer(srv.cfg, &fakeKube{}, writer, base, nil, nil, nil)
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errors <- other.initializeAdmission(context.Background())
		}()
	}
	close(start)
	wg.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatalf("concurrent bootstrap failed: %v", err)
		}
	}
	assertBootstrapLedgers(t, srv, base, 2)
	if writer.writes.Load() != 2 {
		t.Fatalf("concurrent bootstrap persisted %d writes", writer.writes.Load())
	}
}

type bootstrapPausedReader struct {
	client.Reader
	getMissingActive bool
	once             atomic.Bool
	paused           chan struct{}
	release          chan struct{}
}

func (r *bootstrapPausedReader) pause(ctx context.Context) error {
	close(r.paused)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.release:
		return nil
	}
}

func (r *bootstrapPausedReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	err := r.Reader.Get(ctx, key, obj, opts...)
	if r.getMissingActive && key.Name == buildcontroller.AdmissionLedgerName && apierrors.IsNotFound(err) && r.once.CompareAndSwap(false, true) {
		if pauseErr := r.pause(ctx); pauseErr != nil {
			return pauseErr
		}
	}
	return err
}

func (r *bootstrapPausedReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*kovav1.KovaBuildList); ok && !r.getMissingActive && r.once.CompareAndSwap(false, true) {
		if err := r.pause(ctx); err != nil {
			return err
		}
	}
	return r.Reader.List(ctx, list, opts...)
}

func TestAdmissionBootstrapObservesCompletedConcurrentReplica(t *testing.T) {
	for _, mode := range []string{"initial-get", "preflight-list", "active-create"} {
		t.Run(mode, func(t *testing.T) {
			srv, writer, base := newBootstrapServer(t, testConfig(""))
			paused, release := make(chan struct{}), make(chan struct{})
			var once atomic.Bool
			if mode == "active-create" {
				writer.before = func(ctx context.Context, obj client.Object) error {
					if obj.GetName() == buildcontroller.AdmissionLedgerName && once.CompareAndSwap(false, true) {
						close(paused)
						select {
						case <-ctx.Done():
							return ctx.Err()
						case <-release:
						}
					}
					return nil
				}
			} else {
				srv.reader = &bootstrapPausedReader{Reader: base, getMissingActive: mode == "initial-get", paused: paused, release: release}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- srv.initializeAdmission(ctx) }()
			select {
			case <-paused:
			case <-ctx.Done():
				t.Fatal("bootstrap did not reach deterministic pause")
			}
			other := NewServer(srv.cfg, &fakeKube{}, base, base, nil, nil, nil)
			if err := other.initializeAdmission(ctx); err != nil {
				t.Fatal(err)
			}
			// A valid first-start replica can accept direct/admin CRs after
			// readiness, even while another replica's initial GET is paused.
			if err := base.Create(ctx, &kovav1.KovaBuild{ObjectMeta: metav1.ObjectMeta{Name: "new", Namespace: "jobs"}}); err != nil {
				t.Fatal(err)
			}
			close(release)
			if err := <-result; err != nil {
				t.Fatalf("completed concurrent replica was mistaken for legacy state: %v", err)
			}
			assertBootstrapLedgers(t, srv, base, 2)
			if writer.writes.Load() != 0 {
				t.Fatalf("late replica persisted %d extra ledgers", writer.writes.Load())
			}
		})
	}
}

func TestAdmissionBootstrapRecoversOnlyObservedCommittedCreate(t *testing.T) {
	for _, name := range []string{buildcontroller.AdmissionLedgerName, queueadmission.ConfigMapName} {
		t.Run(name, func(t *testing.T) {
			srv, writer, base := newBootstrapServer(t, testConfig(""))
			var lost atomic.Bool
			writer.after = func(obj client.Object) error {
				if obj.GetName() == name && lost.CompareAndSwap(false, true) {
					return context.DeadlineExceeded
				}
				return nil
			}
			if err := srv.initializeAdmission(context.Background()); err != nil {
				t.Fatalf("observed committed %s was not recovered: %v", name, err)
			}
			assertBootstrapLedgers(t, srv, base, 2)
			if !lost.Load() || writer.writes.Load() != 2 {
				t.Fatalf("lost-response injection missing or bootstrap wrote replacements: lost=%t writes=%d", lost.Load(), writer.writes.Load())
			}
		})
	}
}

func TestAdmissionBootstrapRetryNeverRepairsPartialPair(t *testing.T) {
	for _, name := range []string{buildcontroller.AdmissionLedgerName, queueadmission.ConfigMapName} {
		t.Run(name, func(t *testing.T) {
			srv, writer, base := newBootstrapServer(t, testConfig(""))
			var failed atomic.Bool
			writer.before = func(_ context.Context, obj client.Object) error {
				if obj.GetName() == name && failed.CompareAndSwap(false, true) {
					return fmt.Errorf("injected unobserved %s Create failure", name)
				}
				return nil
			}
			if err := srv.initializeAdmission(context.Background()); err == nil {
				t.Fatal("uncommitted Create failure was reported ready")
			}
			if name == buildcontroller.AdmissionLedgerName {
				assertBootstrapLedgers(t, srv, base, 0)
				if err := srv.initializeAdmission(context.Background()); err != nil {
					t.Fatalf("still-fresh namespace could not retry: %v", err)
				}
				assertBootstrapLedgers(t, srv, base, 2)
				return
			}
			assertBootstrapLedgers(t, srv, base, 1)
			before := writer.attempts.Load()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
			defer cancel()
			if err := srv.initializeAdmission(ctx); err == nil {
				t.Fatal("restart repaired a partial ledger pair")
			}
			if writer.attempts.Load() != before {
				t.Fatal("restart attempted to repair the absent queue")
			}
			assertBootstrapLedgers(t, srv, base, 1)
		})
	}
}

func TestAdmissionBootstrapLateReplicaDoesNotRecreateLostPeerQueue(t *testing.T) {
	srv, writer, base := newBootstrapServer(t, testConfig(""))
	paused, release := make(chan struct{}), make(chan struct{})
	var once atomic.Bool
	writer.before = func(ctx context.Context, obj client.Object) error {
		if obj.GetName() == buildcontroller.AdmissionLedgerName && once.CompareAndSwap(false, true) {
			close(paused)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-release:
			}
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- srv.initializeAdmission(ctx) }()
	select {
	case <-paused:
	case <-ctx.Done():
		t.Fatal("bootstrap did not pause before active Create")
	}
	other := NewServer(srv.cfg, &fakeKube{}, base, base, nil, nil, nil)
	if err := other.initializeAdmission(ctx); err != nil {
		t.Fatal(err)
	}
	deleteReadinessLedger(t, other, queueadmission.ConfigMapName)
	close(release)
	if err := <-result; err == nil {
		t.Fatal("a previously fresh observer recreated its running peer's lost queue")
	}
	assertBootstrapLedgers(t, srv, base, 1)
	if writer.writes.Load() != 0 || writer.attempts.Load() != 1 {
		t.Fatalf("late observer attempted queue repair: attempts=%d writes=%d", writer.attempts.Load(), writer.writes.Load())
	}
}

func TestAdmissionBootstrapLostCreateCannotClaimPeersNonce(t *testing.T) {
	srv, writer, base := newBootstrapServer(t, testConfig(""))
	writer.before = func(ctx context.Context, obj client.Object) error {
		if obj.GetName() != buildcontroller.AdmissionLedgerName {
			t.Error("lost active response authorized queue creation")
			return errors.New("unauthorized queue create")
		}
		created, err := buildcontroller.EnsureAdmissionLedger(ctx, base, base, "jobs", srv.cfg)
		if err != nil || !created {
			return fmt.Errorf("peer bootstrap failed: created=%t error=%v", created, err)
		}
		return context.DeadlineExceeded
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := srv.initializeAdmission(ctx); err == nil {
		t.Fatal("read of a peer's active ledger authorized missing queue creation")
	}
	assertBootstrapLedgers(t, srv, base, 1)
	if writer.attempts.Load() != 1 || writer.writes.Load() != 0 {
		t.Fatalf("peer nonce claimed: attempts=%d writes=%d", writer.attempts.Load(), writer.writes.Load())
	}
}
