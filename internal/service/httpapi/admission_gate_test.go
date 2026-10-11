package httpapi

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cofy-x/kova/internal/admissioncontract"
	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/queueadmission"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// These transports share one concurrency-safe fake API, while each Server
// retains its own local gate. They count attempted side effects, not just
// objects that happened to remain visible after a response.
type gateAuthAPI struct {
	client.Client
	queuePatches atomic.Int32
	beforePatch  func(context.Context, int32) error
}

func (a *gateAuthAPI) GetNamespace(ctx context.Context, name string) (*corev1.Namespace, error) {
	var namespace corev1.Namespace
	err := a.Client.Get(ctx, client.ObjectKey{Name: name}, &namespace)
	return &namespace, err
}

func (a *gateAuthAPI) GetConfigMap(ctx context.Context, namespace, name string) (*corev1.ConfigMap, error) {
	var cm corev1.ConfigMap
	err := a.Client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &cm)
	return &cm, err
}

func (a *gateAuthAPI) CreateConfigMap(ctx context.Context, _ string, cm *corev1.ConfigMap) (*corev1.ConfigMap, error) {
	if err := a.Client.Create(ctx, cm); err != nil {
		return nil, err
	}
	return cm, nil
}

func (a *gateAuthAPI) PatchConfigMap(ctx context.Context, namespace, name string, body []byte) (*corev1.ConfigMap, error) {
	if name == admissioncontract.QueueLedgerName {
		attempt := a.queuePatches.Add(1)
		if a.beforePatch != nil {
			if err := a.beforePatch(ctx, attempt); err != nil {
				return nil, err
			}
		}
	}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
	if err := a.Client.Patch(ctx, cm, client.RawPatch(types.JSONPatchType, body)); err != nil {
		return nil, err
	}
	return cm, nil
}

type gateReceipts struct {
	client.Client
	creates      atomic.Int32
	beforeCreate func(context.Context, int32) error
}

func (r *gateReceipts) Create(ctx context.Context, cm *corev1.ConfigMap, _ metav1.CreateOptions) (*corev1.ConfigMap, error) {
	attempt := r.creates.Add(1)
	if r.beforeCreate != nil {
		if err := r.beforeCreate(ctx, attempt); err != nil {
			return nil, err
		}
	}
	cm.UID = types.UID(fmt.Sprintf("queue-receipt-%d", attempt))
	if err := r.Client.Create(ctx, cm); err != nil {
		return nil, err
	}
	return r.Get(ctx, cm.Name, metav1.GetOptions{})
}

func (r *gateReceipts) Get(ctx context.Context, name string, _ metav1.GetOptions) (*corev1.ConfigMap, error) {
	var cm corev1.ConfigMap
	err := r.Client.Get(ctx, client.ObjectKey{Namespace: "receipts-http", Name: name}, &cm)
	return &cm, err
}

type gateBuildWriter struct {
	client.Client
	mu           sync.Mutex
	byName       map[string]int
	creates      int
	beforeCreate func(context.Context, int) error
}

func (w *gateBuildWriter) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	build, ok := obj.(*kovav1.KovaBuild)
	if !ok {
		return w.Client.Create(ctx, obj, opts...)
	}
	w.mu.Lock()
	w.creates++
	w.byName[build.Name]++
	attempt := w.creates
	w.mu.Unlock()
	if w.beforeCreate != nil {
		if err := w.beforeCreate(ctx, attempt); err != nil {
			return err
		}
	}
	build.UID = types.UID(fmt.Sprintf("accepted-build-%d", attempt))
	return w.Client.Create(ctx, build, opts...)
}

func (w *gateBuildWriter) snapshot() (int, map[string]int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	byName := make(map[string]int, len(w.byName))
	for name, count := range w.byName {
		byName[name] = count
	}
	return w.creates, byName
}

func dualGenesisGateServers(t *testing.T) ([2]*Server, client.Client, *gateAuthAPI, *gateReceipts, *gateBuildWriter) {
	t.Helper()
	first, base, _, _, _ := newGenesisReceiptHTTPServer(t)
	authAPI := &gateAuthAPI{Client: base}
	guard := *first.genesisLedger
	guard.Bootstrap.API = authAPI
	if err := first.WithGenesisGuards(&guard, &guard); err != nil {
		t.Fatal(err)
	}
	receipts := &gateReceipts{Client: base}
	if err := first.WithRecoveryReceipts(receipts); err != nil {
		t.Fatal(err)
	}
	writer := &gateBuildWriter{Client: base, byName: make(map[string]int)}
	first.client, first.reader = writer, base
	second := NewServer(first.cfg, first.kube, writer, base, nil, first.auth, first.authz)
	if err := second.WithGenesisGuards(&guard, &guard); err != nil {
		t.Fatal(err)
	}
	if err := second.WithRecoveryReceipts(receipts); err != nil {
		t.Fatal(err)
	}
	return [2]*Server{first, second}, base, authAPI, receipts, writer
}

type gateHTTPResult struct {
	key string
	rec *httptest.ResponseRecorder
}

func startGatePost(srv *Server, key string, request *http.Request) <-chan gateHTTPResult {
	result := make(chan gateHTTPResult, 1)
	handler := srv.routes()
	go func() {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, request)
		result <- gateHTTPResult{key: key, rec: rec}
	}()
	return result
}

func awaitGateResult(t *testing.T, result <-chan gateHTTPResult) gateHTTPResult {
	t.Helper()
	select {
	case got := <-result:
		return got
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for HTTP admission result")
		return gateHTTPResult{}
	}
}

func waitGateWaiters(t *testing.T, srv *Server, want int32) {
	t.Helper()
	deadline := time.After(10 * time.Second)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if srv.admissionGate.waiters.Load() == want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("local gate waiters=%d, want=%d", srv.admissionGate.waiters.Load(), want)
		case <-tick.C:
		}
	}
}

func assertGateBuilds(t *testing.T, srv *Server, base client.Client, receipts *gateReceipts, writer *gateBuildWriter, results []gateHTTPResult) {
	t.Helper()
	ctx := context.Background()
	creates, byName := writer.snapshot()
	if creates != len(results) || int(receipts.creates.Load()) != len(results) {
		t.Fatalf("side-effect attempts: CR=%d receipts=%d results=%d", creates, receipts.creates.Load(), len(results))
	}
	for _, result := range results {
		if result.rec.Code != http.StatusAccepted {
			t.Fatalf("%s: status=%d body=%s", result.key, result.rec.Code, result.rec.Body.String())
		}
		id := result.rec.Header().Get("X-Kova-Build-ID")
		if id != idempotentJobID("test-user", result.key) || byName[id] != 1 {
			t.Fatalf("%s: id=%q CR attempts=%d", result.key, id, byName[id])
		}
		entry, found, err := srv.queueStore().Lookup(ctx, id)
		if err != nil || !found || entry.ReceiptUID == "" || entry.ReceiptDigest == "" {
			t.Fatalf("%s: unpinned queue intent: entry=%#v found=%t err=%v", result.key, entry, found, err)
		}
		receipt, err := receipts.Get(ctx, "kova-admission-intent-"+entry.Nonce, metav1.GetOptions{})
		if err != nil || string(receipt.UID) != entry.ReceiptUID {
			t.Fatalf("%s: receipt UID mismatch: receipt=%#v err=%v", result.key, receipt, err)
		}
		var build kovav1.KovaBuild
		if err := base.Get(ctx, client.ObjectKey{Namespace: srv.cfg.Namespace, Name: id}, &build); err != nil {
			t.Fatal(err)
		}
		if build.Annotations[queueadmission.ReceiptUIDAnnotation] != entry.ReceiptUID ||
			build.Annotations[queueadmission.ReceiptDigestAnnotation] != entry.ReceiptDigest {
			t.Fatalf("%s: CR does not carry pinned receipt identity", result.key)
		}
	}
}

func TestGenesisLocalAdmissionGateBoundsBurstAndCanceledWaiter(t *testing.T) {
	servers, base, authAPI, receipts, writer := dualGenesisGateServers(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	receipts.beforeCreate = func(ctx context.Context, attempt int32) error {
		if attempt != 1 {
			return nil
		}
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	first := startGatePost(servers[0], "gate-0", queueRequest(t, "gate-0"))
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("first local receipt Create did not reach barrier")
	}
	local := startGatePost(servers[0], "gate-1", queueRequest(t, "gate-1"))
	waitGateWaiters(t, servers[0], 1)
	if authAPI.queuePatches.Load() != 1 || receipts.creates.Load() != 1 {
		t.Fatalf("same-Server waiter attempted a write: queue patches=%d receipts=%d", authAPI.queuePatches.Load(), receipts.creates.Load())
	}

	// The other Server has its own gate and must progress while this Server's
	// first receipt Create is stalled.
	other := awaitGateResult(t, startGatePost(servers[1], "gate-4", queueRequest(t, "gate-4")))
	if other.rec.Code != http.StatusAccepted {
		t.Fatalf("other Server stalled: status=%d body=%s", other.rec.Code, other.rec.Body.String())
	}
	patchesBeforeCancel, receiptsBeforeCancel := authAPI.queuePatches.Load(), receipts.creates.Load()
	cancelCtx, cancel := context.WithCancel(context.Background())
	canceledRequest := queueRequest(t, "gate-canceled").WithContext(cancelCtx)
	canceled := startGatePost(servers[0], "gate-canceled", canceledRequest)
	waitGateWaiters(t, servers[0], 2)
	cancel()
	canceledResult := awaitGateResult(t, canceled)
	if canceledResult.rec.Code != http.StatusServiceUnavailable ||
		authAPI.queuePatches.Load() != patchesBeforeCancel || receipts.creates.Load() != receiptsBeforeCancel {
		t.Fatalf("canceled gate waiter wrote or succeeded: code=%d patches=%d receipts=%d", canceledResult.rec.Code,
			authAPI.queuePatches.Load(), receipts.creates.Load())
	}
	if _, found, err := servers[0].queueStore().Lookup(context.Background(), idempotentJobID("test-user", "gate-canceled")); err != nil || found {
		t.Fatalf("canceled waiter left queue intent: found=%t err=%v", found, err)
	}

	var results []gateHTTPResult
	results = append(results, other)
	var localPending [2]<-chan gateHTTPResult
	for i := 2; i < 4; i++ {
		key := fmt.Sprintf("gate-%d", i)
		localPending[i-2] = startGatePost(servers[0], key, queueRequest(t, key))
	}
	waitGateWaiters(t, servers[0], 3)
	var remotePending [3]<-chan gateHTTPResult
	for i := 5; i < 8; i++ {
		key := fmt.Sprintf("gate-%d", i)
		remotePending[i-5] = startGatePost(servers[1], key, queueRequest(t, key))
	}
	for _, pending := range remotePending {
		results = append(results, awaitGateResult(t, pending))
	}
	releaseOnce.Do(func() { close(release) })
	results = append(results, awaitGateResult(t, first), awaitGateResult(t, local))
	for _, pending := range localPending {
		results = append(results, awaitGateResult(t, pending))
	}
	assertGateBuilds(t, servers[0], base, receipts, writer, results)
	if got := authAPI.queuePatches.Load(); got != 16 {
		t.Fatalf("serialized eight-build burst used %d queue PATCH attempts, want 16", got)
	}
}

func TestGenesisLocalAdmissionGatePreservesCrossServerStale422Readback(t *testing.T) {
	servers, base, authAPI, receipts, writer := dualGenesisGateServers(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	var stale422s atomic.Int32
	authAPI.beforePatch = func(ctx context.Context, attempt int32) error {
		if attempt != 1 {
			return nil
		}
		close(entered)
		select {
		case <-release:
			stale422s.Add(1)
			return apierrors.NewGenericServerResponse(http.StatusUnprocessableEntity, "patch",
				schema.GroupResource{Resource: "configmaps"}, admissioncontract.QueueLedgerName, "stale JSON Patch test", 0, false)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	first := startGatePost(servers[0], "stale-a", queueRequest(t, "stale-a"))
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("first Server did not reach stale Patch barrier")
	}
	second := awaitGateResult(t, startGatePost(servers[1], "stale-b", queueRequest(t, "stale-b")))
	if second.rec.Code != http.StatusAccepted {
		t.Fatalf("other Server did not advance original queue: code=%d body=%s", second.rec.Code, second.rec.Body.String())
	}
	releaseOnce.Do(func() { close(release) })
	result := awaitGateResult(t, first)
	assertGateBuilds(t, servers[0], base, receipts, writer, []gateHTTPResult{second, result})
	if stale422s.Load() != 1 || authAPI.queuePatches.Load() != 5 {
		t.Fatalf("stale 422 was not read back and retried once: stale=%d patches=%d", stale422s.Load(), authAPI.queuePatches.Load())
	}
}

func TestGenesisLocalAdmissionGateUnknownReleasesPermitButRetainsCharge(t *testing.T) {
	for _, mode := range []string{"receipt", "pin"} {
		t.Run(mode, func(t *testing.T) {
			servers, base, authAPI, receipts, writer := dualGenesisGateServers(t)
			if mode == "receipt" {
				receipts.beforeCreate = func(_ context.Context, attempt int32) error {
					if attempt == 1 {
						return context.DeadlineExceeded
					}
					return nil
				}
			} else {
				authAPI.beforePatch = func(_ context.Context, attempt int32) error {
					if attempt == 2 {
						return context.DeadlineExceeded
					}
					return nil
				}
			}
			first := awaitGateResult(t, startGatePost(servers[0], "unknown-first", queueRequest(t, "unknown-first")))
			firstID := idempotentJobID("test-user", first.key)
			if first.rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("unknown %s outcome: code=%d body=%s", mode, first.rec.Code, first.rec.Body.String())
			}
			if _, found, err := servers[0].queueStore().Lookup(context.Background(), firstID); err != nil || !found {
				t.Fatalf("unknown %s lost durable charge: found=%t err=%v", mode, found, err)
			}
			var firstBuild kovav1.KovaBuild
			if err := base.Get(context.Background(), client.ObjectKey{Namespace: servers[0].cfg.Namespace, Name: firstID}, &firstBuild); !apierrors.IsNotFound(err) {
				t.Fatalf("unknown %s issued CR Create: %v", mode, err)
			}
			// This request must acquire the released local permit, without
			// reissuing the first request's unknown receipt or pin operation.
			second := awaitGateResult(t, startGatePost(servers[0], "after-unknown", queueRequest(t, "after-unknown")))
			if second.rec.Code != http.StatusAccepted {
				t.Fatalf("unknown %s retained local permit: code=%d body=%s", mode, second.rec.Code, second.rec.Body.String())
			}
			if _, found, err := servers[0].queueStore().Lookup(context.Background(), firstID); err != nil || !found {
				t.Fatalf("next request released unknown %s charge: found=%t err=%v", mode, found, err)
			}
			creates, byName := writer.snapshot()
			if creates != 1 || byName[firstID] != 0 || byName[idempotentJobID("test-user", second.key)] != 1 || receipts.creates.Load() != 2 {
				t.Fatalf("unknown %s reissued an effect: CR=%d perName=%v receipt attempts=%d", mode, creates, byName, receipts.creates.Load())
			}
		})
	}
}

func TestGenesisLocalAdmissionGateReleasesBeforeCRCreate(t *testing.T) {
	servers, base, authAPI, receipts, writer := dualGenesisGateServers(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	writer.beforeCreate = func(ctx context.Context, attempt int) error {
		if attempt != 1 {
			return nil
		}
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	first := startGatePost(servers[0], "create-held", queueRequest(t, "create-held"))
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("first CR Create did not reach barrier")
	}
	second := awaitGateResult(t, startGatePost(servers[0], "after-create-held", queueRequest(t, "after-create-held")))
	if second.rec.Code != http.StatusAccepted || authAPI.queuePatches.Load() != 4 || receipts.creates.Load() != 2 {
		t.Fatalf("CR Create held local gate: code=%d patches=%d receipts=%d body=%s", second.rec.Code,
			authAPI.queuePatches.Load(), receipts.creates.Load(), second.rec.Body.String())
	}
	releaseOnce.Do(func() { close(release) })
	assertGateBuilds(t, servers[0], base, receipts, writer, []gateHTTPResult{awaitGateResult(t, first), second})
}

// The second Err check models cancellation after the token is received but
// before acquire returns. The token must be returned for the next request.
type cancelOnSecondErr struct {
	context.Context
	checks atomic.Int32
}

func (c *cancelOnSecondErr) Err() error {
	if c.checks.Add(1) >= 2 {
		return context.Canceled
	}
	return nil
}

func TestLocalAdmissionGateZeroValueAndCancellationAtAcquisition(t *testing.T) {
	var gate localAdmissionGate
	alreadyCanceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := gate.acquire(alreadyCanceled); err != context.Canceled {
		t.Fatalf("already canceled acquire returned %v", err)
	}

	const arrivals = 8
	start := make(chan struct{})
	var wg sync.WaitGroup
	var admitted atomic.Int32
	for i := 0; i < arrivals; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if err := gate.acquire(context.Background()); err != nil {
				t.Errorf("zero-value gate acquisition: %v", err)
				return
			}
			admitted.Add(1)
			gate.release()
		}()
	}
	close(start)
	wg.Wait()
	if admitted.Load() != arrivals {
		t.Fatalf("zero-value concurrent initialization admitted %d/%d", admitted.Load(), arrivals)
	}

	if err := gate.acquire(&cancelOnSecondErr{Context: context.Background()}); err != context.Canceled {
		t.Fatalf("cancellation racing with token acquisition returned %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := gate.acquire(ctx); err != nil {
		t.Fatalf("racing cancellation lost gate token: %v", err)
	}
	gate.release()
}
