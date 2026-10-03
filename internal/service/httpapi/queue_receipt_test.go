package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cofy-x/kova/internal/admissioncontract"
	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/admissiongenesis"
	serviceauth "github.com/cofy-x/kova/internal/service/auth"
	"github.com/cofy-x/kova/internal/service/buildcontroller"
	"github.com/cofy-x/kova/internal/service/queueadmission"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type receiptHTTPAuthAPI struct {
	client.Client
	queuePatches int
	failPin      bool
	losePinReply bool
}

func (a *receiptHTTPAuthAPI) GetNamespace(ctx context.Context, name string) (*corev1.Namespace, error) {
	var namespace corev1.Namespace
	err := a.Client.Get(ctx, client.ObjectKey{Name: name}, &namespace)
	return &namespace, err
}

func (a *receiptHTTPAuthAPI) GetConfigMap(ctx context.Context, namespace, name string) (*corev1.ConfigMap, error) {
	var cm corev1.ConfigMap
	err := a.Client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &cm)
	return &cm, err
}

func (a *receiptHTTPAuthAPI) CreateConfigMap(ctx context.Context, _ string, cm *corev1.ConfigMap) (*corev1.ConfigMap, error) {
	err := a.Client.Create(ctx, cm)
	return cm, err
}

func (a *receiptHTTPAuthAPI) PatchConfigMap(ctx context.Context, namespace, name string, body []byte) (*corev1.ConfigMap, error) {
	if name == admissioncontract.QueueLedgerName {
		a.queuePatches++
		if a.failPin && a.queuePatches == 2 {
			return nil, context.DeadlineExceeded
		}
	}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
	err := a.Client.Patch(ctx, cm, client.RawPatch(types.JSONPatchType, body))
	if err == nil && name == admissioncontract.QueueLedgerName && a.losePinReply && a.queuePatches == 2 {
		return nil, context.DeadlineExceeded
	}
	return cm, err
}

type receiptHTTPConfigMaps struct {
	client.Client
	creates    int
	failCreate bool
}

func (a *receiptHTTPConfigMaps) Create(ctx context.Context, cm *corev1.ConfigMap, _ metav1.CreateOptions) (*corev1.ConfigMap, error) {
	a.creates++
	if a.failCreate {
		return nil, context.DeadlineExceeded
	}
	cm.UID = types.UID(fmt.Sprintf("queue-receipt-%d", a.creates))
	if err := a.Client.Create(ctx, cm); err != nil {
		return nil, err
	}
	return a.Get(ctx, cm.Name, metav1.GetOptions{})
}

func (a *receiptHTTPConfigMaps) Get(ctx context.Context, name string, _ metav1.GetOptions) (*corev1.ConfigMap, error) {
	var cm corev1.ConfigMap
	err := a.Client.Get(ctx, client.ObjectKey{Namespace: "receipts-http", Name: name}, &cm)
	return &cm, err
}

type receiptHTTPWriter struct {
	client.Client
	crCreates int
	crError   error
	beforeCR  func(context.Context, *kovav1.KovaBuild) error
}

type oneMissBuildReader struct {
	client.Reader
	name   string
	missed bool
}

func (r *oneMissBuildReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*kovav1.KovaBuild); ok && key.Name == r.name && !r.missed {
		r.missed = true
		return apierrors.NewNotFound(schema.GroupResource{Group: kovav1.Group, Resource: "kovabuilds"}, key.Name)
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

func (w *receiptHTTPWriter) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	build, ok := obj.(*kovav1.KovaBuild)
	if !ok {
		return w.Client.Create(ctx, obj, opts...)
	}
	w.crCreates++
	if w.beforeCR != nil {
		if err := w.beforeCR(ctx, build); err != nil {
			return err
		}
	}
	if w.crError != nil {
		return w.crError
	}
	build.UID = "accepted-build-original"
	return w.Client.Create(ctx, build, opts...)
}

func newGenesisReceiptHTTPServer(t *testing.T) (*Server, client.Client, *receiptHTTPAuthAPI, *receiptHTTPConfigMaps, *receiptHTTPWriter) {
	t.Helper()
	ctx := context.Background()
	cfg := testConfig(t.TempDir())
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := kovav1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	receipt := admissioncontract.Receipt{Namespace: cfg.Namespace, GenesisName: admissioncontract.GenesisName,
		GenesisUID: "genesis-http-original", Contract: admissioncontract.Contract{
			Version: 3, NamespaceUID: "runner-http-original", ReceiptNamespace: "receipts-http",
			ReceiptNamespaceUID: "receipt-namespace-original", Generation: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			WorkerPoolID: cfg.WorkerPoolID, RunnerImage: cfg.RunnerImage,
			ActiveLedgerName: admissioncontract.ActiveLedgerName, ActiveLedgerSchema: 2,
			QueueLedgerName: admissioncontract.QueueLedgerName, QueueLedgerSchema: 2,
			Limits: admissioncontract.Limits{MaxActiveJobs: cfg.MaxActiveJobs, MaxActiveJobsPerRequester: cfg.MaxActiveJobsPerRequester,
				WorkerSlots: cfg.WorkerSlots, MaxQueuedJobs: cfg.MaxQueuedJobs, MaxQueuedJobsPerRequester: cfg.MaxQueuedJobsPerRequester},
		}}
	activeData, err := buildcontroller.GenesisEmptyAdmissionData(cfg)
	if err != nil {
		t.Fatal(err)
	}
	queueData, err := queueadmission.GenesisEmptyQueueData(cfg.MaxQueuedJobs, cfg.MaxQueuedJobsPerRequester)
	if err != nil {
		t.Fatal(err)
	}
	active, err := receipt.NewLedgerObject(admissioncontract.Active, admissioncontract.ActiveLedgerDataKey, activeData, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	if err != nil {
		t.Fatal(err)
	}
	queue, err := receipt.NewLedgerObject(admissioncontract.Queue, admissioncontract.QueueLedgerDataKey, queueData, "cccccccccccccccccccccccccccccccc")
	if err != nil {
		t.Fatal(err)
	}
	active.UID, active.ResourceVersion = "active-http-original", "11"
	queue.UID, queue.ResourceVersion = "queue-http-original", "12"
	genesisData, err := json.Marshal(admissioncontract.GenesisData{Contract: receipt.Contract, Phase: admissioncontract.PhaseCommitted,
		ActiveLedgerUID: string(active.UID), QueueLedgerUID: string(queue.UID)})
	if err != nil {
		t.Fatal(err)
	}
	immutable := true
	genesis := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: cfg.Namespace, Name: receipt.GenesisName,
		UID: types.UID(receipt.GenesisUID), ResourceVersion: "13"}, Immutable: &immutable,
		Data: map[string]string{admissioncontract.GenesisDataKey: string(genesisData)}}
	runnerNS := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: cfg.Namespace, UID: types.UID(receipt.Contract.NamespaceUID)},
		Status: corev1.NamespaceStatus{Phase: corev1.NamespaceActive}}
	receiptNS := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: receipt.Contract.ReceiptNamespace,
		UID: types.UID(receipt.Contract.ReceiptNamespaceUID)}, Status: corev1.NamespaceStatus{Phase: corev1.NamespaceActive}}
	base := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).
		WithObjects(runnerNS, receiptNS, genesis, active, queue).Build()
	authAPI := &receiptHTTPAuthAPI{Client: base}
	bootstrap := admissiongenesis.Bootstrapper{API: authAPI, Receipt: receipt,
		Active: admissiongenesis.LedgerTemplate{Role: admissioncontract.Active, DataKey: admissioncontract.ActiveLedgerDataKey,
			EmptyData: activeData, Validate: func(cm *corev1.ConfigMap) error { return buildcontroller.ValidateAdmissionLedgerForGenesis(cm, cfg) }},
		Queue: admissiongenesis.LedgerTemplate{Role: admissioncontract.Queue, DataKey: admissioncontract.QueueLedgerDataKey,
			EmptyData: queueData, Validate: func(cm *corev1.ConfigMap) error {
				return queueadmission.ValidateQueueLedgerForGenesis(cm, cfg.MaxQueuedJobs, cfg.MaxQueuedJobsPerRequester)
			}},
		Preflight: func(context.Context) error { return nil }}
	binding := admissiongenesis.Binding{NamespaceUID: receipt.Contract.NamespaceUID, GenesisUID: receipt.GenesisUID,
		ActiveLedgerUID: string(active.UID), QueueLedgerUID: string(queue.UID)}
	guard, err := admissiongenesis.NewGuard(ctx, bootstrap, binding)
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := serviceauth.New(serviceauth.ModeStatic, "token", "test-user", nil)
	if err != nil {
		t.Fatal(err)
	}
	writer := &receiptHTTPWriter{Client: base}
	srv := NewServer(cfg, &fakeKube{}, writer, base, nil, authenticator, serviceauth.AllowAllAuthorizer{})
	if err := srv.WithGenesisGuards(guard, guard); err != nil {
		t.Fatal(err)
	}
	receipts := &receiptHTTPConfigMaps{Client: base}
	if err := srv.WithRecoveryReceipts(receipts); err != nil {
		t.Fatal(err)
	}
	return srv, base, authAPI, receipts, writer
}

func postGenesisReceiptBuild(t *testing.T, srv *Server, key string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	srv.routes().ServeHTTP(rec, queueRequest(t, key))
	return rec
}

func TestGenesisHTTPReceiptBeforeSoleCRCreateAndOrdinarySettlement(t *testing.T) {
	ctx := context.Background()
	srv, base, _, receipts, writer := newGenesisReceiptHTTPServer(t)
	observedBeforeCR := false
	writer.beforeCR = func(ctx context.Context, build *kovav1.KovaBuild) error {
		entry, found, err := srv.queueStore().Lookup(ctx, build.Name)
		if err != nil || !found || entry.ReceiptUID == "" || entry.ReceiptDigest == "" ||
			build.Annotations[queueadmission.ReceiptUIDAnnotation] != entry.ReceiptUID ||
			build.Annotations[queueadmission.ReceiptDigestAnnotation] != entry.ReceiptDigest {
			return fmt.Errorf("CR Create preceded durable exact queue pin: entry=%#v found=%t err=%v", entry, found, err)
		}
		cm, err := receipts.Get(ctx, "kova-admission-intent-"+entry.Nonce, metav1.GetOptions{})
		if err != nil || string(cm.UID) != entry.ReceiptUID {
			return fmt.Errorf("CR Create preceded exact receipt readback: receipt=%#v err=%v", cm, err)
		}
		observedBeforeCR = true
		return nil
	}
	first := postGenesisReceiptBuild(t, srv, "accepted")
	if first.Code != http.StatusAccepted || !observedBeforeCR || writer.crCreates != 1 || receipts.creates != 1 {
		t.Fatalf("accepted path: code=%d before=%t CR=%d receipts=%d body=%s", first.Code, observedBeforeCR, writer.crCreates, receipts.creates, first.Body.String())
	}
	id := first.Header().Get("X-Kova-Build-ID")
	second := postGenesisReceiptBuild(t, srv, "accepted")
	if second.Code != http.StatusOK || writer.crCreates != 1 || receipts.creates != 1 {
		t.Fatalf("duplicate reissued Create: code=%d CR=%d receipts=%d", second.Code, writer.crCreates, receipts.creates)
	}
	entry, found, err := srv.queueStore().Lookup(ctx, id)
	if err != nil || !found || entry.ReceiptUID == "" {
		t.Fatalf("active HTTP build lost queue charge: entry=%#v found=%t err=%v", entry, found, err)
	}
	var build kovav1.KovaBuild
	if err := base.Get(ctx, client.ObjectKey{Namespace: srv.cfg.Namespace, Name: id}, &build); err != nil {
		t.Fatal(err)
	}
	build.Status.Phase = kovav1.PhaseSucceeded
	if err := base.Status().Update(ctx, &build); err != nil {
		t.Fatal(err)
	}
	if err := srv.queueStore().ReleaseForBuild(ctx, &build); err != nil {
		t.Fatalf("ordinary terminal receipt cleanup failed: %v", err)
	}
	if _, found, err := srv.queueStore().Lookup(ctx, id); err != nil || found {
		t.Fatalf("settled queue charge remains: found=%t err=%v", found, err)
	}
	if _, err := receipts.Get(ctx, "kova-admission-intent-"+entry.Nonce, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("settled exact receipt remains: %v", err)
	}
	third := postGenesisReceiptBuild(t, srv, "accepted")
	if third.Code != http.StatusOK || writer.crCreates != 1 || receipts.creates != 1 {
		t.Fatalf("settled terminal replay was not read-only: code=%d CR=%d receipts=%d body=%s", third.Code, writer.crCreates, receipts.creates, third.Body.String())
	}
}

func TestGenesisHTTPReplayDoesNotDescribeUnreceiptedOrMismatchedCRAsAccepted(t *testing.T) {
	ctx := context.Background()
	srv, base, _, receipts, writer := newGenesisReceiptHTTPServer(t)
	first := postGenesisReceiptBuild(t, srv, "source")
	if first.Code != http.StatusAccepted {
		t.Fatalf("source fixture: code=%d body=%s", first.Code, first.Body.String())
	}
	var source kovav1.KovaBuild
	if err := base.Get(ctx, client.ObjectKey{Namespace: srv.cfg.Namespace, Name: first.Header().Get("X-Kova-Build-ID")}, &source); err != nil {
		t.Fatal(err)
	}
	foreign := source.DeepCopy()
	foreign.Name = idempotentJobID("test-user", "foreign")
	foreign.UID, foreign.ResourceVersion = "foreign-direct-uid", ""
	foreign.Spec.IdempotencyKey = "foreign"
	foreign.Annotations = nil
	if err := base.Create(ctx, foreign); err != nil {
		t.Fatal(err)
	}
	foreignReplay := postGenesisReceiptBuild(t, srv, "foreign")
	if foreignReplay.Code != http.StatusConflict || writer.crCreates != 1 || receipts.creates != 1 {
		t.Fatalf("direct/admin same-name CR was described as receipted: code=%d CR=%d receipts=%d body=%s", foreignReplay.Code, writer.crCreates, receipts.creates, foreignReplay.Body.String())
	}
	source.Annotations[queueadmission.ReceiptUIDAnnotation] = "mismatched-receipt-uid"
	if err := base.Update(ctx, &source); err != nil {
		t.Fatal(err)
	}
	mismatchReplay := postGenesisReceiptBuild(t, srv, "source")
	if mismatchReplay.Code != http.StatusServiceUnavailable || writer.crCreates != 1 || receipts.creates != 1 {
		t.Fatalf("mismatched queue link was described as accepted: code=%d CR=%d receipts=%d body=%s", mismatchReplay.Code, writer.crCreates, receipts.creates, mismatchReplay.Body.String())
	}
	srv.reader = &oneMissBuildReader{Reader: base, name: source.Name}
	raceReplay := postGenesisReceiptBuild(t, srv, "source")
	if raceReplay.Code != http.StatusServiceUnavailable || writer.crCreates != 1 || receipts.creates != 1 {
		t.Fatalf("post-reserve existing CR bypassed receipt check: code=%d CR=%d receipts=%d body=%s", raceReplay.Code, writer.crCreates, receipts.creates, raceReplay.Body.String())
	}
}

func TestGenesisHTTPAlreadyExistsDoesNotAcceptForeignCR(t *testing.T) {
	ctx := context.Background()
	srv, base, _, receipts, writer := newGenesisReceiptHTTPServer(t)
	// Build the exact request/spec through a normal accepted submission, then
	// place a separate direct/admin CR at the next idempotency name.
	first := postGenesisReceiptBuild(t, srv, "source")
	if first.Code != http.StatusAccepted {
		t.Fatalf("source fixture: code=%d body=%s", first.Code, first.Body.String())
	}
	var source kovav1.KovaBuild
	if err := base.Get(ctx, client.ObjectKey{Namespace: srv.cfg.Namespace, Name: first.Header().Get("X-Kova-Build-ID")}, &source); err != nil {
		t.Fatal(err)
	}
	foreign := source.DeepCopy()
	foreign.Name = idempotentJobID("test-user", "already-exists")
	foreign.UID, foreign.ResourceVersion = "foreign-direct-uid", ""
	foreign.Spec.IdempotencyKey = "already-exists"
	foreign.Annotations = nil
	if err := base.Create(ctx, foreign); err != nil {
		t.Fatal(err)
	}
	srv.reader = &oneMissBuildReader{Reader: base, name: foreign.Name}
	response := postGenesisReceiptBuild(t, srv, "already-exists")
	if response.Code != http.StatusConflict || writer.crCreates != 2 || receipts.creates != 2 {
		t.Fatalf("AlreadyExists accepted foreign CR: code=%d CR=%d receipts=%d body=%s", response.Code, writer.crCreates, receipts.creates, response.Body.String())
	}
	if _, found, err := srv.queueStore().Lookup(ctx, foreign.Name); err != nil || found {
		t.Fatalf("definitively rejected own Create retained queue charge: found=%t err=%v", found, err)
	}
	var cms corev1.ConfigMapList
	if err := base.List(ctx, &cms, client.InNamespace("receipts-http")); err != nil {
		t.Fatal(err)
	}
	if len(cms.Items) != 1 {
		t.Fatalf("foreign AlreadyExists leaked rejected receipt: %d", len(cms.Items))
	}
}

func TestGenesisHTTPReceiptOrPinUnknownNeverCreatesCR(t *testing.T) {
	for _, mode := range []string{"receipt-create", "queue-pin"} {
		t.Run(mode, func(t *testing.T) {
			srv, _, authAPI, receipts, writer := newGenesisReceiptHTTPServer(t)
			if mode == "receipt-create" {
				receipts.failCreate = true
			} else {
				authAPI.failPin = true
			}
			first := postGenesisReceiptBuild(t, srv, mode)
			id := first.Header().Get("X-Kova-Build-ID")
			if first.Code != http.StatusServiceUnavailable || id == "" || writer.crCreates != 0 || receipts.creates != 1 {
				t.Fatalf("unknown effect escaped: code=%d id=%q CR=%d receipts=%d body=%s", first.Code, id, writer.crCreates, receipts.creates, first.Body.String())
			}
			if _, found, err := srv.queueStore().Lookup(context.Background(), id); err != nil || !found {
				t.Fatalf("unknown effect lost charge: found=%t err=%v", found, err)
			}
			second := postGenesisReceiptBuild(t, srv, mode)
			if second.Code != http.StatusServiceUnavailable || writer.crCreates != 0 || receipts.creates != 1 {
				t.Fatalf("duplicate retried effect: code=%d CR=%d receipts=%d", second.Code, writer.crCreates, receipts.creates)
			}
		})
	}
}

func TestGenesisHTTPPinLostResponseAfterWriteDirectReadbackAllowsSoleCR(t *testing.T) {
	srv, _, authAPI, receipts, writer := newGenesisReceiptHTTPServer(t)
	authAPI.losePinReply = true
	response := postGenesisReceiptBuild(t, srv, "pin-lost-response")
	if response.Code != http.StatusAccepted || authAPI.queuePatches != 2 || writer.crCreates != 1 || receipts.creates != 1 {
		t.Fatalf("persisted pin was not qualified after lost response: code=%d patches=%d CR=%d receipts=%d body=%s",
			response.Code, authAPI.queuePatches, writer.crCreates, receipts.creates, response.Body.String())
	}
	entry, found, err := srv.queueStore().Lookup(context.Background(), response.Header().Get("X-Kova-Build-ID"))
	if err != nil || !found || entry.ReceiptUID == "" || entry.ReceiptDigest == "" {
		t.Fatalf("lost-response pin lacks durable readback: entry=%#v found=%t err=%v", entry, found, err)
	}
}

func TestGenesisHTTPDefinitiveCRRejectionSettlesButUncertainRetains(t *testing.T) {
	for _, tc := range []struct {
		name       string
		createErr  error
		wantCharge bool
	}{
		{"definitive", apierrors.NewForbidden(schema.GroupResource{Group: kovav1.Group, Resource: "kovabuilds"}, "build", errors.New("policy denied")), false},
		{"uncertain", context.DeadlineExceeded, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, _, _, receipts, writer := newGenesisReceiptHTTPServer(t)
			writer.crError = tc.createErr
			first := postGenesisReceiptBuild(t, srv, tc.name)
			id := first.Header().Get("X-Kova-Build-ID")
			entry, found, err := srv.queueStore().Lookup(context.Background(), id)
			if err != nil || found != tc.wantCharge || writer.crCreates != 1 || receipts.creates != 1 {
				t.Fatalf("wrong disposition: code=%d entry=%#v found=%t err=%v CR=%d receipts=%d body=%s", first.Code, entry, found, err, writer.crCreates, receipts.creates, first.Body.String())
			}
			if tc.wantCharge {
				if _, err := receipts.Get(context.Background(), "kova-admission-intent-"+entry.Nonce, metav1.GetOptions{}); err != nil {
					t.Fatalf("uncertain CR lost receipt: %v", err)
				}
				second := postGenesisReceiptBuild(t, srv, tc.name)
				if second.Code != http.StatusServiceUnavailable || writer.crCreates != 1 || receipts.creates != 1 {
					t.Fatalf("uncertain duplicate reissued Create: code=%d CR=%d receipts=%d", second.Code, writer.crCreates, receipts.creates)
				}
			} else {
				var cms corev1.ConfigMapList
				if err := receipts.Client.List(context.Background(), &cms, client.InNamespace("receipts-http")); err != nil {
					t.Fatal(err)
				}
				if len(cms.Items) != 0 {
					t.Fatalf("definitive rejection retained receipt: %d", len(cms.Items))
				}
			}
		})
	}
}
