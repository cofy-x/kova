package service

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/cofy-x/kova/internal/admissioncontract"
	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/daemonclient"
	"github.com/cofy-x/kova/internal/service/admissiongenesis"
	"github.com/cofy-x/kova/internal/service/buildcontroller"
	"github.com/cofy-x/kova/internal/service/config"
	"github.com/cofy-x/kova/internal/service/queueadmission"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type staticGenesisCore struct {
	namespace        *corev1.Namespace
	receiptNamespace *corev1.Namespace
	objects          map[string]*corev1.ConfigMap
	writes           int
}

type exhaustedControllerCore struct{ admissiongenesis.CoreAPI }

func (exhaustedControllerCore) GetNamespace(context.Context, string) (*corev1.Namespace, error) {
	return nil, errors.New("controller API budget exhausted")
}

// This is a deterministic API-backed startup fixture, not a Kubernetes
// apiserver. It assigns ledger UIDs and applies the production JSON Patch
// through controller-runtime's fake client so a second Service startup sees
// the exact objects left by the first one.
type restartingGenesisCore struct {
	client.Client
	createNames         []string
	patches             int
	failQueueCreateOnce bool
}

func (f *restartingGenesisCore) GetNamespace(ctx context.Context, name string) (*corev1.Namespace, error) {
	var ns corev1.Namespace
	if err := f.Get(ctx, client.ObjectKey{Name: name}, &ns); err != nil {
		return nil, err
	}
	return &ns, nil
}

func (f *restartingGenesisCore) GetConfigMap(ctx context.Context, namespace, name string) (*corev1.ConfigMap, error) {
	var cm corev1.ConfigMap
	if err := f.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &cm); err != nil {
		return nil, err
	}
	return &cm, nil
}

func (f *restartingGenesisCore) CreateConfigMap(ctx context.Context, namespace string, cm *corev1.ConfigMap) (*corev1.ConfigMap, error) {
	f.createNames = append(f.createNames, cm.Name)
	if cm.Name == admissioncontract.QueueLedgerName && f.failQueueCreateOnce {
		f.failQueueCreateOnce = false
		return nil, errors.New("injected queue Create failure before the request reached the API")
	}
	created := cm.DeepCopy()
	if cm.Name == admissioncontract.ActiveLedgerName {
		created.UID = types.UID("active-created-once")
	} else if cm.Name == admissioncontract.QueueLedgerName {
		created.UID = types.UID("queue-created-once")
	} else {
		return nil, errors.New("runtime attempted an unexpected ConfigMap Create")
	}
	if err := f.Create(ctx, created); err != nil {
		return nil, err
	}
	return f.GetConfigMap(ctx, namespace, cm.Name)
}

func (f *restartingGenesisCore) PatchConfigMap(ctx context.Context, namespace, name string, body []byte) (*corev1.ConfigMap, error) {
	if name != admissioncontract.GenesisName {
		return nil, errors.New("runtime attempted an unexpected ConfigMap Patch")
	}
	f.patches++
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
	if err := f.Patch(ctx, cm, client.RawPatch(types.JSONPatchType, body)); err != nil {
		return nil, err
	}
	return f.GetConfigMap(ctx, namespace, name)
}

func (f *staticGenesisCore) GetNamespace(_ context.Context, name string) (*corev1.Namespace, error) {
	if f.namespace != nil && f.namespace.Name == name {
		return f.namespace.DeepCopy(), nil
	}
	if f.receiptNamespace != nil && f.receiptNamespace.Name == name {
		return f.receiptNamespace.DeepCopy(), nil
	}
	return nil, errors.New("missing Namespace")
}

func (f *staticGenesisCore) GetConfigMap(_ context.Context, _, name string) (*corev1.ConfigMap, error) {
	if cm := f.objects[name]; cm != nil {
		return cm.DeepCopy(), nil
	}
	return nil, errors.New("missing ConfigMap")
}

func (f *staticGenesisCore) CreateConfigMap(context.Context, string, *corev1.ConfigMap) (*corev1.ConfigMap, error) {
	f.writes++
	return nil, errors.New("unexpected Create")
}

func (f *staticGenesisCore) PatchConfigMap(context.Context, string, string, []byte) (*corev1.ConfigMap, error) {
	f.writes++
	return nil, errors.New("unexpected Patch")
}

func genesisTestReceiptAndConfig() (admissioncontract.Receipt, config.Config) {
	cfg := config.Config{Namespace: "jobs-57", MaxActiveJobs: 20, MaxActiveJobsPerRequester: 4,
		WorkerSlots: 20, MaxQueuedJobs: 1000, MaxQueuedJobsPerRequester: 100}
	r := admissioncontract.Receipt{Namespace: cfg.Namespace, GenesisName: admissioncontract.GenesisName,
		GenesisUID: "genesis-original", Contract: admissioncontract.Contract{
			Version: 2, NamespaceUID: "namespace-original", Generation: strings.Repeat("a", 32),
			ReceiptNamespace: "receipts-57", ReceiptNamespaceUID: "receipts-original",
			ActiveLedgerName: admissioncontract.ActiveLedgerName, ActiveLedgerSchema: 1,
			QueueLedgerName: admissioncontract.QueueLedgerName, QueueLedgerSchema: 2,
			Limits: admissioncontract.Limits{MaxActiveJobs: cfg.MaxActiveJobs,
				MaxActiveJobsPerRequester: cfg.MaxActiveJobsPerRequester, WorkerSlots: cfg.WorkerSlots,
				MaxQueuedJobs: cfg.MaxQueuedJobs, MaxQueuedJobsPerRequester: cfg.MaxQueuedJobsPerRequester},
		}}
	return r, cfg
}

func TestGenesisRuntimeConfigMustMatchReceiptBeforeBootstrap(t *testing.T) {
	receipt, cfg := genesisTestReceiptAndConfig()
	if err := validateGenesisRuntimeConfig(cfg, receipt); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*config.Config){
		"namespace": func(c *config.Config) { c.Namespace = "other-jobs" },
		"active":    func(c *config.Config) { c.MaxActiveJobs++ },
		"requester": func(c *config.Config) { c.MaxActiveJobsPerRequester++ },
		"slots":     func(c *config.Config) { c.WorkerSlots++ },
		"queued":    func(c *config.Config) { c.MaxQueuedJobs-- },
		"q-requester": func(c *config.Config) {
			c.MaxQueuedJobsPerRequester--
		},
		"reserved runner UID env": func(c *config.Config) {
			c.RunnerEnv = map[string]string{daemonclient.RunnerPodUIDEnv: "forged"}
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := cfg
			mutate(&changed)
			if err := validateGenesisRuntimeConfig(changed, receipt); err == nil {
				t.Fatal("runtime configuration drift passed")
			}
			raw, err := json.Marshal(receipt)
			if err != nil {
				t.Fatal(err)
			}
			// A mismatch must stop before a direct API/client is even needed.
			if guard, err := prepareGenesisRuntime(context.Background(), changed, raw, nil, nil, nil); err == nil || guard != nil {
				t.Fatalf("bootstrap reached API with mismatched config: guard=%v err=%v", guard, err)
			}
		})
	}
}

func TestPrepareGenesisRuntimeObservesExactCommittedPairWithoutLegacyWrites(t *testing.T) {
	receipt, cfg := genesisTestReceiptAndConfig()
	activeData, err := buildcontroller.GenesisEmptyAdmissionData(cfg)
	if err != nil {
		t.Fatal(err)
	}
	queueData, err := queueadmission.GenesisEmptyQueueData(cfg.MaxQueuedJobs, cfg.MaxQueuedJobsPerRequester)
	if err != nil {
		t.Fatal(err)
	}
	active, err := receipt.NewLedgerObject(admissioncontract.Active, admissioncontract.ActiveLedgerDataKey,
		activeData, strings.Repeat("b", 32))
	if err != nil {
		t.Fatal(err)
	}
	queue, err := receipt.NewLedgerObject(admissioncontract.Queue, admissioncontract.QueueLedgerDataKey,
		queueData, strings.Repeat("c", 32))
	if err != nil {
		t.Fatal(err)
	}
	active.UID, active.ResourceVersion = "active-original", "1"
	queue.UID, queue.ResourceVersion = "queue-original", "2"
	state := admissioncontract.GenesisData{Contract: receipt.Contract, Phase: admissioncontract.PhaseCommitted,
		ActiveLedgerUID: string(active.UID), QueueLedgerUID: string(queue.UID)}
	encoded, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	immutable := true
	genesis := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: cfg.Namespace,
		Name: receipt.GenesisName, UID: types.UID(receipt.GenesisUID), ResourceVersion: "3"},
		Data: map[string]string{admissioncontract.GenesisDataKey: string(encoded)}, Immutable: &immutable}
	api := &staticGenesisCore{namespace: &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: cfg.Namespace, UID: types.UID(receipt.Contract.NamespaceUID)},
		Status: corev1.NamespaceStatus{Phase: corev1.NamespaceActive}},
		receiptNamespace: &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: receipt.Contract.ReceiptNamespace,
			UID: types.UID(receipt.Contract.ReceiptNamespaceUID)}, Status: corev1.NamespaceStatus{Phase: corev1.NamespaceActive}},
		objects: map[string]*corev1.ConfigMap{
			receipt.GenesisName: genesis, active.Name: active, queue.Name: queue,
		}}
	receiptRaw, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	guard, err := prepareGenesisRuntime(context.Background(), cfg, receiptRaw, api, genesisTestReader(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	if guard.Original.ActiveLedgerUID != string(active.UID) || guard.Original.QueueLedgerUID != string(queue.UID) || api.writes != 0 {
		t.Fatalf("startup pair or writes differ: %+v, writes=%d", guard.Original, api.writes)
	}
	httpAPI := &staticGenesisCore{namespace: api.namespace, receiptNamespace: api.receiptNamespace, objects: api.objects}
	readinessAPI := &staticGenesisCore{namespace: api.namespace, receiptNamespace: api.receiptNamespace, objects: api.objects}
	httpView, err := forkGenesisGuard(context.Background(), guard, httpAPI, func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	readinessView, err := forkGenesisGuard(context.Background(), guard, readinessAPI, func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	guard.Bootstrap.API = exhaustedControllerCore{CoreAPI: api}
	if err := guard.Check(context.Background()); err == nil {
		t.Fatal("test controller budget was not exhausted")
	}
	if err := httpView.Check(context.Background()); err != nil {
		t.Fatalf("controller budget blocked HTTP admission: %v", err)
	}
	if err := readinessView.Check(context.Background()); err != nil {
		t.Fatalf("controller budget blocked readiness: %v", err)
	}
	guard.Bootstrap.API = api
	delete(api.objects, queue.Name)
	if err := guard.Check(context.Background()); err == nil || api.writes != 0 {
		t.Fatalf("committed queue loss gained authority or triggered a write: %v, writes=%d", err, api.writes)
	}
}

func TestPrepareGenesisRuntimeCompletesOriginalInitializingPairAfterPartialRestart(t *testing.T) {
	ctx := context.Background()
	receipt, cfg := genesisTestReceiptAndConfig()
	initial := admissioncontract.GenesisData{Contract: receipt.Contract, Phase: admissioncontract.PhaseInitializing}
	initialRaw, err := json.Marshal(initial)
	if err != nil {
		t.Fatal(err)
	}
	receiptRaw, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	genesis := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Namespace: cfg.Namespace, Name: receipt.GenesisName, UID: types.UID(receipt.GenesisUID), ResourceVersion: "1",
	}, Data: map[string]string{admissioncontract.GenesisDataKey: string(initialRaw)}}
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: cfg.Namespace, UID: types.UID(receipt.Contract.NamespaceUID),
	}, Status: corev1.NamespaceStatus{Phase: corev1.NamespaceActive}}
	receiptNamespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: receipt.Contract.ReceiptNamespace,
		UID: types.UID(receipt.Contract.ReceiptNamespaceUID)}, Status: corev1.NamespaceStatus{Phase: corev1.NamespaceActive}}
	base := genesisTestClient(t, namespace, receiptNamespace, genesis)
	api := &restartingGenesisCore{Client: base, failQueueCreateOnce: true}
	if guard, err := prepareGenesisRuntime(ctx, cfg, receiptRaw, api, base, nil); err == nil || guard != nil {
		t.Fatalf("partial first startup unexpectedly routed: guard=%v err=%v", guard, err)
	}
	var pinned corev1.ConfigMap
	if err := base.Get(ctx, client.ObjectKey{Namespace: cfg.Namespace, Name: receipt.GenesisName}, &pinned); err != nil {
		t.Fatal(err)
	}
	var partial admissioncontract.GenesisData
	if err := json.Unmarshal([]byte(pinned.Data[admissioncontract.GenesisDataKey]), &partial); err != nil {
		t.Fatal(err)
	}
	if partial.Phase != admissioncontract.PhaseInitializing || partial.ActiveLedgerUID != "active-created-once" ||
		partial.QueueLedgerUID != "" || len(api.createNames) != 2 || api.patches != 1 {
		t.Fatalf("first startup did not retain the exact provisional pin: state=%+v creates=%v patches=%d",
			partial, api.createNames, api.patches)
	}
	guard, err := prepareGenesisRuntime(ctx, cfg, receiptRaw, api, base, nil)
	if err != nil {
		t.Fatalf("same-receipt restart could not finish fresh Initializing Genesis: %v", err)
	}
	if guard.Original.ActiveLedgerUID != "active-created-once" || guard.Original.QueueLedgerUID != "queue-created-once" ||
		len(api.createNames) != 3 || api.createNames[2] != admissioncontract.QueueLedgerName || api.patches != 3 {
		t.Fatalf("restart replaced original ledger or missed commit: binding=%+v creates=%v patches=%d",
			guard.Original, api.createNames, api.patches)
	}
	active, err := api.GetConfigMap(ctx, cfg.Namespace, admissioncontract.ActiveLedgerName)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := api.GetConfigMap(ctx, cfg.Namespace, admissioncontract.QueueLedgerName)
	if err != nil {
		t.Fatal(err)
	}
	activeEmpty, err := buildcontroller.GenesisEmptyAdmissionData(cfg)
	if err != nil {
		t.Fatal(err)
	}
	queueEmpty, err := queueadmission.GenesisEmptyQueueData(cfg.MaxQueuedJobs, cfg.MaxQueuedJobsPerRequester)
	if err != nil {
		t.Fatal(err)
	}
	if active.Data[admissioncontract.ActiveLedgerDataKey] != activeEmpty ||
		queue.Data[admissioncontract.QueueLedgerDataKey] != queueEmpty ||
		buildcontroller.ValidateAdmissionLedgerForGenesis(active, cfg) != nil ||
		queueadmission.ValidateQueueLedgerForGenesis(queue, cfg.MaxQueuedJobs, cfg.MaxQueuedJobsPerRequester) != nil {
		t.Fatal("restarted Service did not commit canonical #58-bounded ledgers")
	}
	committed, err := api.GetConfigMap(ctx, cfg.Namespace, receipt.GenesisName)
	if err != nil {
		t.Fatal(err)
	}
	var committedData admissioncontract.GenesisData
	if err := json.Unmarshal([]byte(committed.Data[admissioncontract.GenesisDataKey]), &committedData); err != nil {
		t.Fatal(err)
	}
	if committedData.Phase != admissioncontract.PhaseCommitted || committed.Immutable == nil || !*committed.Immutable {
		t.Fatalf("restart did not atomically commit immutable Genesis: data=%+v immutable=%v", committedData, committed.Immutable)
	}
	if err := guard.Check(ctx); err != nil {
		t.Fatal(err)
	}
	creates, patches := len(api.createNames), api.patches
	if _, err := prepareGenesisRuntime(ctx, cfg, receiptRaw, api, base, nil); err != nil {
		t.Fatalf("same-receipt committed restart failed: %v", err)
	}
	if len(api.createNames) != creates || api.patches != patches {
		t.Fatalf("committed restart performed a bootstrap write: creates=%v patches=%d", api.createNames, api.patches)
	}
	if err := base.Delete(ctx, queue); err != nil {
		t.Fatal(err)
	}
	if guard, err := prepareGenesisRuntime(ctx, cfg, receiptRaw, api, base, nil); !apierrors.IsNotFound(err) || guard != nil {
		t.Fatalf("committed queue loss was repaired or misclassified: guard=%v err=%v", guard, err)
	}
	if len(api.createNames) != creates || api.patches != patches {
		t.Fatalf("committed loss triggered a replacement write: creates=%v patches=%d", api.createNames, api.patches)
	}
}

func genesisTestClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := kovav1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

func genesisTestReader(t *testing.T, objects ...client.Object) client.Reader {
	return genesisTestClient(t, objects...)
}

func TestGenesisPreflightVetoesVisibleOldCRsAndRunnerPods(t *testing.T) {
	ctx := context.Background()
	if err := genesisOldWorkVeto(ctx, genesisTestReader(t), "jobs-57"); err != nil {
		t.Fatal(err)
	}
	unrelated := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "helper", Namespace: "jobs-57"}}
	if err := genesisOldWorkVeto(ctx, genesisTestReader(t, unrelated), "jobs-57"); err != nil {
		t.Fatalf("unrelated Pod was treated as old runner: %v", err)
	}
	for name, object := range map[string]client.Object{
		"old CR": &kovav1.KovaBuild{ObjectMeta: metav1.ObjectMeta{Name: "old", Namespace: "jobs-57"}},
		"runner label": &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "jobs-57",
			Labels: map[string]string{"kova.cofy.dev/build-id": "old"}}},
		"runner name": &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "kova-job-old", Namespace: "jobs-57"}},
		"runner owner": &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "renamed-runner", Namespace: "jobs-57",
			OwnerReferences: []metav1.OwnerReference{{APIVersion: kovav1.Group + "/" + kovav1.Version,
				Kind: "KovaBuild", Name: "old", UID: types.UID("old-uid")}}}},
	} {
		t.Run(name, func(t *testing.T) {
			if err := genesisOldWorkVeto(ctx, genesisTestReader(t, object), "jobs-57"); err == nil {
				t.Fatal("visible old work did not veto Genesis bootstrap")
			}
		})
	}
}
