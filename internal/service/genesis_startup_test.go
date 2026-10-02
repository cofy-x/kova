package service

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/admissiongenesis"
	"github.com/cofy-x/kova/internal/service/buildcontroller"
	"github.com/cofy-x/kova/internal/service/config"
	"github.com/cofy-x/kova/internal/service/queueadmission"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type staticGenesisCore struct {
	namespace *corev1.Namespace
	objects   map[string]*corev1.ConfigMap
	writes    int
}

func (f *staticGenesisCore) GetNamespace(context.Context, string) (*corev1.Namespace, error) {
	return f.namespace.DeepCopy(), nil
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

func genesisTestReceiptAndConfig() (admissiongenesis.Receipt, config.Config) {
	cfg := config.Config{Namespace: "jobs-57", MaxActiveJobs: 20, MaxActiveJobsPerRequester: 4,
		WorkerSlots: 20, MaxQueuedJobs: 1000, MaxQueuedJobsPerRequester: 100}
	r := admissiongenesis.Receipt{Namespace: cfg.Namespace, GenesisName: admissiongenesis.GenesisName,
		GenesisUID: "genesis-original", Contract: admissiongenesis.Contract{
			Version: 1, NamespaceUID: "namespace-original", Generation: strings.Repeat("a", 32),
			ActiveLedgerName: admissiongenesis.ActiveLedgerName, ActiveLedgerSchema: 1,
			QueueLedgerName: admissiongenesis.QueueLedgerName, QueueLedgerSchema: 1,
			Limits: admissiongenesis.Limits{MaxActiveJobs: cfg.MaxActiveJobs,
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
			if guard, err := prepareGenesisRuntime(context.Background(), changed, raw, nil, nil); err == nil || guard != nil {
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
	active, err := receipt.NewLedgerObject(admissiongenesis.Active, admissiongenesis.ActiveLedgerDataKey,
		activeData, strings.Repeat("b", 32))
	if err != nil {
		t.Fatal(err)
	}
	queue, err := receipt.NewLedgerObject(admissiongenesis.Queue, admissiongenesis.QueueLedgerDataKey,
		queueData, strings.Repeat("c", 32))
	if err != nil {
		t.Fatal(err)
	}
	active.UID, active.ResourceVersion = "active-original", "1"
	queue.UID, queue.ResourceVersion = "queue-original", "2"
	state := admissiongenesis.GenesisData{Contract: receipt.Contract, Phase: admissiongenesis.PhaseCommitted,
		ActiveLedgerUID: string(active.UID), QueueLedgerUID: string(queue.UID)}
	encoded, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	immutable := true
	genesis := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: cfg.Namespace,
		Name: receipt.GenesisName, UID: types.UID(receipt.GenesisUID), ResourceVersion: "3"},
		Data: map[string]string{admissiongenesis.GenesisDataKey: string(encoded)}, Immutable: &immutable}
	api := &staticGenesisCore{namespace: &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: cfg.Namespace, UID: types.UID(receipt.Contract.NamespaceUID)},
		Status: corev1.NamespaceStatus{Phase: corev1.NamespaceActive}},
		objects: map[string]*corev1.ConfigMap{
			receipt.GenesisName: genesis, active.Name: active, queue.Name: queue,
		}}
	receiptRaw, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	guard, err := prepareGenesisRuntime(context.Background(), cfg, receiptRaw, api, genesisTestReader(t))
	if err != nil {
		t.Fatal(err)
	}
	if guard.Original.ActiveLedgerUID != string(active.UID) || guard.Original.QueueLedgerUID != string(queue.UID) || api.writes != 0 {
		t.Fatalf("startup pair or writes differ: %+v, writes=%d", guard.Original, api.writes)
	}
	delete(api.objects, queue.Name)
	if err := guard.Check(context.Background()); err == nil || api.writes != 0 {
		t.Fatalf("committed queue loss gained authority or triggered a write: %v, writes=%d", err, api.writes)
	}
}

func genesisTestReader(t *testing.T, objects ...client.Object) client.Reader {
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
