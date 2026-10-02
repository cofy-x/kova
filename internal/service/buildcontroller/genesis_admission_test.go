package buildcontroller

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/admissiongenesis"
	"github.com/cofy-x/kova/internal/service/queueadmission"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type controllerGenesisAPI struct {
	client.Client
	patches int
}

func (a *controllerGenesisAPI) GetNamespace(ctx context.Context, name string) (*corev1.Namespace, error) {
	var ns corev1.Namespace
	if err := a.Get(ctx, client.ObjectKey{Name: name}, &ns); err != nil {
		return nil, err
	}
	return &ns, nil
}

func (a *controllerGenesisAPI) GetConfigMap(ctx context.Context, namespace, name string) (*corev1.ConfigMap, error) {
	var cm corev1.ConfigMap
	if err := a.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &cm); err != nil {
		return nil, err
	}
	return &cm, nil
}

func (a *controllerGenesisAPI) CreateConfigMap(ctx context.Context, _ string, cm *corev1.ConfigMap) (*corev1.ConfigMap, error) {
	if err := a.Create(ctx, cm); err != nil {
		return nil, err
	}
	return cm, nil
}

func (a *controllerGenesisAPI) PatchConfigMap(ctx context.Context, namespace, name string, body []byte) (*corev1.ConfigMap, error) {
	a.patches++
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
	if err := a.Patch(ctx, cm, client.RawPatch(types.JSONPatchType, body)); err != nil {
		return nil, err
	}
	return a.GetConfigMap(ctx, namespace, name)
}

func genesisAdmissionFixture(t *testing.T) (*KovaBuildReconciler, *controllerGenesisAPI, *kovav1.KovaBuild) {
	t.Helper()
	cfg := admissionConfig()
	cfg.Namespace = "jobs"
	build := queuedBuild("direct", "alice", 1, 1)
	receipt := admissiongenesis.Receipt{Namespace: cfg.Namespace, GenesisName: admissiongenesis.GenesisName,
		GenesisUID: "genesis-original", Contract: admissiongenesis.Contract{
			Version: 1, NamespaceUID: "namespace-original", Generation: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			ActiveLedgerName: admissiongenesis.ActiveLedgerName, ActiveLedgerSchema: 1,
			QueueLedgerName: admissiongenesis.QueueLedgerName, QueueLedgerSchema: 1,
			Limits: admissiongenesis.Limits{MaxActiveJobs: cfg.MaxActiveJobs, MaxActiveJobsPerRequester: cfg.MaxActiveJobsPerRequester,
				WorkerSlots: cfg.WorkerSlots, MaxQueuedJobs: cfg.MaxQueuedJobs, MaxQueuedJobsPerRequester: cfg.MaxQueuedJobsPerRequester},
		}}
	activeData, err := GenesisEmptyAdmissionData(cfg)
	if err != nil {
		t.Fatal(err)
	}
	queueData, err := queueadmission.GenesisEmptyQueueData(cfg.MaxQueuedJobs, cfg.MaxQueuedJobsPerRequester)
	if err != nil {
		t.Fatal(err)
	}
	active, err := receipt.NewLedgerObject(admissiongenesis.Active, admissiongenesis.ActiveLedgerDataKey, activeData,
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	if err != nil {
		t.Fatal(err)
	}
	queue, err := receipt.NewLedgerObject(admissiongenesis.Queue, admissiongenesis.QueueLedgerDataKey, queueData,
		"cccccccccccccccccccccccccccccccc")
	if err != nil {
		t.Fatal(err)
	}
	active.UID, active.ResourceVersion = "active-original", "11"
	queue.UID, queue.ResourceVersion = "queue-original", "12"
	data, err := json.Marshal(admissiongenesis.GenesisData{Contract: receipt.Contract, Phase: admissiongenesis.PhaseCommitted,
		ActiveLedgerUID: string(active.UID), QueueLedgerUID: string(queue.UID)})
	if err != nil {
		t.Fatal(err)
	}
	immutable := true
	genesis := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: cfg.Namespace, Name: receipt.GenesisName,
		UID: types.UID(receipt.GenesisUID), ResourceVersion: "13"}, Immutable: &immutable,
		Data: map[string]string{admissiongenesis.GenesisDataKey: string(data)}}
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: cfg.Namespace, UID: types.UID(receipt.Contract.NamespaceUID)},
		Status: corev1.NamespaceStatus{Phase: corev1.NamespaceActive}}
	base := crfake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(namespace, genesis, active, queue, build).Build()
	api := &controllerGenesisAPI{Client: base}
	bootstrap := admissiongenesis.Bootstrapper{API: api, Receipt: receipt,
		Active: admissiongenesis.LedgerTemplate{Role: admissiongenesis.Active, DataKey: admissiongenesis.ActiveLedgerDataKey,
			EmptyData: activeData, Validate: func(cm *corev1.ConfigMap) error { return ValidateAdmissionLedgerForGenesis(cm, cfg) }},
		Queue: admissiongenesis.LedgerTemplate{Role: admissiongenesis.Queue, DataKey: admissiongenesis.QueueLedgerDataKey,
			EmptyData: queueData, Validate: func(cm *corev1.ConfigMap) error {
				return queueadmission.ValidateQueueLedgerForGenesis(cm, cfg.MaxQueuedJobs, cfg.MaxQueuedJobsPerRequester)
			}},
		Preflight: func(context.Context) error { return nil },
	}
	binding := admissiongenesis.Binding{NamespaceUID: receipt.Contract.NamespaceUID, GenesisUID: receipt.GenesisUID,
		ActiveLedgerUID: string(active.UID), QueueLedgerUID: string(queue.UID)}
	guard, err := admissiongenesis.NewGuard(context.Background(), bootstrap, binding)
	if err != nil {
		t.Fatal(err)
	}
	r := &KovaBuildReconciler{Client: base, APIReader: base, Cfg: cfg, Genesis: guard}
	return r, api, build
}

func TestGenesisDirectBuildRefusesMissingOrReplacedLedger(t *testing.T) {
	for _, tc := range []struct {
		name, ledger string
		replace      bool
	}{
		{"active lost", admissiongenesis.ActiveLedgerName, false},
		{"queue lost", admissiongenesis.QueueLedgerName, false},
		{"active replaced", admissiongenesis.ActiveLedgerName, true},
		{"queue replaced", admissiongenesis.QueueLedgerName, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			r, api, build := genesisAdmissionFixture(t)
			var cm corev1.ConfigMap
			if err := api.Get(ctx, client.ObjectKey{Namespace: build.Namespace, Name: tc.ledger}, &cm); err != nil {
				t.Fatal(err)
			}
			if err := api.Delete(ctx, &cm); err != nil {
				t.Fatal(err)
			}
			if tc.replace {
				replacement := cm.DeepCopy()
				replacement.ResourceVersion = ""
				replacement.UID = types.UID("replacement-ledger")
				if err := api.Create(ctx, replacement); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := r.admission(ctx, build); err == nil {
				t.Fatal("direct/admin CR received a grant without the original pair")
			}
			if api.patches != 0 {
				t.Fatalf("patched admission ledger after pair loss: %d", api.patches)
			}
			if tc.ledger == admissiongenesis.QueueLedgerName && !tc.replace {
				if err := r.queueStoreForNamespace(build.Namespace).EnsureInitialized(ctx); err == nil {
					t.Fatal("Genesis queue store recreated lost ledger")
				}
				var missing corev1.ConfigMap
				if err := api.Get(ctx, client.ObjectKey{Namespace: build.Namespace, Name: tc.ledger}, &missing); err == nil {
					t.Fatal("Genesis queue store recreated lost ledger")
				}
			}
		})
	}
}

func TestGenesisAdmissionPumpRefusesLostPair(t *testing.T) {
	ctx := context.Background()
	r, api, _ := genesisAdmissionFixture(t)
	var queue corev1.ConfigMap
	if err := api.Get(ctx, client.ObjectKey{Namespace: r.Cfg.Namespace, Name: queueadmission.ConfigMapName}, &queue); err != nil {
		t.Fatal(err)
	}
	if err := api.Delete(ctx, &queue); err != nil {
		t.Fatal(err)
	}
	pump := NewAdmissionPump(api.Client, r.Cfg)
	pump.Genesis = r.Genesis
	if _, err := pump.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{
		Namespace: r.Cfg.Namespace, Name: AdmissionLedgerName,
	}}); err == nil {
		t.Fatal("pump continued selecting admissions without the original pair")
	}
}

func TestGenesisDirectBuildGrantUsesConditionalLedgerPatch(t *testing.T) {
	ctx := context.Background()
	r, api, build := genesisAdmissionFixture(t)
	decision, err := r.admission(ctx, build)
	if err != nil {
		t.Fatal(err)
	}
	if !decision.Admitted || api.patches != 1 {
		t.Fatalf("direct/admin CR did not receive exactly one guarded grant: decision=%+v patches=%d", decision, api.patches)
	}
	_, state, err := r.readReservations(ctx, build.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := state.Active[reservationKey(build)]; !exists {
		t.Fatal("conditional grant did not persist active reservation")
	}
	if err := r.Genesis.Check(ctx); err != nil {
		t.Fatal(fmt.Errorf("grant changed qualified pair: %w", err))
	}
}
