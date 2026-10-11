package buildcontroller

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/cofy-x/kova/internal/admissioncontract"
	"github.com/cofy-x/kova/internal/service/admissiongenesis"
	"github.com/cofy-x/kova/internal/service/queueadmission"
	"github.com/cofy-x/kova/internal/service/recoveryreceipt"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type a11OneConflict struct {
	admissiongenesis.CoreAPI
	actor client.Client
	kind  string
	fired bool
}

// The production receipt callback performs one direct Secret GET at each
// invocation. Keep this as an actual fake-client read, separate from the
// counted CoreAPI/normal controller client, not the capacity harness's
// five-external-GET owner preflight model.
func a11ReceiptCheck(t *testing.T, r *KovaBuildReconciler, base client.Client) func(context.Context) error {
	t.Helper()
	raw, err := json.Marshal(r.Genesis.Bootstrap.Receipt)
	if err != nil {
		t.Fatal(err)
	}
	immutable := true
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "control-a11", Name: "receipt", UID: "secret-a11-original"},
		Immutable: &immutable, Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{admissioncontract.ReceiptSecretDataKey: raw}}
	if err := base.Create(t.Context(), secret); err != nil {
		t.Fatal(err)
	}
	return func(ctx context.Context) error {
		var observed corev1.Secret
		if err := base.Get(ctx, client.ObjectKeyFromObject(secret), &observed); err != nil {
			return err
		}
		return admissioncontract.ValidateReceiptSecret(&observed, secret.Namespace, secret.Name, string(secret.UID), raw)
	}
}

func (a *a11OneConflict) PatchConfigMap(ctx context.Context, namespace, name string, body []byte) (*corev1.ConfigMap, error) {
	if !a.fired && name == admissioncontract.ActiveLedgerName {
		a.fired = true
		if a.kind == "422" {
			// A separate same-original writer advances RV; the measured
			// writer receives only an RFC6902 failed-test response. Its
			// readback must prove this before classifying contention.
			var cm corev1.ConfigMap
			key := client.ObjectKey{Namespace: namespace, Name: name}
			if err := a.actor.Get(ctx, key, &cm); err != nil {
				return nil, err
			}
			if err := a.actor.Update(ctx, &cm); err != nil {
				return nil, err
			}
			return nil, &apierrors.StatusError{ErrStatus: metav1.Status{Code: 422, Reason: metav1.StatusReasonInvalid, Message: "synthetic stale JSON Patch test"}}
		}
		return nil, apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, name, errors.New("synthetic one-shot conflict"))
	}
	return a.CoreAPI.PatchConfigMap(ctx, namespace, name, body)
}

// TestA11AdmissionReadBudget isolates one successful v3 active grant from
// live apiserver latency, client throttling, and competing reconcilers.
// A future change should reduce reads without weakening the separate
// UID/RV/data-CAS and uncertain-outcome protocol tests.
func TestA11AdmissionReadBudget(t *testing.T) {
	for _, tt := range []struct {
		name, conflict                          string
		maxCalls, maxReceiptReads, maxMutations int
	}{
		{name: "no-contention", conflict: "none", maxCalls: 187, maxReceiptReads: 32, maxMutations: 4},
		{name: "one-409", conflict: "409", maxCalls: 221, maxReceiptReads: 38, maxMutations: 5},
		{name: "one-same-original-422", conflict: "422", maxCalls: 232, maxReceiptReads: 40, maxMutations: 5},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r, api, build := genesisAdmissionFixture(t)
			rawReceiptCheck := a11ReceiptCheck(t, r, api.Client)
			counted := &capacityCountingClient{Client: api.Client}
			var underlying admissiongenesis.CoreAPI = api
			var conflict *a11OneConflict
			if tt.conflict != "none" {
				conflict = &a11OneConflict{CoreAPI: api, actor: api.Client, kind: tt.conflict}
				underlying = conflict
			}
			core := &capacityCountingCoreAPI{CoreAPI: underlying}
			r.Client, r.APIReader = counted, counted
			r.Genesis.Bootstrap.API = core
			r.RecoveryReceipts = controllerReceiptConfigMaps{Client: counted, namespace: r.Genesis.Bootstrap.Receipt.Contract.ReceiptNamespace}
			externalReads := 0
			var stageReceiptReads [6]int
			r.Genesis.ReceiptCheck = func(ctx context.Context) error {
				externalReads++
				stage := core.mutations + counted.mutations
				if stage < len(stageReceiptReads) {
					stageReceiptReads[stage]++
				}
				return rawReceiptCheck(ctx)
			}
			decision, err := r.admission(t.Context(), build)
			if err != nil || !decision.Admitted {
				t.Fatalf("v3 direct grant failed: decision=%#v err=%v", decision, err)
			}
			total := core.calls + counted.calls + externalReads
			mutations := core.mutations + counted.mutations
			t.Logf("v3 direct grant: total=%d, core=%d, controller=%d, external=%d, checks=%d, mutations=%d", total, core.calls, counted.calls, externalReads, externalReads/2, mutations)
			t.Logf("v3 direct grant receipt checks by mutation stage=%v", stageReceiptReads)
			if conflict != nil && !conflict.fired {
				t.Fatal("one-shot conflict was not exercised")
			}
			if externalReads != tt.maxReceiptReads || total != tt.maxCalls || mutations != tt.maxMutations {
				t.Fatalf("v3 direct grant request model exceeded budget or lost receipt checks: total=%d external=%d mutations=%d", total, externalReads, mutations)
			}
		})
	}
}

// This measured request starts after the HTTP queue charge and immutable
// receipt have been persisted, matching the controller's queued-CR path.
// It does not include the preceding HTTP POST or subsequent Pod attempt.
func TestA11QueuedGrantReadBudget(t *testing.T) {
	r, api, build := genesisAdmissionFixture(t)
	queue := r.queueStoreForNamespace(build.Namespace)
	entry, fresh, err := queue.Reserve(t.Context(), build)
	if err != nil || !fresh {
		t.Fatalf("queue reserve: fresh=%t err=%v", fresh, err)
	}
	intent, err := queue.ReceiptIntent(build, entry)
	if err != nil {
		t.Fatal(err)
	}
	witness, err := recoveryreceipt.RecordQueueOnce(t.Context(), r.RecoveryReceipts, intent)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.PinReceipt(t.Context(), build, entry, witness); err != nil {
		t.Fatal(err)
	}
	build.Annotations = map[string]string{
		queueadmission.IntentAnnotation:        entry.Nonce,
		queueadmission.ReceiptUIDAnnotation:    witness.ReceiptUID,
		queueadmission.ReceiptDigestAnnotation: witness.DataDigest,
	}
	if err := api.Update(t.Context(), build); err != nil {
		t.Fatal(err)
	}
	rawReceiptCheck := a11ReceiptCheck(t, r, api.Client)
	counted := &capacityCountingClient{Client: api.Client}
	core := &capacityCountingCoreAPI{CoreAPI: api}
	r.Client, r.APIReader = counted, counted
	r.Genesis.Bootstrap.API = core
	r.RecoveryReceipts = controllerReceiptConfigMaps{Client: counted, namespace: r.Genesis.Bootstrap.Receipt.Contract.ReceiptNamespace}
	externalReads := 0
	var stageReceiptReads [5]int
	r.Genesis.ReceiptCheck = func(ctx context.Context) error {
		externalReads++
		stage := core.mutations + counted.mutations
		if stage < len(stageReceiptReads) {
			stageReceiptReads[stage]++
		}
		return rawReceiptCheck(ctx)
	}
	decision, err := r.admission(t.Context(), build)
	if err != nil || !decision.Admitted {
		t.Fatalf("queued grant: decision=%#v err=%v", decision, err)
	}
	total := core.calls + counted.calls + externalReads
	t.Logf("v3 queued grant: total=%d core=%d controller=%d external=%d checks=%d mutations=%d", total, core.calls, counted.calls, externalReads, externalReads/2, core.mutations+counted.mutations)
	t.Logf("v3 queued grant receipt check counts by mutation stage=%v", stageReceiptReads)
	if total != 203 || externalReads != 34 || core.mutations+counted.mutations != 4 {
		t.Fatal("queued grant lost receipt checks or exceeded offline budget")
	}
}
