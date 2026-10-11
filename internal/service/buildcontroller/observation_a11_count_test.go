package buildcontroller

import (
	"context"
	"testing"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/queueadmission"
	"github.com/cofy-x/kova/internal/service/recoveryreceipt"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type a11FullCounts struct {
	total, core, controller, receipt, effects int
}

func TestA11FullReceiptedGrantPodAndRunnerReadBudget(t *testing.T) {
	for _, queued := range []bool{false, true} {
		name := "direct"
		if queued {
			name = "queued"
		}
		t.Run(name, func(t *testing.T) { runA11FullReceiptedBudget(t, queued) })
	}
}

func runA11FullReceiptedBudget(t *testing.T, queued bool) {
	ctx := context.Background()
	r, api, build := genesisAdmissionFixture(t)
	if queued {
		queue := r.queueStoreForNamespace(build.Namespace)
		entry, fresh, err := queue.Reserve(ctx, build)
		if err != nil || !fresh {
			t.Fatalf("queue reserve: fresh=%t err=%v", fresh, err)
		}
		intent, err := queue.ReceiptIntent(build, entry)
		if err != nil {
			t.Fatal(err)
		}
		witness, err := recoveryreceipt.RecordQueueOnce(ctx, r.RecoveryReceipts, intent)
		if err != nil {
			t.Fatal(err)
		}
		if err := queue.PinReceipt(ctx, build, entry, witness); err != nil {
			t.Fatal(err)
		}
		build.Annotations = map[string]string{queueadmission.IntentAnnotation: entry.Nonce,
			queueadmission.ReceiptUIDAnnotation:    witness.ReceiptUID,
			queueadmission.ReceiptDigestAnnotation: witness.DataDigest}
		if err := api.Update(ctx, build); err != nil {
			t.Fatal(err)
		}
	}
	rawReceiptCheck := a11ReceiptCheck(t, r, api.Client)
	counted := &capacityCountingClient{Client: api.Client}
	core := &capacityCountingCoreAPI{CoreAPI: api}
	r.Client, r.APIReader = counted, counted
	r.Genesis.Bootstrap.API = core
	r.RecoveryReceipts = controllerReceiptConfigMaps{Client: counted, namespace: r.Genesis.Bootstrap.Receipt.Contract.ReceiptNamespace}
	receiptReads := 0
	r.Genesis.ReceiptCheck = func(ctx context.Context) error {
		receiptReads++
		return rawReceiptCheck(ctx)
	}
	snapshot := func() a11FullCounts {
		return a11FullCounts{total: core.calls + counted.calls + receiptReads, core: core.calls,
			controller: counted.calls, receipt: receiptReads, effects: core.mutations + counted.mutations}
	}
	stage := func(name string, before a11FullCounts, maxCalls, maxChecks, exactEffects int) a11FullCounts {
		after := snapshot()
		calls := after.total - before.total
		checks := (after.receipt - before.receipt) / 2
		effects := after.effects - before.effects
		t.Logf("%s: calls=%d core=%d controller=%d externalSecret=%d fullChecks=%d effects=%d",
			name, calls, after.core-before.core, after.controller-before.controller,
			after.receipt-before.receipt, checks, effects)
		if (after.receipt-before.receipt)%2 != 0 {
			t.Fatalf("%s ended inside a full external receipt Check", name)
		}
		if calls > maxCalls || checks > maxChecks || effects != exactEffects {
			t.Fatalf("%s changed the bounded read/effect model: calls=%d checks=%d effects=%d", name, calls, checks, effects)
		}
		return after
	}
	previous := snapshot()
	if decision, err := r.admission(ctx, build); err != nil || !decision.Admitted {
		t.Fatalf("grant: decision=%+v err=%v", decision, err)
	}
	grantBudget, grantChecks := 187, 16
	nonceBudget, podArmBudget, completionBudget, runnerBudget := 59, 196, 58, 34
	if queued {
		grantBudget, grantChecks = 203, 17
		nonceBudget, podArmBudget, completionBudget, runnerBudget = 61, 204, 60, 38
	}
	terminalBudget, terminalChecks, terminalEffects := 413, 35, 7
	if queued {
		terminalBudget, terminalChecks, terminalEffects = 591, 50, 11
	}
	previous = stage("active grant E1-E4", previous, grantBudget, grantChecks, 4)
	pod := receiptedGenesisPodTemplate(t, r, build)
	attempt, err := r.beginPodCreate(ctx, build)
	if err != nil {
		t.Fatal(err)
	}
	previous = stage("Pod nonce", previous, nonceBudget, 5, 1)
	if err := r.stampGenesisPod(ctx, build, pod, attempt); err != nil {
		t.Fatal(err)
	}
	previous = stage("Pod immutable stamp", previous, 10, 1, 0)
	if err := r.finishFreshPodAttempt(ctx, build, pod, attempt); err != nil {
		t.Fatal(err)
	}
	previous = stage("Pod arm receipt pin issue", previous, podArmBudget, 16, 4)
	pod.UID = "pod-original-a11"
	if err := api.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	if err := r.completePodCreate(ctx, build, attempt); err != nil {
		t.Fatal(err)
	}
	previous = stage("Pod Create completion readback", previous, completionBudget, 5, 1)
	var observed corev1.Pod
	if err := api.Get(ctx, client.ObjectKeyFromObject(pod), &observed); err != nil {
		t.Fatal(err)
	}
	witness, err := r.witnessFromPod(build, &observed)
	if err != nil {
		t.Fatal(err)
	}
	var starting kovav1.KovaBuild
	if err := api.Get(ctx, client.ObjectKeyFromObject(build), &starting); err != nil {
		t.Fatal(err)
	}
	starting.Status.Phase = kovav1.PhaseStarting
	starting.Status.RunnerPodName = observed.Name
	starting.Status.AllocatedConcurrency = 1
	starting.Status.AdmissionGenesisWitness = witness
	if err := api.Status().Update(ctx, &starting); err != nil {
		t.Fatal(err)
	}
	if err := api.Get(ctx, client.ObjectKeyFromObject(build), &starting); err != nil {
		t.Fatal(err)
	}
	previous = stage("Starting fixture status only", previous, 0, 0, 0)
	if _, _, err := r.directGenesisGrant(ctx, &starting); err != nil {
		t.Fatal(err)
	}
	previous = stage("direct runner grant qualification", previous, runnerBudget, 2, 0)
	if previous.effects != 10 {
		t.Fatalf("counted grant/Pod issue path changed effect count: %+v", previous)
	}
	// A terminal status is produced by the runner/result path, outside this
	// admission read model. From that durable boundary, count the actual
	// terminal controller path: closing fence, UID Pod deletion, receipt
	// cleanup, charge release, and (for queued builds) queue release.
	starting.Status.Phase = kovav1.PhaseSucceeded
	if err := api.Status().Update(ctx, &starting); err != nil {
		t.Fatal(err)
	}
	var terminal kovav1.KovaBuild
	if err := api.Get(ctx, client.ObjectKeyFromObject(build), &terminal); err != nil {
		t.Fatal(err)
	}
	kube := &fakeKube{podClient: counted}
	r.Kube = kube
	if _, err := r.reconcileTerminal(ctx, &terminal); err != nil {
		t.Fatal(err)
	}
	previous = stage("terminal controller cleanup", previous, terminalBudget, terminalChecks, terminalEffects)
	if len(kube.deletedUIDs) != 1 || kube.deletedUIDs[0] != pod.UID {
		t.Fatalf("terminal cleanup did not delete the exact original Pod UID: %+v", kube.deletedUIDs)
	}
	var remaining corev1.Pod
	if err := api.Get(ctx, client.ObjectKeyFromObject(pod), &remaining); !apierrors.IsNotFound(err) {
		t.Fatalf("terminal cleanup left original Pod or read failed: %v", err)
	}
	_, state, err := r.readReservations(ctx, build.Namespace)
	if err != nil || len(state.Active) != 0 {
		t.Fatalf("terminal cleanup did not release active charge: %#v err=%v", state.Active, err)
	}
	if queued {
		if _, found, lookupErr := r.queueStoreForNamespace(build.Namespace).Lookup(ctx, build.Name); lookupErr != nil || found {
			t.Fatalf("terminal cleanup did not release queued charge: found=%t err=%v", found, lookupErr)
		}
	}
}
