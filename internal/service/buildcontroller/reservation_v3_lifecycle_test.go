package buildcontroller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/cofy-x/kova/internal/admissioncontract"
	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/runner"
	"github.com/cofy-x/kova/internal/service/config"
	"github.com/cofy-x/kova/internal/service/queueadmission"
	"github.com/cofy-x/kova/internal/service/recoveryreceipt"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// Only the publicly validated schema-2 queue representation is duplicated;
// the product validator remains the authority for every probe write/read.
type capacityQueueState struct {
	Version        int                              `json:"version"`
	GlobalLimit    int                              `json:"globalLimit"`
	RequesterLimit int                              `json:"requesterLimit"`
	Intents        map[string]queueadmission.Intent `json:"intents"`
}

func fullCapacityQueueState() capacityQueueState {
	s := capacityQueueState{Version: 2, GlobalLimit: 1000, RequesterLimit: 1000, Intents: map[string]queueadmission.Intent{}}
	for i := 0; i < 1000; i++ {
		name := fmt.Sprintf("%04d", i) + strings.Repeat("a", 249)
		s.Intents[name] = queueadmission.Intent{RequesterHash: strings.Repeat("a", 64), RequestDigest: strings.Repeat("b", 64),
			Nonce: strings.Repeat("c", 32), CreatedAtUnix: math.MaxInt64, ReceiptUID: strings.Repeat("R", 64),
			ReceiptDigest: "sha256:" + strings.Repeat("d", 64), CleanupKind: "terminal"}
	}
	return s
}

func encodeCapacityQueue(t *testing.T, s capacityQueueState) string {
	t.Helper()
	raw, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	if err := queueadmission.ValidateQueueLedgerForGenesis(&corev1.ConfigMap{Data: map[string]string{queueadmission.LedgerDataKey: string(raw)}}, 1000, 1000); err != nil {
		t.Fatalf("invalid queue storage probe: %v", err)
	}
	return string(raw)
}

// The same lifecycle runs with a fake client in normal CI and a pinned real
// API only under the separately opt-in manifest. It never calls Pod Create.
// Clearing the issue is based on that local, definitive never-sent fact, NOT
// a NotFound read, timeout, leader death, or unknown-response inference.
type capacityResult struct {
	MaxIssueActiveBytes     int `json:"maxIssueActiveBytes"`
	MaxCleanupActiveBytes   int `json:"maxCleanupActiveBytes"`
	MaxQueueBytes           int `json:"maxQueueBytes"`
	AfterReleaseActiveBytes int `json:"afterReleaseActiveBytes"`
	AfterReleaseQueueBytes  int `json:"afterReleaseQueueBytes"`
}

func exerciseV3CapacityLifecycle(t *testing.T, ctx context.Context, r *KovaBuildReconciler, build *kovav1.KovaBuild, realAPI bool, checkpoint func(string)) capacityResult {
	t.Helper()
	var result capacityResult
	check := func(stage string) {
		if checkpoint != nil {
			checkpoint(stage)
		}
	}
	active, _, err := r.readReservations(ctx, r.Cfg.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	for _, cleanup := range []bool{false, true} {
		state := fullGenesisReservationState(cleanup)
		if err := r.writeReservations(ctx, active, state); err != nil {
			t.Fatalf("full schema-2 active write: %v", err)
		}
		var observed reservationState
		active, observed, err = r.readReservations(ctx, r.Cfg.Namespace)
		if err != nil || len(observed.Active) != 128 {
			t.Fatalf("full active readback: %v", err)
		}
		expected := mustEncodeReservation(t, state)
		if active.Data[reservationDataKey] != string(expected) {
			t.Fatal("full active bytes changed")
		}
		t.Logf("v3 active storage (%t cleanup): %d JSON bytes", cleanup, len(expected))
		if cleanup {
			result.MaxCleanupActiveBytes = len(expected)
		} else {
			result.MaxIssueActiveBytes = len(expected)
		}
	}
	check("full-active-roundtrip")
	queue := r.queueStoreForNamespace(r.Cfg.Namespace)
	var queueCM corev1.ConfigMap
	if err := r.reader().Get(ctx, client.ObjectKey{Namespace: r.Cfg.Namespace, Name: queueadmission.ConfigMapName}, &queueCM); err != nil {
		t.Fatal(err)
	}
	qs := fullCapacityQueueState()
	if err := r.Genesis.PatchLedgerData(ctx, &queueCM, admissioncontract.Queue, encodeCapacityQueue(t, qs)); err != nil {
		t.Fatal(err)
	}
	if err := r.reader().Get(ctx, client.ObjectKeyFromObject(&queueCM), &queueCM); err != nil {
		t.Fatal(err)
	}
	if queueCM.Data[queueadmission.LedgerDataKey] != encodeCapacityQueue(t, qs) {
		t.Fatal("full queue bytes changed")
	}
	t.Logf("v3 queue storage: %d JSON bytes", len(queueCM.Data[queueadmission.LedgerDataKey]))
	result.MaxQueueBytes = len(queueCM.Data[queueadmission.LedgerDataKey])
	check("full-queue-roundtrip")
	if _, _, err := queue.Reserve(ctx, build); !errors.Is(err, queueadmission.ErrFull) {
		t.Fatalf("full queue admitted a new intent: %v", err)
	}
	var refused corev1.ConfigMap
	if err := r.reader().Get(ctx, client.ObjectKeyFromObject(&queueCM), &refused); err != nil || refused.UID != queueCM.UID ||
		refused.ResourceVersion != queueCM.ResourceVersion || refused.Data[queueadmission.LedgerDataKey] != queueCM.Data[queueadmission.LedgerDataKey] {
		t.Fatal("full queue refusal changed original ledger state")
	}
	// Admin storage setup replaces exactly one synthetic probe with this
	// test's one real CR. It is not evidence of HTTP or scheduler admission.
	delete(qs.Intents, strings.Repeat("0", 4)+strings.Repeat("a", 249))
	if err := r.Genesis.PatchLedgerData(ctx, &queueCM, admissioncontract.Queue, encodeCapacityQueue(t, qs)); err != nil {
		t.Fatal(err)
	}
	intent, fresh, err := queue.Reserve(ctx, build)
	if err != nil || !fresh {
		t.Fatalf("queue reserve at 999: %v", err)
	}
	qi, err := queue.ReceiptIntent(build, intent)
	if err != nil {
		t.Fatal(err)
	}
	witness, err := recoveryreceipt.RecordQueueOnce(ctx, r.RecoveryReceipts, qi)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.PinReceipt(ctx, build, intent, witness); err != nil {
		t.Fatal(err)
	}
	build.Annotations = map[string]string{queueadmission.IntentAnnotation: intent.Nonce,
		queueadmission.ReceiptUIDAnnotation: witness.ReceiptUID, queueadmission.ReceiptDigestAnnotation: witness.DataDigest}
	if err := r.Create(ctx, build); err != nil {
		t.Fatal(err)
	}
	if build.UID == "" {
		t.Fatal("CR Create lacks an assigned UID; retain state")
	}
	if err := r.reader().Get(ctx, client.ObjectKeyFromObject(build), build); err != nil {
		t.Fatal(err)
	}
	active, state, err := r.readReservations(ctx, r.Cfg.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	delete(state.Active, fmt.Sprintf("%04d", 127)+strings.Repeat("<", 252))
	grant := activeReservation{BuildName: build.Name, Requester: build.Spec.Requester.Username, Slots: 1,
		GrantNonce: strings.Repeat("9", 32), GrantFence: math.MaxUint64}
	state.Active[reservationKey(build)] = grant
	if err := r.writeReservations(ctx, active, state); err != nil {
		t.Fatal(err)
	}
	if err := r.finishFreshGrant(ctx, build, grant); err != nil {
		t.Fatal(err)
	}
	attempt, err := r.beginPodCreate(ctx, build)
	if err != nil {
		t.Fatal(err)
	}
	pod := runner.PreparePod(runner.ManifestOptions{PodName: buildPodName(build.Name), Namespace: build.Namespace,
		Image: r.Cfg.RunnerImage, ImagePullPolicy: "Never", SourceURI: build.Spec.Source.URI, SourceDigest: build.Spec.Source.Digest,
		Labels: map[string]string{"kova.cofy.dev/build-id": build.Name}})
	prepareGenesisPodDefaults(&pod)
	if err := controllerutil.SetControllerReference(build, &pod, r.Scheme); err != nil {
		t.Fatal(err)
	}
	if err := r.stampGenesisPod(ctx, build, &pod, attempt); err != nil {
		t.Fatal(err)
	}
	if err := r.finishFreshPodAttempt(ctx, build, &pod, attempt); err != nil {
		t.Fatal(err)
	}
	stale, staleState, err := r.readReservations(ctx, build.Namespace)
	if err != nil || len(staleState.Active) != 128 || len(staleState.Active[reservationKey(build)].InFlight) != 1 {
		t.Fatalf("full issued nonce was not persisted: %v", err)
	}
	check("receipt-and-issue-pinned-no-pod-create")
	// Deliberate direct status fixture: this does not execute cancellation or
	// claim observation of a real runner or successful build.
	build.Status.Phase = kovav1.PhaseCancelled
	if err := r.Status().Update(ctx, build); err != nil {
		t.Fatal(err)
	}
	if err := r.reader().Get(ctx, client.ObjectKeyFromObject(build), build); err != nil {
		t.Fatal(err)
	}
	if err := r.fenceReservation(ctx, build); err != nil {
		t.Fatal(err)
	}
	_, fenced, err := r.readReservations(ctx, build.Namespace)
	if err != nil || fenced.Fence != 0 || !fenced.Active[reservationKey(build)].Closing {
		t.Fatalf("full fence wrap: %v", err)
	}
	staleState.LastGrantedRequesterHash = strings.Repeat("8", 64)
	err = r.writeReservations(ctx, stale, staleState)
	if err == nil || (realAPI && !apierrors.IsConflict(err)) {
		t.Fatalf("stale original-ledger CAS was not refused as expected: %v", err)
	}
	if err := r.releaseReservation(ctx, build); err == nil {
		t.Fatal("unresolved issue released capacity")
	}
	_, held, err := r.readReservations(ctx, build.Namespace)
	if err != nil || len(held.Active) != 128 || len(held.Active[reservationKey(build)].InFlight) != 1 {
		t.Fatal("refused release lost the held charge")
	}
	// No Pod request has been issued by this test or its sealed transport.
	if err := r.completePodCreate(ctx, build, attempt); err != nil {
		t.Fatal(err)
	}
	if _, err := r.beginPodCreate(ctx, build); err == nil {
		t.Fatal("spent/closing nonce was reused")
	}
	check("closing-fence-and-spent-nonce")
	if err := r.releaseReservation(ctx, build); err != nil {
		t.Fatal(err)
	}
	if err := queue.ReleaseForBuild(ctx, build); err != nil {
		t.Fatal(err)
	}
	releasedCM, released, err := r.readReservations(ctx, build.Namespace)
	if err != nil || len(released.Active) != 127 {
		t.Fatalf("active exact release: %v", err)
	}
	if _, found := released.Active[reservationKey(build)]; found {
		t.Fatal("original CR charge remained")
	}
	result.AfterReleaseActiveBytes = len(releasedCM.Data[reservationDataKey])
	if _, found, err := queue.Lookup(ctx, build.Name); err != nil || found {
		t.Fatalf("queue charge remained: %v", err)
	}
	if err := r.reader().Get(ctx, client.ObjectKeyFromObject(&queueCM), &queueCM); err != nil {
		t.Fatal(err)
	}
	var after capacityQueueState
	if err := json.Unmarshal([]byte(queueCM.Data[queueadmission.LedgerDataKey]), &after); err != nil || len(after.Intents) != 999 {
		t.Fatalf("queue exact release: %v", err)
	}
	result.AfterReleaseQueueBytes = len(queueCM.Data[queueadmission.LedgerDataKey])
	// Restart-safe repeat reads original terminal CR's settled receipt link.
	if err := r.releaseReservation(ctx, build); err != nil {
		t.Fatal(err)
	}
	if err := queue.ReleaseForBuild(ctx, build); err != nil {
		t.Fatal(err)
	}
	probe := build.DeepCopy()
	probe.Name, probe.UID, probe.Annotations = build.Name+"-capacity", "", nil
	if _, fresh, err := queue.Reserve(ctx, probe); err != nil || !fresh {
		t.Fatalf("released queue capacity not usable: %v", err)
	}
	check("exact-release-and-bounded-reserve")
	return result
}

func TestGenesisV3FullTableLifecycle(t *testing.T) {
	cfg := config.Config{MaxActiveJobs: 128, MaxActiveJobsPerRequester: 128, WorkerSlots: 65535,
		MaxQueuedJobs: 1000, MaxQueuedJobsPerRequester: 1000}
	r, api, build := genesisAdmissionFixtureWithConfig(t, cfg)
	// The generic fake fixture has a synthetic direct CR; remove it before
	// exercising the shared create path. No real API is opened here.
	build.Finalizers = nil
	if err := api.Update(t.Context(), build); err != nil {
		t.Fatal(err)
	}
	if err := api.Delete(t.Context(), build); err != nil {
		t.Fatal(err)
	}
	// Fake clients do not assign UIDs. The real gate never supplies one.
	build.UID, build.ResourceVersion, build.Finalizers = "fake-capacity-build", "", nil
	exerciseV3CapacityLifecycle(t, t.Context(), r, build, false, nil)
}

func TestGenesisV3OversizeProposalHasNoAPIEffect(t *testing.T) {
	cfg := config.Config{MaxActiveJobs: 128, MaxActiveJobsPerRequester: 128, WorkerSlots: 65535,
		MaxQueuedJobs: 1000, MaxQueuedJobsPerRequester: 1000}
	r, api, _ := genesisAdmissionFixtureWithConfig(t, cfg)
	for _, role := range []admissioncontract.Role{admissioncontract.Active, admissioncontract.Queue} {
		name, key := admissioncontract.ActiveLedgerName, admissioncontract.ActiveLedgerDataKey
		if role == admissioncontract.Queue {
			name, key = admissioncontract.QueueLedgerName, admissioncontract.QueueLedgerDataKey
		}
		var cm corev1.ConfigMap
		if err := r.Get(t.Context(), client.ObjectKey{Namespace: r.Cfg.Namespace, Name: name}, &cm); err != nil {
			t.Fatal(err)
		}
		old := cm.Data[key]
		oversize := old + strings.Repeat(" ", admissioncontract.MaxLedgerDataBytes+1-len(old))
		before := api.patches
		if err := r.Genesis.PatchLedgerData(t.Context(), &cm, role, oversize); err == nil {
			t.Fatal("oversize proposal accepted")
		}
		var got corev1.ConfigMap
		if err := r.Get(t.Context(), client.ObjectKeyFromObject(&cm), &got); err != nil || got.ResourceVersion != cm.ResourceVersion || got.Data[key] != old || api.patches != before {
			t.Fatal("raw byte refusal caused an API mutation")
		}
	}
}
