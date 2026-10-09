package buildcontroller

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/config"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestReservationCapacityRejectsUnsupportedConfiguration(t *testing.T) {
	valid := config.Config{MaxActiveJobs: 128, MaxActiveJobsPerRequester: 128, WorkerSlots: 65535}
	if err := ValidateReservationCapacity(valid); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*config.Config){
		func(c *config.Config) { c.MaxActiveJobs = 0 },
		func(c *config.Config) { c.MaxActiveJobs = 129 },
		func(c *config.Config) { c.MaxActiveJobsPerRequester = 0 },
		func(c *config.Config) { c.MaxActiveJobsPerRequester = 129 },
		func(c *config.Config) { c.WorkerSlots = 0 },
		func(c *config.Config) { c.WorkerSlots = 65536 },
	} {
		invalid := valid
		change(&invalid)
		if err := ValidateReservationCapacity(invalid); err == nil {
			t.Fatalf("unsupported capacity accepted: %+v", invalid)
		}
	}
}

func TestGenesisActiveSchemaRejectsLegacyAndMissingGrantNonce(t *testing.T) {
	cfg := config.Config{MaxActiveJobs: 2, MaxActiveJobsPerRequester: 2, WorkerSlots: 2}
	data, err := GenesisEmptyAdmissionData(cfg)
	if err != nil {
		t.Fatal(err)
	}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "jobs", Name: reservationConfigMap},
		Data: map[string]string{reservationDataKey: data}}
	if err := ValidateAdmissionLedgerForGenesis(cm, cfg); err != nil {
		t.Fatalf("fresh v2 active ledger was refused: %v", err)
	}
	state, err := decodeReservations(cm)
	if err != nil || state.Version != 2 || state.Fence != 1 {
		t.Fatalf("fresh active header=%+v err=%v", state, err)
	}
	state.Version = 1
	state.Fence = 0
	legacy, err := encodeReservations(state)
	if err != nil {
		t.Fatal(err)
	}
	cm.Data[reservationDataKey] = string(legacy)
	if err := ValidateAdmissionLedgerForGenesis(cm, cfg); err == nil {
		t.Fatal("Genesis adopted a legacy active ledger")
	}
	cm.Data[reservationDataKey] = `{"version":2,"fence":1,"maxJobs":2,"maxPerRequester":2,"workerSlots":2,"active":{"build-uid":{"buildName":"build","requester":"alice","slots":1}}}`
	if err := ValidateAdmissionLedgerForGenesis(cm, cfg); err == nil {
		t.Fatal("Genesis accepted active entry without a stable grant nonce")
	}
}

func fullReservationState() (reservationState, string) {
	state := reservationState{Version: 1, Fence: math.MaxUint64, MaxJobs: 128, MaxPerRequester: 128, WorkerSlots: 65535,
		LastGrantedRequesterHash: strings.Repeat("a", 64), Active: make(map[string]activeReservation, 128)}
	var first string
	for i := 0; i < 128; i++ {
		uid := fmt.Sprintf("%04d", i) + strings.Repeat("<", 252)
		name := fmt.Sprintf("%04d", i) + strings.Repeat("a", 249)
		slots := 1
		if i == 0 {
			slots = 65408 // 65408 + 127 = the maximum supported 65535 slots.
		}
		state.Active[uid] = activeReservation{BuildName: name, Requester: strings.Repeat("<", 253), Slots: slots}
		if i == 0 {
			first = uid
		}
	}
	return state, first
}

func TestGenesisFullReservationTableHasReceiptAndFuturePodHeadroom(t *testing.T) {
	state := reservationState{Version: 2, Fence: math.MaxUint64, MaxJobs: 128, MaxPerRequester: 128, WorkerSlots: 65535,
		LastGrantedRequesterHash: strings.Repeat("a", 64), Active: make(map[string]activeReservation, 128)}
	for i := 0; i < 128; i++ {
		uid := fmt.Sprintf("%04d", i) + strings.Repeat("<", 252)
		name := fmt.Sprintf("%04d", i) + strings.Repeat("a", 249)
		slots := 100
		if i < 58 {
			slots = 1000 // 58,000 + 7,000 = 65,000 slots.
		}
		state.Active[uid] = activeReservation{
			BuildName: name, Requester: strings.Repeat("<", 253), Slots: slots,
			GrantNonce: strings.Repeat("b", 32), GrantFence: math.MaxUint64,
			GrantObservedRV: strings.Repeat("<", maxGrantObservedRVBytes),
			GrantReceiptUID: strings.Repeat("R", 64), GrantReceiptDigest: "sha256:" + strings.Repeat("d", 64),
			PodAttemptNonce: strings.Repeat("e", 32), PodTemplateDigest: "sha256:" + strings.Repeat("f", 64),
			PodReceiptUID: strings.Repeat("P", 64), PodReceiptDigest: "sha256:" + strings.Repeat("c", 64),
			Closing: true, InFlight: []string{strings.Repeat("e", 32)},
		}
	}
	withPodAttempt, err := encodeReservations(state)
	if err != nil {
		t.Fatal(err)
	}
	for key, entry := range state.Active {
		entry.InFlight = nil
		entry.PodCleanupReady = true
		entry.GrantCleanupReady = true
		state.Active[key] = entry
	}
	withCleanupMarker, err := encodeReservations(state)
	if err != nil {
		t.Fatal(err)
	}
	bound := maxReservationHeaderBytes + MaxActiveJobs*maxReservationEntryBytes
	if len(withPodAttempt) > bound || len(withCleanupMarker) > bound || bound > maxReservationLedgerBytes {
		t.Fatalf("full Genesis active table exceeds conservative bound: pod=%d cleanup=%d bound=%d max=%d",
			len(withPodAttempt), len(withCleanupMarker), bound, maxReservationLedgerBytes)
	}
	t.Logf("128-entry Genesis active JSON: Pod attempt=%d cleanup=%d; reserved bound=%d; guard=%d",
		len(withPodAttempt), len(withCleanupMarker), bound, maxReservationLedgerBytes)
}

func TestFullReservationTableKeepsNonceClosingFenceAndReleaseHeadroom(t *testing.T) {
	ctx := context.Background()
	state, uid := fullReservationState()
	initial, err := encodeReservations(state)
	if err != nil {
		t.Fatal(err)
	}
	maxState := state
	maxState.Active = make(map[string]activeReservation, len(state.Active))
	for key, grant := range state.Active {
		grant.Closing = true
		grant.InFlight = []string{strings.Repeat("b", 32)}
		maxState.Active[key] = grant
	}
	full, err := encodeReservations(maxState)
	if err != nil {
		t.Fatal(err)
	}
	if len(full) > maxReservationHeaderBytes+128*maxReservationEntryBytes || len(full) > maxReservationLedgerBytes {
		t.Fatalf("full encoded active ledger = %d bytes", len(full))
	}
	t.Logf("full active JSON = %d bytes; 768 KiB headroom = %d bytes; before nonce/closing = %d", len(full), maxReservationLedgerBytes-len(full), len(initial))
	grant := state.Active[uid]
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "jobs", Name: reservationConfigMap},
		Data: map[string]string{reservationDataKey: string(initial)}}
	build := &kovav1.KovaBuild{ObjectMeta: metav1.ObjectMeta{Namespace: "jobs", Name: grant.BuildName, UID: types.UID(uid)},
		Spec: kovav1.KovaBuildSpec{Requester: kovav1.KovaBuildRequester{Username: grant.Requester}}}
	base := crfake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(cm, build).Build()
	r := KovaBuildReconciler{Client: base, APIReader: base, Cfg: config.Config{MaxActiveJobs: 128, MaxActiveJobsPerRequester: 128, WorkerSlots: 65535}}
	attempt, err := r.beginPodCreate(ctx, build)
	if err != nil {
		t.Fatalf("full active table could not durably append Pod nonce: %v", err)
	}
	_, withNonce, err := r.readReservations(ctx, "jobs")
	if err != nil || len(withNonce.Active[uid].InFlight) != 1 || withNonce.Active[uid].InFlight[0] != attempt {
		t.Fatalf("full active table lost Pod nonce: entry=%#v err=%v", withNonce.Active[uid], err)
	}
	if err := r.completePodCreate(ctx, build, attempt); err != nil {
		t.Fatalf("full active table could not resolve Pod nonce: %v", err)
	}
	stale, staleState, err := r.readReservations(ctx, "jobs")
	if err != nil {
		t.Fatal(err)
	}
	if err := r.fenceReservation(ctx, build); err != nil {
		t.Fatal(err)
	}
	_, wrapped, err := r.readReservations(ctx, "jobs")
	if err != nil || wrapped.Fence != 0 || !wrapped.Active[uid].Closing {
		t.Fatalf("fence wrap lost cleanup authority: fence=%d err=%v", wrapped.Fence, err)
	}
	staleState.LastGrantedRequesterHash = strings.Repeat("c", 64)
	if err := r.writeReservations(ctx, stale, staleState); err == nil {
		t.Fatal("pre-fence writer bypassed resourceVersion conflict at counter wrap")
	}
	if err := r.releaseReservation(ctx, build); err != nil {
		t.Fatal(err)
	}
	_, released, err := r.readReservations(ctx, "jobs")
	if err != nil || len(released.Active) != 127 {
		t.Fatalf("release at full capacity: active=%d err=%v", len(released.Active), err)
	}
	if _, found := released.Active[uid]; found {
		t.Fatal("released grant remained charged")
	}
}

func mustEncodeReservation(t *testing.T, state reservationState) []byte {
	t.Helper()
	encoded, err := encodeReservations(state)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestReservationDecodeRejectsUnboundedOrLossyState(t *testing.T) {
	base := reservationState{Version: 1, MaxJobs: 1, MaxPerRequester: 1, WorkerSlots: 1, Active: map[string]activeReservation{
		"uid": {BuildName: "build", Requester: "alice", Slots: 1},
	}}
	for name, mutate := range map[string]func(*reservationState){
		"too many grants": func(s *reservationState) {
			s.Active["other"] = activeReservation{BuildName: "other", Requester: "bob", Slots: 1}
		},
		"too many slots": func(s *reservationState) { grant := s.Active["uid"]; grant.Slots = 2; s.Active["uid"] = grant },
		"long uid": func(s *reservationState) {
			s.Active[strings.Repeat("u", 257)] = s.Active["uid"]
			delete(s.Active, "uid")
		},
		"long name": func(s *reservationState) {
			grant := s.Active["uid"]
			grant.BuildName = strings.Repeat("a", 254)
			s.Active["uid"] = grant
		},
		"long requester": func(s *reservationState) {
			grant := s.Active["uid"]
			grant.Requester = strings.Repeat("界", 254)
			s.Active["uid"] = grant
		},
		"two nonces": func(s *reservationState) {
			grant := s.Active["uid"]
			grant.InFlight = []string{strings.Repeat("a", 32), strings.Repeat("b", 32)}
			s.Active["uid"] = grant
		},
		"bad nonce": func(s *reservationState) {
			grant := s.Active["uid"]
			grant.InFlight = []string{strings.Repeat("G", 32)}
			s.Active["uid"] = grant
		},
		"bad cursor": func(s *reservationState) { s.LastGrantedRequesterHash = strings.Repeat("G", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			state := base
			state.Active = map[string]activeReservation{"uid": base.Active["uid"]}
			mutate(&state)
			if err := validateReservationState(state); err == nil {
				t.Fatal("invalid active ledger was accepted")
			}
		})
	}
	valid := string(mustEncodeReservation(t, base))
	for _, raw := range []string{
		strings.Replace(valid, `"active":`, `"unknown":0,"active":`, 1),
		strings.Replace(valid, `"active":`, `"active":{},"active":`, 1),
		strings.Replace(valid, `"active":`, `"\u0061ctive":{},"active":`, 1),
		strings.Replace(valid, `"fence":0`, `"fence":null`, 1),
		strings.Replace(valid, `"alice"`, `"\ud800"`, 1),
	} {
		cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "jobs", Name: reservationConfigMap}, Data: map[string]string{reservationDataKey: raw}}
		if _, err := decodeReservations(cm); err == nil {
			t.Fatalf("lossy active JSON accepted: %s", raw)
		}
	}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "jobs", Name: reservationConfigMap}, Data: map[string]string{reservationDataKey: valid, "other": "evidence"}}
	if _, err := decodeReservations(cm); err == nil {
		t.Fatal("extra ConfigMap data would be silently removed")
	}
}

func TestMissingActiveLedgerDoesNotCompleteCapacityRelease(t *testing.T) {
	ctx := context.Background()
	base := crfake.NewClientBuilder().WithScheme(testScheme(t)).Build()
	r := KovaBuildReconciler{Client: base, APIReader: base, Cfg: config.Config{MaxActiveJobs: 1, MaxActiveJobsPerRequester: 1, WorkerSlots: 1}}
	build := &kovav1.KovaBuild{ObjectMeta: metav1.ObjectMeta{Namespace: "jobs", Name: "build", UID: "uid"}}
	if err := r.releaseReservation(ctx, build); err == nil {
		t.Fatal("missing active ledger was treated as released")
	}
	var cm corev1.ConfigMap
	if err := base.Get(ctx, client.ObjectKey{Namespace: "jobs", Name: reservationConfigMap}, &cm); err == nil {
		t.Fatal("release recreated the missing ledger")
	}
}
