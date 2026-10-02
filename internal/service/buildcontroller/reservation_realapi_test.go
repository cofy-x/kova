package buildcontroller

import (
	"math"
	"strings"
	"testing"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/config"
	"github.com/cofy-x/kova/internal/service/realapitest"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestRealAPIActiveFullTable is opt-in and requires a dedicated Kind cluster
// with the KovaBuild CRD, but no running Service/controller. It verifies the
// API server's actual ConfigMap storage and resourceVersion CAS behavior.
func TestRealAPIActiveFullTable(t *testing.T) {
	g := realapitest.Open(t, testScheme(t))
	ctx := t.Context()
	state, syntheticUID := fullReservationState()
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
	if len(full) > maxReservationLedgerBytes || maxState.Fence != math.MaxUint64 {
		t.Fatalf("unexpected max-table fixture: bytes=%d fence=%d", len(full), maxState.Fence)
	}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: g.Namespace, Name: reservationConfigMap},
		Data: map[string]string{reservationDataKey: string(full)}}
	g.CreateOwned(t, cm)
	cmUID := cm.UID
	readCM := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: g.Namespace, Name: reservationConfigMap}}
	g.GetOwned(t, readCM, cmUID)
	if readCM.Data[reservationDataKey] != string(full) {
		t.Fatal("API server did not round-trip the fully expanded active table")
	}
	t.Logf("active full table round-tripped: %d bytes", len(full))

	// A synthetic worst-case UID cannot be assigned to a real KovaBuild. Use
	// one actual server-assigned UID for the lifecycle while retaining 127
	// worst-case synthetic entries and the maximum slot sum.
	grant := state.Active[syntheticUID]
	build := &kovav1.KovaBuild{
		ObjectMeta: metav1.ObjectMeta{Namespace: g.Namespace, Name: "kova-rc-active-probe"},
		Spec: kovav1.KovaBuildSpec{
			Requester: kovav1.KovaBuildRequester{Username: grant.Requester},
			Targets:   []kovav1.KovaBuildTargetSpec{{Target: "example.invalid/kova:probe", Platform: "linux/amd64"}},
			Source:    kovav1.KovaBuildSourceSpec{URI: "https://example.invalid/source.tar.gz", Digest: "sha256:" + strings.Repeat("a", 64)},
			Build:     kovav1.KovaBuildOptions{Concurrency: 1},
		},
	}
	g.CreateOwned(t, build)
	buildUID := build.UID
	readBuild := &kovav1.KovaBuild{ObjectMeta: metav1.ObjectMeta{Namespace: g.Namespace, Name: build.Name}}
	g.GetOwned(t, readBuild, buildUID)
	if readBuild.Spec.Requester.Username != grant.Requester {
		t.Fatal("API server changed the probe build requester")
	}
	delete(state.Active, syntheticUID)
	grant.BuildName = build.Name
	state.Active[string(buildUID)] = grant
	base, err := encodeReservations(state)
	if err != nil {
		t.Fatal(err)
	}
	readCM.Data = map[string]string{reservationDataKey: string(base)}
	g.UpdateOwned(t, readCM, cmUID)
	g.GetOwned(t, readCM, cmUID)
	if readCM.Data[reservationDataKey] != string(base) {
		t.Fatal("API server did not round-trip the active-table update")
	}
	r := KovaBuildReconciler{Client: g.Client, APIReader: g.Client,
		Cfg: config.Config{MaxActiveJobs: 128, MaxActiveJobsPerRequester: 128, WorkerSlots: 65535}}
	g.GetOwned(t, readCM, cmUID)
	attempt, err := r.beginPodCreate(ctx, readBuild)
	if err != nil {
		t.Fatalf("real API beginPodCreate at full capacity: %v", err)
	}
	g.Check(t)
	_, withNonce, err := r.readReservations(ctx, g.Namespace)
	if err != nil || len(withNonce.Active[string(buildUID)].InFlight) != 1 || withNonce.Active[string(buildUID)].InFlight[0] != attempt {
		t.Fatalf("real API did not persist Pod nonce: entry=%#v err=%v", withNonce.Active[string(buildUID)], err)
	}
	g.GetOwned(t, readCM, cmUID)
	if err := r.completePodCreate(ctx, readBuild, attempt); err != nil {
		t.Fatalf("real API completePodCreate: %v", err)
	}
	g.Check(t)
	stale, staleState, err := r.readReservations(ctx, g.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	staleRV := stale.ResourceVersion
	g.GetOwned(t, readCM, cmUID)
	if err := r.fenceReservation(ctx, readBuild); err != nil {
		t.Fatalf("real API fenceReservation: %v", err)
	}
	g.Check(t)
	current, fenced, err := r.readReservations(ctx, g.Namespace)
	if err != nil || fenced.Fence != 0 || !fenced.Active[string(buildUID)].Closing {
		t.Fatalf("real API fence wrap: fence=%d entry=%#v err=%v", fenced.Fence, fenced.Active[string(buildUID)], err)
	}
	t.Logf("active fence CAS UID=%s RV=%s -> %s", cmUID, staleRV, current.ResourceVersion)
	staleState.LastGrantedRequesterHash = strings.Repeat("c", 64)
	g.GetOwned(t, readCM, cmUID)
	if err := r.writeReservations(ctx, stale, staleState); !apierrors.IsConflict(err) {
		t.Fatalf("stale resourceVersion write = %v, want Conflict", err)
	}
	g.GetOwned(t, readCM, cmUID)
	if err := r.releaseReservation(ctx, readBuild); err != nil {
		t.Fatalf("real API releaseReservation: %v", err)
	}
	g.Check(t)
	_, released, err := r.readReservations(ctx, g.Namespace)
	if err != nil || len(released.Active) != 127 {
		t.Fatalf("real API release: active=%d err=%v", len(released.Active), err)
	}
	if _, found := released.Active[string(buildUID)]; found {
		t.Fatal("real API released grant remained charged")
	}
	g.GetOwned(t, readCM, cmUID)
	// Cleanup is success-only. Any partial/uncertain result is left intact for
	// operator inspection; the namespace is left for exact-cluster cleanup.
	g.DeleteOwned(t, readBuild)
	g.DeleteOwned(t, readCM)
}
