package queueadmission

import (
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/cofy-x/kova/internal/service/realapitest"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// TestRealAPIQueueFullTable is opt-in and uses its own fresh explicit
// namespace. It exercises the actual API server's ConfigMap size and CAS
// behavior before ReleaseRejected removes one of 1000 intents.
func TestRealAPIQueueFullTable(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	g := realapitest.Open(t, scheme)
	ctx := t.Context()
	current := state{Version: 1, GlobalLimit: 1000, RequesterLimit: 1000, Intents: make(map[string]Intent, 1000)}
	for i := 0; i < 1000; i++ {
		id := fmt.Sprintf("%04d", i) + strings.Repeat("a", 249)
		current.Intents[id] = Intent{RequesterHash: strings.Repeat("a", 64), RequestDigest: strings.Repeat("b", 64),
			Nonce: strings.Repeat("c", 32), CreatedAtUnix: math.MaxInt64}
	}
	full, err := encodeState(current)
	if err != nil {
		t.Fatal(err)
	}
	if len(full) > maxLedgerBytes {
		t.Fatalf("unexpected queue table size: %d bytes", len(full))
	}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: g.Namespace, Name: ConfigMapName},
		Data: map[string]string{dataKey: string(full)}}
	g.CreateOwned(t, cm)
	cmUID := cm.UID
	readCM := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: g.Namespace, Name: ConfigMapName}}
	g.GetOwned(t, readCM, cmUID)
	if readCM.Data[dataKey] != string(full) {
		t.Fatal("API server did not round-trip the full 1000-intent queue table")
	}
	t.Logf("queue full table round-tripped: %d bytes", len(full))
	stale := readCM.DeepCopy()
	staleRV := stale.ResourceVersion
	id := strings.Repeat("0", 4) + strings.Repeat("a", 249)
	entry := current.Intents[id]
	entry.CreatedAtUnix--
	current.Intents[id] = entry
	updated, err := encodeState(current)
	if err != nil {
		t.Fatal(err)
	}
	readCM.Data = map[string]string{dataKey: string(updated)}
	g.UpdateOwned(t, readCM, cmUID)
	g.GetOwned(t, readCM, cmUID)
	if readCM.Data[dataKey] != string(updated) || readCM.ResourceVersion == staleRV {
		t.Fatal("API server did not preserve the full-table queue update or advance its resourceVersion")
	}
	t.Logf("queue full-table CAS UID=%s RV=%s -> %s", cmUID, staleRV, readCM.ResourceVersion)
	g.GetOwned(t, readCM, cmUID)
	if err := g.Client.Update(ctx, stale); !apierrors.IsConflict(err) {
		t.Fatalf("stale queue ConfigMap write = %v, want Conflict", err)
	}
	store := Store{Client: g.Client, Reader: g.Client, Namespace: g.Namespace, GlobalLimit: 1000, RequesterLimit: 1000}
	g.GetOwned(t, readCM, cmUID)
	if err := store.ReleaseRejected(ctx, id, entry.Nonce); err != nil {
		t.Fatalf("real API ReleaseRejected at full capacity: %v", err)
	}
	g.GetOwned(t, readCM, cmUID)
	_, released, err := store.read(ctx)
	if err != nil || len(released.Intents) != 999 {
		t.Fatalf("real API ReleaseRejected: intents=%d err=%v", len(released.Intents), err)
	}
	if _, found := released.Intents[id]; found {
		t.Fatal("real API rejected intent remained charged")
	}
	// Success-only UID-preconditioned cleanup; leave partial state and the
	// namespace to the operator on any failure or uncertain API response.
	g.DeleteOwned(t, readCM)
}
