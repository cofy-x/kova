package queueadmission

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func testStore(t *testing.T, global, requester int) (Store, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := kovav1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	base := crfake.NewClientBuilder().WithScheme(scheme).Build()
	return Store{Client: base, Reader: base, Namespace: "jobs", GlobalLimit: global, RequesterLimit: requester}, base
}

func testBuild(id, requester string) *kovav1.KovaBuild {
	return &kovav1.KovaBuild{ObjectMeta: metav1.ObjectMeta{Name: id, Namespace: "jobs"}, Spec: kovav1.KovaBuildSpec{
		Requester: kovav1.KovaBuildRequester{Username: requester},
		Source:    kovav1.KovaBuildSourceSpec{URI: "https://sources.example.com/build.zip", Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		Build:     kovav1.KovaBuildOptions{Format: "oci", Concurrency: 1},
	}}
}

type lostLedgerUpdate struct {
	client.Client
	mu     sync.Mutex
	failed bool
}

type initiallyMissingLedgerReader struct {
	client.Reader
	seen atomic.Bool
}

func (r *initiallyMissingLedgerReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*corev1.ConfigMap); ok && key.Name == ConfigMapName && r.seen.CompareAndSwap(false, true) {
		return apierrors.NewNotFound(schema.GroupResource{Resource: "configmaps"}, key.Name)
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

func (c *lostLedgerUpdate) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if cm, ok := obj.(*corev1.ConfigMap); ok && cm.Name == ConfigMapName {
		c.mu.Lock()
		defer c.mu.Unlock()
		if !c.failed {
			c.failed = true
			if err := c.Client.Update(ctx, obj, opts...); err != nil {
				return err
			}
			return context.DeadlineExceeded
		}
	}
	return c.Client.Update(ctx, obj, opts...)
}

func TestConcurrentQueueCASAcrossReplicas(t *testing.T) {
	for _, mode := range []string{"same-requester", "global"} {
		t.Run(mode, func(t *testing.T) {
			store, base := testStore(t, 3, 2)
			other := store
			// Independent Store values represent separate Service processes.
			other.Client, other.Reader = base, base
			var wg sync.WaitGroup
			var mu sync.Mutex
			accepted := 0
			for i := 0; i < 40; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					requester := "alice"
					if mode == "global" {
						requester = fmt.Sprintf("user-%d", i)
					}
					candidate := testBuild(fmt.Sprintf("job-%d", i), requester)
					current := store
					if i%2 == 1 {
						current = other
					}
					_, fresh, err := current.Reserve(context.Background(), candidate)
					if err != nil && !errors.Is(err, ErrFull) && !errors.Is(err, ErrBusy) {
						t.Errorf("reserve job-%d: %v", i, err)
					}
					if fresh {
						mu.Lock()
						accepted++
						mu.Unlock()
					}
				}(i)
			}
			wg.Wait()
			limit := 2
			if mode == "global" {
				limit = 3
			}
			if accepted == 0 || accepted > limit {
				t.Fatalf("accepted=%d, limit=%d", accepted, limit)
			}
			_, snapshot, err := store.read(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(snapshot.Intents) != accepted {
				t.Fatalf("intents=%d accepted=%d", len(snapshot.Intents), accepted)
			}
		})
	}
}

func TestLostQueueCASResponseAuthorizesOnlyOriginalNonce(t *testing.T) {
	store, base := testStore(t, 1, 1)
	writer := &lostLedgerUpdate{Client: base}
	store.Client = writer
	build := testBuild("one", "alice")
	intent, fresh, err := store.Reserve(context.Background(), build)
	if err != nil || !fresh || !writer.failed {
		t.Fatalf("lost CAS response: intent=%#v fresh=%t error=%v failed=%t", intent, fresh, err, writer.failed)
	}
	retry, fresh, err := (Store{Client: base, Reader: base, Namespace: "jobs", GlobalLimit: 1, RequesterLimit: 1}).Reserve(context.Background(), build)
	if err != nil || fresh || retry != intent {
		t.Fatalf("duplicate issued new Create authority: %#v fresh=%t err=%v", retry, fresh, err)
	}
	if _, _, err := store.Reserve(context.Background(), testBuild("two", "alice")); !errors.Is(err, ErrFull) {
		t.Fatalf("unknown outcome freed capacity: %v", err)
	}
	build.Annotations = map[string]string{IntentAnnotation: intent.Nonce}
	if err := store.VerifyForBuild(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	build.Spec.Source.URI = "https://sources.example.com/other.zip"
	if err := store.VerifyForBuild(context.Background(), build); !errors.Is(err, ErrDrift) {
		t.Fatalf("changed build bypassed digest: %v", err)
	}
	if err := store.ReleaseForBuild(context.Background(), build); !errors.Is(err, ErrDrift) {
		t.Fatalf("changed build released its queue intent: %v", err)
	}
	if _, found, err := store.Lookup(context.Background(), build.Name); err != nil || !found {
		t.Fatalf("drifted intent was not retained: found=%t err=%v", found, err)
	}
}

func TestQueueLedgerFailsClosedOnLimitDriftAndOldBuilds(t *testing.T) {
	store, base := testStore(t, 3, 2)
	if _, fresh, err := store.Reserve(context.Background(), testBuild("one", "alice")); err != nil || !fresh {
		t.Fatalf("first reserve fresh=%t err=%v", fresh, err)
	}
	changed := store
	changed.GlobalLimit = 4
	if _, _, err := changed.Reserve(context.Background(), testBuild("two", "bob")); err == nil {
		t.Fatal("replica with different global limit accepted work")
	}
	if err := base.Delete(context.Background(), &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: ConfigMapName, Namespace: "jobs"}}); err != nil {
		t.Fatal(err)
	}
	if err := base.Create(context.Background(), testBuild("legacy", "alice")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Reserve(context.Background(), testBuild("new", "bob")); err == nil {
		t.Fatal("missing ledger with old CRs did not fail closed")
	}
}

func TestInitializationRereadsLedgerWhenAnotherReplicaCreatedBuild(t *testing.T) {
	store, base := testStore(t, 2, 2)
	first := testBuild("first", "alice")
	intent, fresh, err := store.Reserve(context.Background(), first)
	if err != nil || !fresh {
		t.Fatalf("first intent fresh=%t err=%v", fresh, err)
	}
	first.Annotations = map[string]string{IntentAnnotation: intent.Nonce}
	if err := base.Create(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	staleFirstGet := store
	staleFirstGet.Reader = &initiallyMissingLedgerReader{Reader: base}
	if _, fresh, err := staleFirstGet.Reserve(context.Background(), testBuild("second", "bob")); err != nil || !fresh {
		t.Fatalf("another replica's CR was mistaken for legacy state: fresh=%t err=%v", fresh, err)
	}
}

func TestQueueLedgerRejectsCorruptRequesterCount(t *testing.T) {
	store, base := testStore(t, 3, 1)
	if _, fresh, err := store.Reserve(context.Background(), testBuild("one", "alice")); err != nil || !fresh {
		t.Fatalf("first reserve fresh=%t err=%v", fresh, err)
	}
	var cm corev1.ConfigMap
	if err := base.Get(context.Background(), store.key(), &cm); err != nil {
		t.Fatal(err)
	}
	var snapshot state
	if err := json.Unmarshal([]byte(cm.Data[dataKey]), &snapshot); err != nil {
		t.Fatal(err)
	}
	snapshot.Intents["two"] = snapshot.Intents["one"]
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	cm.Data[dataKey] = string(data)
	if err := base.Update(context.Background(), &cm); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Reserve(context.Background(), testBuild("three", "bob")); err == nil {
		t.Fatal("corrupt per-requester ledger admitted another build")
	}
}

func TestMaximumQueueLedgerFitsBelowConfigMapLimit(t *testing.T) {
	store, _ := testStore(t, 1000, 1000)
	snapshot := store.freshState()
	for i := 0; i < 1000; i++ {
		id := fmt.Sprintf("idem-%020d", i)
		snapshot.Intents[id] = Intent{
			RequesterHash: HashRequester(fmt.Sprintf("requester-%d", i)),
			RequestDigest: HashRequester(fmt.Sprintf("request-%d", i)),
			Nonce:         "00112233445566778899aabbccddeeff",
			CreatedAtUnix: 1,
		}
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) >= maxLedgerBytes {
		t.Fatalf("1000 intents require %d bytes, exceeding %d-byte guard", len(data), maxLedgerBytes)
	}
}
