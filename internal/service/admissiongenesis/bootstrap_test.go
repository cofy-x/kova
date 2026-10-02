package admissiongenesis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"

	jsonpatch "github.com/evanphx/json-patch/v5"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

var errLostResponse = errors.New("simulated lost API response")

func TestFreshBootstrapRechecksExternalReceiptBeforeEachEffect(t *testing.T) {
	f, bootstrap := testBootstrap(t)
	checks := 0
	bootstrap.BeforeEffect = func(context.Context) error {
		checks++
		if checks == 2 {
			return ErrChanged
		}
		return nil
	}
	if _, err := bootstrap.EnsureFresh(context.Background()); !errors.Is(err, ErrChanged) {
		t.Fatalf("revoked receipt did not stop provisional Genesis pin: %v", err)
	}
	if checks != 2 || f.createCount[ActiveLedgerName] != 1 || f.patchCount[GenesisName] != 0 ||
		f.createCount[QueueLedgerName] != 0 {
		t.Fatalf("bootstrap continued after receipt revocation: checks=%d creates=%v patches=%v", checks, f.createCount, f.patchCount)
	}
}

type responseEffect int

const (
	respondNormally responseEffect = iota
	rejectBeforeWrite
	rejectInvalidBeforeWrite
	loseAfterWrite
)

// fakeCore models a serialized API server: Create is name-unique and each
// JSON Patch is applied to one object with UID/resourceVersion tests. Effects
// inject a lost response either before or after persistence, not a retry.
type fakeCore struct {
	mu            sync.Mutex
	namespace     *corev1.Namespace
	objects       map[string]*corev1.ConfigMap
	nextUID       int
	nextRV        int
	createEffects map[string][]responseEffect
	patchEffects  map[string][]responseEffect
	createCount   map[string]int
	patchCount    map[string]int
	beforeGet     map[string]func(*fakeCore)
	beforePatch   map[string]func(*fakeCore)
}

func newFakeCore(t *testing.T, r Receipt) *fakeCore {
	t.Helper()
	return &fakeCore{
		namespace: &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: r.Namespace, UID: types.UID(r.Contract.NamespaceUID)},
			Status: corev1.NamespaceStatus{Phase: corev1.NamespaceActive}},
		objects: map[string]*corev1.ConfigMap{r.GenesisName: genesisObject(t, r,
			GenesisData{Contract: r.Contract, Phase: PhaseInitializing}, nil)},
		nextUID: 1, nextRV: 1,
		createEffects: map[string][]responseEffect{}, patchEffects: map[string][]responseEffect{},
		createCount: map[string]int{}, patchCount: map[string]int{}, beforeGet: map[string]func(*fakeCore){},
		beforePatch: map[string]func(*fakeCore){},
	}
}

func popEffect(effects map[string][]responseEffect, name string) responseEffect {
	queue := effects[name]
	if len(queue) == 0 {
		return respondNormally
	}
	effects[name] = queue[1:]
	return queue[0]
}

func (f *fakeCore) GetNamespace(_ context.Context, name string) (*corev1.Namespace, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.namespace == nil || f.namespace.Name != name {
		return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "namespaces"}, name)
	}
	return f.namespace.DeepCopy(), nil
}

func (f *fakeCore) GetConfigMap(_ context.Context, _, name string) (*corev1.ConfigMap, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if hook := f.beforeGet[name]; hook != nil {
		delete(f.beforeGet, name)
		hook(f)
	}
	if cm := f.objects[name]; cm != nil {
		return cm.DeepCopy(), nil
	}
	return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "configmaps"}, name)
}

func (f *fakeCore) CreateConfigMap(_ context.Context, _ string, request *corev1.ConfigMap) (*corev1.ConfigMap, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createCount[request.Name]++
	effect := popEffect(f.createEffects, request.Name)
	if effect == rejectBeforeWrite {
		return nil, errLostResponse
	}
	if f.objects[request.Name] != nil {
		return nil, apierrors.NewAlreadyExists(schema.GroupResource{Resource: "configmaps"}, request.Name)
	}
	f.nextUID++
	f.nextRV++
	stored := request.DeepCopy()
	stored.UID = types.UID("ledger-" + strconv.Itoa(f.nextUID))
	stored.ResourceVersion = strconv.Itoa(f.nextRV)
	f.objects[stored.Name] = stored
	if effect == loseAfterWrite {
		return nil, errLostResponse
	}
	return stored.DeepCopy(), nil
}

func (f *fakeCore) PatchConfigMap(_ context.Context, _, name string, body []byte) (*corev1.ConfigMap, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.patchCount[name]++
	if hook := f.beforePatch[name]; hook != nil {
		delete(f.beforePatch, name)
		hook(f)
	}
	effect := popEffect(f.patchEffects, name)
	if effect == rejectBeforeWrite {
		return nil, errLostResponse
	}
	if effect == rejectInvalidBeforeWrite {
		return nil, apierrors.NewGenericServerResponse(http.StatusUnprocessableEntity, "", schema.GroupResource{}, "", "rejected valid-shape patch", 0, false)
	}
	original := f.objects[name]
	if original == nil {
		return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "configmaps"}, name)
	}
	patch, err := jsonpatch.DecodePatch(body)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(original)
	if err != nil {
		return nil, err
	}
	updatedRaw, err := patch.Apply(raw)
	if err != nil {
		return nil, apierrors.NewGenericServerResponse(http.StatusUnprocessableEntity, "", schema.GroupResource{}, "", err.Error(), 0, false)
	}
	var updated corev1.ConfigMap
	if err := json.Unmarshal(updatedRaw, &updated); err != nil {
		return nil, err
	}
	if updated.UID != original.UID || updated.Name != original.Name || updated.Namespace != original.Namespace ||
		(original.Immutable != nil && *original.Immutable) {
		return nil, fmt.Errorf("fake API rejected immutable or identity mutation")
	}
	f.nextRV++
	updated.ResourceVersion = strconv.Itoa(f.nextRV)
	f.objects[name] = updated.DeepCopy()
	if effect == loseAfterWrite {
		return nil, errLostResponse
	}
	return updated.DeepCopy(), nil
}

func testBootstrap(t *testing.T) (*fakeCore, Bootstrapper) {
	t.Helper()
	r := testReceipt()
	f := newFakeCore(t, r)
	limits := r.Contract.Limits
	activeEmpty := fmt.Sprintf(`{"version":1,"fence":0,"maxJobs":%d,"maxPerRequester":%d,"workerSlots":%d,"active":{}}`,
		limits.MaxActiveJobs, limits.MaxActiveJobsPerRequester, limits.WorkerSlots)
	queueEmpty := fmt.Sprintf(`{"version":1,"globalLimit":%d,"requesterLimit":%d,"intents":{}}`,
		limits.MaxQueuedJobs, limits.MaxQueuedJobsPerRequester)
	validate := func(key, contents string) func(*corev1.ConfigMap) error {
		return func(cm *corev1.ConfigMap) error {
			if len(cm.Data) != 1 || cm.Data[key] != contents || len(cm.BinaryData) != 0 {
				return fmt.Errorf("ledger data differs from canonical empty state")
			}
			return nil
		}
	}
	b := Bootstrapper{API: f, Receipt: r,
		Active: LedgerTemplate{Role: Active, DataKey: ActiveLedgerDataKey, EmptyData: activeEmpty,
			Validate: validate(ActiveLedgerDataKey, activeEmpty)},
		Queue: LedgerTemplate{Role: Queue, DataKey: QueueLedgerDataKey, EmptyData: queueEmpty,
			Validate: validate(QueueLedgerDataKey, queueEmpty)},
		Preflight: func(context.Context) error { return nil },
	}
	return f, b
}

func TestInvalidSecondCanonicalTemplateCannotWriteFirstLedger(t *testing.T) {
	f, b := testBootstrap(t)
	b.Queue.EmptyData = `not valid queue data`
	if _, err := b.EnsureFresh(context.Background()); err == nil {
		t.Fatal("malformed second template passed bootstrap validation")
	}
	if f.createCount[ActiveLedgerName] != 0 || f.createCount[QueueLedgerName] != 0 || f.patchCount[GenesisName] != 0 {
		t.Fatalf("first ledger was written before both templates qualified: creates=%v patches=%v", f.createCount, f.patchCount)
	}
}

func TestCanonicalEmptyCapacityMustMatchAllFiveReceiptLimits(t *testing.T) {
	for _, tc := range []struct {
		name, before, after string
		role                Role
	}{
		{name: "active jobs", role: Active, before: `"maxJobs":128`, after: `"maxJobs":127`},
		{name: "active requester", role: Active, before: `"maxPerRequester":8`, after: `"maxPerRequester":7`},
		{name: "worker slots", role: Active, before: `"workerSlots":65535`, after: `"workerSlots":65534`},
		{name: "queued jobs", role: Queue, before: `"globalLimit":1000`, after: `"globalLimit":999`},
		{name: "queued requester", role: Queue, before: `"requesterLimit":100`, after: `"requesterLimit":99`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, b := testBootstrap(t)
			// A mistakenly matched schema callback can find the mutated ledger
			// self-consistent. Receipt equality must be independent of it.
			b.Active.Validate = func(*corev1.ConfigMap) error { return nil }
			b.Queue.Validate = func(*corev1.ConfigMap) error { return nil }
			if tc.role == Active {
				b.Active.EmptyData = strings.Replace(b.Active.EmptyData, tc.before, tc.after, 1)
				if !strings.Contains(b.Active.EmptyData, tc.after) {
					t.Fatal("test did not mutate active header")
				}
			} else {
				b.Queue.EmptyData = strings.Replace(b.Queue.EmptyData, tc.before, tc.after, 1)
				if !strings.Contains(b.Queue.EmptyData, tc.after) {
					t.Fatal("test did not mutate queue header")
				}
			}
			if _, err := b.EnsureFresh(context.Background()); err == nil || !strings.Contains(err.Error(), "differs from admission receipt") {
				t.Fatalf("capacity mismatch was not refused by independent check: %v", err)
			}
			if f.createCount[ActiveLedgerName] != 0 || f.createCount[QueueLedgerName] != 0 || f.patchCount[GenesisName] != 0 {
				t.Fatalf("capacity mismatch wrote admission state: creates=%v patches=%v", f.createCount, f.patchCount)
			}
		})
	}
}

func TestBootstrapRejectsWrongRoleDataKeysBeforeAnyWrite(t *testing.T) {
	for _, role := range []Role{Active, Queue} {
		t.Run(string(role), func(t *testing.T) {
			f, b := testBootstrap(t)
			b.Active.Validate = func(*corev1.ConfigMap) error { return nil }
			b.Queue.Validate = func(*corev1.ConfigMap) error { return nil }
			if role == Active {
				b.Active.DataKey = "foo"
			} else {
				b.Queue.DataKey = "bar"
			}
			if _, err := b.EnsureFresh(context.Background()); err == nil {
				t.Fatal("self-consistent wrong ledger data key qualified")
			}
			if f.createCount[ActiveLedgerName] != 0 || f.createCount[QueueLedgerName] != 0 || f.patchCount[GenesisName] != 0 {
				t.Fatalf("wrong role data key wrote admission state: creates=%v patches=%v", f.createCount, f.patchCount)
			}
		})
	}
}

func TestNonemptyReadRaceRequiresValidCommittedPeerPair(t *testing.T) {
	const nonemptyActive = `{"version":1,"active":{"job":"running"}}`
	for _, mode := range []string{"valid", "malformed", "replaced"} {
		t.Run(mode, func(t *testing.T) {
			f, b := testBootstrap(t)
			f.objects[ActiveLedgerName] = installedLedger(t, b, b.Active, "active-first")
			f.objects[QueueLedgerName] = installedLedger(t, b, b.Queue, "queue-first")
			initial := GenesisData{Contract: b.Receipt.Contract, Phase: PhaseInitializing,
				ActiveLedgerUID: "active-first", QueueLedgerUID: "queue-first"}
			setGenesis(t, f, b, initial, nil)
			originalValidate := b.Active.Validate
			b.Active.Validate = func(cm *corev1.ConfigMap) error {
				if cm.Data[b.Active.DataKey] == nonemptyActive && len(cm.Data) == 1 && len(cm.BinaryData) == 0 {
					return nil
				}
				return originalValidate(cm)
			}
			// The losing replica has already read Initializing when its first
			// ledger GET is intercepted by the peer's commit and first grant.
			f.beforeGet[ActiveLedgerName] = func(f *fakeCore) {
				committed := initial
				committed.Phase = PhaseCommitted
				cm := genesisObject(t, b.Receipt, committed, boolPtr(true))
				cm.ResourceVersion = "22"
				f.objects[GenesisName] = cm
				f.objects[ActiveLedgerName].Data[b.Active.DataKey] = nonemptyActive
				switch mode {
				case "malformed":
					f.objects[ActiveLedgerName].Data[b.Active.DataKey] = `{"bad":true}`
				case "replaced":
					f.objects[ActiveLedgerName].UID = "active-replacement"
				}
			}
			binding, err := b.EnsureFresh(context.Background())
			if mode == "valid" {
				if err != nil || binding.ActiveLedgerUID != "active-first" || binding.QueueLedgerUID != "queue-first" {
					t.Fatalf("valid committed peer did not qualify: %+v, %v", binding, err)
				}
			} else if err == nil {
				t.Fatalf("%s committed peer incorrectly qualified", mode)
			}
			if f.createCount[ActiveLedgerName] != 0 || f.createCount[QueueLedgerName] != 0 || f.patchCount[GenesisName] != 0 {
				t.Fatalf("losing replica wrote during peer handoff: creates=%v patches=%v", f.createCount, f.patchCount)
			}
		})
	}
}

func TestPreflightVetoRemainsAuthoritativeAcrossPeerCommit(t *testing.T) {
	f, b := testBootstrap(t)
	f.objects[ActiveLedgerName] = installedLedger(t, b, b.Active, "active-first")
	f.objects[QueueLedgerName] = installedLedger(t, b, b.Queue, "queue-first")
	veto := errors.New("visible old runner Pod")
	b.Preflight = func(context.Context) error {
		setGenesis(t, f, b, GenesisData{Contract: b.Receipt.Contract, Phase: PhaseCommitted,
			ActiveLedgerUID: "active-first", QueueLedgerUID: "queue-first"}, boolPtr(true))
		return veto
	}
	if _, err := b.EnsureFresh(context.Background()); !errors.Is(err, veto) {
		t.Fatalf("old-work veto was overridden by committed peer: %v", err)
	}
	if f.createCount[ActiveLedgerName] != 0 || f.patchCount[GenesisName] != 0 {
		t.Fatal("wrote after old-work veto")
	}
}

func installedLedger(t *testing.T, b Bootstrapper, template LedgerTemplate, uid string) *corev1.ConfigMap {
	t.Helper()
	cm, err := b.Receipt.NewLedgerObject(template.Role, template.DataKey, template.EmptyData,
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	if err != nil {
		t.Fatal(err)
	}
	cm.UID = types.UID(uid)
	cm.ResourceVersion = "20"
	return cm
}

func setGenesis(t *testing.T, f *fakeCore, b Bootstrapper, state GenesisData, immutable *bool) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	cm := genesisObject(t, b.Receipt, state, immutable)
	cm.ResourceVersion = "21"
	f.objects[b.Receipt.GenesisName] = cm
}

func currentGenesis(t *testing.T, f *fakeCore, b Bootstrapper) GenesisData {
	t.Helper()
	cm, err := f.GetConfigMap(context.Background(), b.Receipt.Namespace, b.Receipt.GenesisName)
	if err != nil {
		t.Fatal(err)
	}
	state, err := b.Receipt.QualifyGenesis(cm)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func TestFreshBootstrapCommitsExactOriginalPair(t *testing.T) {
	f, b := testBootstrap(t)
	binding, err := b.EnsureFresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	state := currentGenesis(t, f, b)
	if state.Phase != PhaseCommitted || state.ActiveLedgerUID == "" || state.QueueLedgerUID == "" ||
		binding.ActiveLedgerUID != state.ActiveLedgerUID || binding.QueueLedgerUID != state.QueueLedgerUID ||
		binding.GenesisUID != b.Receipt.GenesisUID || binding.NamespaceUID != b.Receipt.Contract.NamespaceUID {
		t.Fatalf("unbound commit: binding=%+v state=%+v", binding, state)
	}
	if cm := f.objects[b.Receipt.GenesisName]; cm.Immutable == nil || !*cm.Immutable {
		t.Fatal("committed Genesis is not immutable")
	}
	if f.createCount[ActiveLedgerName] != 1 || f.createCount[QueueLedgerName] != 1 || f.patchCount[GenesisName] != 3 {
		t.Fatalf("unexpected writes: create=%v patch=%v", f.createCount, f.patchCount)
	}
	if again, err := b.EnsureFresh(context.Background()); err != nil || again != binding ||
		f.createCount[ActiveLedgerName] != 1 || f.createCount[QueueLedgerName] != 1 || f.patchCount[GenesisName] != 3 {
		t.Fatalf("committed observation wrote or drifted: %+v, %v", again, err)
	}
}

func TestConcurrentBootstrapPeersConvergeOnOnePair(t *testing.T) {
	for iteration := 0; iteration < 12; iteration++ {
		f, b := testBootstrap(t)
		var wg sync.WaitGroup
		results := make(chan error, 2)
		for peer := 0; peer < 2; peer++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := b.EnsureFresh(context.Background())
				results <- err
			}()
		}
		wg.Wait()
		close(results)
		for err := range results {
			// A stale CAS may refuse one contender; it must never create a
			// different committed pair. A restart must converge on the winner.
			if err != nil && !errors.Is(err, ErrUnknownPin) && !errors.Is(err, ErrUnknownCommit) {
				t.Fatalf("unexpected peer result in iteration %d: %v", iteration, err)
			}
		}
		binding, err := b.EnsureFresh(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if got := currentGenesis(t, f, b); got.Phase != PhaseCommitted ||
			got.ActiveLedgerUID != binding.ActiveLedgerUID || got.QueueLedgerUID != binding.QueueLedgerUID {
			t.Fatalf("peers did not converge: %+v, %+v", got, binding)
		}
	}
}

func TestBootstrapRecoversCreatedAndPinnedPartialPair(t *testing.T) {
	for _, tc := range []struct{ active, queue, activePin, queuePin bool }{
		{active: true}, {queue: true}, {active: true, activePin: true},
		{queue: true, queuePin: true}, {active: true, queue: true},
		{active: true, queue: true, activePin: true, queuePin: true},
	} {
		t.Run(fmt.Sprintf("a%t-q%t-ap%t-qp%t", tc.active, tc.queue, tc.activePin, tc.queuePin), func(t *testing.T) {
			f, b := testBootstrap(t)
			state := GenesisData{Contract: b.Receipt.Contract, Phase: PhaseInitializing}
			if tc.active {
				f.objects[ActiveLedgerName] = installedLedger(t, b, b.Active, "first-active")
				if tc.activePin {
					state.ActiveLedgerUID = "first-active"
				}
			}
			if tc.queue {
				f.objects[QueueLedgerName] = installedLedger(t, b, b.Queue, "first-queue")
				if tc.queuePin {
					state.QueueLedgerUID = "first-queue"
				}
			}
			setGenesis(t, f, b, state, boolPtr(false))
			binding, err := b.EnsureFresh(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if tc.active && binding.ActiveLedgerUID != "first-active" {
				t.Fatalf("active UID overwritten: %+v", binding)
			}
			if tc.queue && binding.QueueLedgerUID != "first-queue" {
				t.Fatalf("queue UID overwritten: %+v", binding)
			}
			if tc.active && f.createCount[ActiveLedgerName] != 0 {
				t.Fatal("recreated existing active ledger")
			}
			if tc.queue && f.createCount[QueueLedgerName] != 0 {
				t.Fatal("recreated existing queue ledger")
			}
		})
	}
}

func TestBootstrapLostCreateAndPinResponses(t *testing.T) {
	for _, lost := range []struct {
		name    string
		effects map[string][]responseEffect
	}{
		{name: "active Create", effects: map[string][]responseEffect{ActiveLedgerName: {loseAfterWrite}}},
		{name: "queue Create", effects: map[string][]responseEffect{QueueLedgerName: {loseAfterWrite}}},
	} {
		t.Run(lost.name, func(t *testing.T) {
			f, b := testBootstrap(t)
			f.createEffects = lost.effects
			if _, err := b.EnsureFresh(context.Background()); err != nil {
				t.Fatal(err)
			}
			if currentGenesis(t, f, b).Phase != PhaseCommitted {
				t.Fatal("lost Create response prevented verified commit")
			}
		})
	}
	t.Run("unpersisted Create has unknown outcome", func(t *testing.T) {
		f, b := testBootstrap(t)
		f.createEffects[ActiveLedgerName] = []responseEffect{rejectBeforeWrite}
		if _, err := b.EnsureFresh(context.Background()); !errors.Is(err, ErrUnknownCreate) {
			t.Fatalf("expected unknown Create, got %v", err)
		}
		if f.objects[ActiveLedgerName] != nil || currentGenesis(t, f, b).Phase != PhaseInitializing || f.createCount[QueueLedgerName] != 0 {
			t.Fatal("advanced after absent read of uncertain Create")
		}
	})
	t.Run("persisted pin with lost response", func(t *testing.T) {
		f, b := testBootstrap(t)
		f.patchEffects[GenesisName] = []responseEffect{loseAfterWrite}
		if _, err := b.EnsureFresh(context.Background()); err != nil {
			t.Fatal(err)
		}
		if currentGenesis(t, f, b).Phase != PhaseCommitted {
			t.Fatal("verified pin was not reused")
		}
	})
	t.Run("unpersisted pin has unknown outcome", func(t *testing.T) {
		f, b := testBootstrap(t)
		f.patchEffects[GenesisName] = []responseEffect{rejectBeforeWrite}
		if _, err := b.EnsureFresh(context.Background()); !errors.Is(err, ErrUnknownPin) {
			t.Fatalf("expected unknown pin, got %v", err)
		}
		if currentGenesis(t, f, b).Phase != PhaseInitializing || f.createCount[QueueLedgerName] != 0 {
			t.Fatal("advanced after an unverified provisional pin")
		}
		if _, err := b.EnsureFresh(context.Background()); err != nil {
			t.Fatalf("restart did not recover first ledger: %v", err)
		}
	})
}

func TestBootstrapLostCommitResponse(t *testing.T) {
	for _, effect := range []responseEffect{loseAfterWrite, rejectBeforeWrite} {
		t.Run(fmt.Sprintf("effect-%d", effect), func(t *testing.T) {
			f, b := testBootstrap(t)
			f.objects[ActiveLedgerName] = installedLedger(t, b, b.Active, "active-first")
			f.objects[QueueLedgerName] = installedLedger(t, b, b.Queue, "queue-first")
			setGenesis(t, f, b, GenesisData{Contract: b.Receipt.Contract, Phase: PhaseInitializing,
				ActiveLedgerUID: "active-first", QueueLedgerUID: "queue-first"}, nil)
			f.patchEffects[GenesisName] = []responseEffect{effect}
			binding, err := b.EnsureFresh(context.Background())
			if effect == loseAfterWrite {
				if err != nil || binding.ActiveLedgerUID != "active-first" || currentGenesis(t, f, b).Phase != PhaseCommitted {
					t.Fatalf("persisted commit not qualified: %+v, %v", binding, err)
				}
			} else {
				if !errors.Is(err, ErrUnknownCommit) || currentGenesis(t, f, b).Phase != PhaseInitializing {
					t.Fatalf("unpersisted commit not uncertain: %+v, %v", binding, err)
				}
				if _, err := b.EnsureFresh(context.Background()); err != nil {
					t.Fatalf("restart did not commit: %v", err)
				}
			}
		})
	}
}

func TestBootstrapRefusesPinnedOrCommittedLossWithoutCreate(t *testing.T) {
	for _, tc := range []struct {
		name                                   string
		committed, missing, replaced, nonempty bool
	}{
		{name: "provisional missing", missing: true},
		{name: "provisional replacement", replaced: true},
		{name: "provisional nonempty", nonempty: true},
		{name: "committed missing", committed: true, missing: true},
		{name: "committed replacement", committed: true, replaced: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, b := testBootstrap(t)
			active := installedLedger(t, b, b.Active, "active-first")
			queue := installedLedger(t, b, b.Queue, "queue-first")
			if tc.replaced {
				active.UID = "active-replacement"
			}
			if tc.nonempty {
				active.Data[b.Active.DataKey] = `{"version":1,"active":{"unexpected":true}}`
			}
			if !tc.missing {
				f.objects[ActiveLedgerName] = active
			}
			f.objects[QueueLedgerName] = queue
			state := GenesisData{Contract: b.Receipt.Contract, Phase: PhaseInitializing,
				ActiveLedgerUID: "active-first", QueueLedgerUID: "queue-first"}
			var immutable *bool
			if tc.committed {
				state.Phase, immutable = PhaseCommitted, boolPtr(true)
			}
			setGenesis(t, f, b, state, immutable)
			if _, err := b.EnsureFresh(context.Background()); err == nil {
				t.Fatal("unsafe pair qualified")
			}
			if f.createCount[ActiveLedgerName] != 0 || f.createCount[QueueLedgerName] != 0 || f.patchCount[GenesisName] != 0 {
				t.Fatalf("unsafe pair was mutated: creates=%v patches=%v", f.createCount, f.patchCount)
			}
		})
	}
}

func TestDelayedOldCreateAfterCommittedLossCannotRebind(t *testing.T) {
	f, b := testBootstrap(t)
	first, err := b.EnsureFresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	delete(f.objects, ActiveLedgerName) // privileged deletion outside protocol
	late, err := b.Receipt.NewLedgerObject(Active, b.Active.DataKey, b.Active.EmptyData,
		"cccccccccccccccccccccccccccccccc")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.CreateConfigMap(context.Background(), b.Receipt.Namespace, late); err != nil {
		t.Fatal(err)
	}
	if f.objects[ActiveLedgerName].UID == types.UID(first.ActiveLedgerUID) {
		t.Fatal("fake API did not assign a new UID")
	}
	creates := f.createCount[ActiveLedgerName]
	if _, err := b.ObserveCommitted(context.Background()); err == nil {
		t.Fatal("replacement ledger qualified under original committed binding")
	}
	if _, err := b.EnsureFresh(context.Background()); err == nil || f.createCount[ActiveLedgerName] != creates {
		t.Fatalf("committed loss was repaired or qualified: %v", err)
	}
}

func TestBootstrapRefusesOriginalIdentityAndPreflightVeto(t *testing.T) {
	for _, tc := range []struct {
		name  string
		alter func(*fakeCore, Bootstrapper)
	}{
		{name: "Namespace replacement", alter: func(f *fakeCore, _ Bootstrapper) { f.namespace.UID = "other-namespace" }},
		{name: "Genesis replacement", alter: func(f *fakeCore, b Bootstrapper) { f.objects[b.Receipt.GenesisName].UID = "other-genesis" }},
		{name: "Genesis limit drift", alter: func(f *fakeCore, b Bootstrapper) {
			cm := f.objects[b.Receipt.GenesisName]
			var state GenesisData
			_ = json.Unmarshal([]byte(cm.Data[GenesisDataKey]), &state)
			state.Contract.Limits.MaxActiveJobs--
			encoded, _ := json.Marshal(state)
			cm.Data[GenesisDataKey] = string(encoded)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, b := testBootstrap(t)
			tc.alter(f, b)
			if _, err := b.EnsureFresh(context.Background()); err == nil {
				t.Fatal("replacement or drift qualified")
			}
			if f.createCount[ActiveLedgerName] != 0 || f.patchCount[GenesisName] != 0 {
				t.Fatal("mutation after original identity drift")
			}
		})
	}
	f, b := testBootstrap(t)
	b.Preflight = func(context.Context) error { return errors.New("old runner Pod exists") }
	if _, err := b.EnsureFresh(context.Background()); err == nil {
		t.Fatal("old-work veto was ignored")
	}
	if f.createCount[ActiveLedgerName] != 0 || f.patchCount[GenesisName] != 0 {
		t.Fatal("wrote after old-work veto")
	}
}

func TestGenesisPatchRequiresOriginalUIDVersionAndData(t *testing.T) {
	r := testReceipt()
	for _, immutable := range []*bool{nil, boolPtr(false)} {
		original := genesisObject(t, r, GenesisData{Contract: r.Contract, Phase: PhaseInitializing}, immutable)
		original.ResourceVersion = "7"
		next := GenesisData{Contract: r.Contract, Phase: PhaseCommitted,
			ActiveLedgerUID: "active-first", QueueLedgerUID: "queue-first"}
		patch, err := genesisPatch(original, next, true)
		if err != nil {
			t.Fatal(err)
		}
		var ops []patchOp
		if err := json.Unmarshal(patch, &ops); err != nil {
			t.Fatal(err)
		}
		if len(ops) < 5 || ops[0].Op != "test" || ops[0].Path != "/metadata/uid" ||
			ops[1].Op != "test" || ops[1].Path != "/metadata/resourceVersion" ||
			ops[2].Op != "test" || ops[2].Path != "/data/genesis.json" ||
			ops[len(ops)-1].Op != "add" || ops[len(ops)-1].Path != "/immutable" {
			t.Fatalf("missing CAS or immutable operation: %s", patch)
		}
		if immutable != nil && len(ops) != 6 {
			t.Fatalf("false immutable needs test: %s", patch)
		}
		if immutable == nil && len(ops) != 5 {
			t.Fatalf("absent immutable must not be tested: %s", patch)
		}
		decoded, _ := json.Marshal(original)
		operation, err := jsonpatch.DecodePatch(patch)
		if err != nil {
			t.Fatal(err)
		}
		result, err := operation.Apply(decoded)
		if err != nil {
			t.Fatal(err)
		}
		var updated corev1.ConfigMap
		if err := json.Unmarshal(result, &updated); err != nil {
			t.Fatal(err)
		}
		if state, err := r.QualifyGenesis(&updated); err != nil || state != next {
			t.Fatalf("commit patch did not produce immutable Genesis: %+v, %v", state, err)
		}
		for _, drift := range []func(*corev1.ConfigMap){
			func(cm *corev1.ConfigMap) { cm.UID = "replacement" },
			func(cm *corev1.ConfigMap) { cm.ResourceVersion = "8" },
			func(cm *corev1.ConfigMap) { cm.Data[GenesisDataKey] = `{"phase":"different"}` },
		} {
			changed := original.DeepCopy()
			drift(changed)
			changedRaw, _ := json.Marshal(changed)
			if _, err := operation.Apply(changedRaw); err == nil {
				t.Fatal("stale Genesis patch applied after identity or data drift")
			}
		}
	}
}
