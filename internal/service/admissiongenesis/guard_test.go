package admissiongenesis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/cofy-x/kova/internal/admissioncontract"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
)

func committedGuardFixture(t *testing.T) (*fakeCore, Bootstrapper, *Guard, map[admissioncontract.Role]string) {
	t.Helper()
	f, b := testBootstrap(t)
	next := map[admissioncontract.Role]string{
		admissioncontract.Active: strings.Replace(b.Active.EmptyData, `"fence":0`, `"fence":1`, 1),
		admissioncontract.Queue:  strings.Replace(b.Queue.EmptyData, `"intents":{}`, `"intents":{ }`, 1),
	}
	for _, template := range []LedgerTemplate{b.Active, b.Queue} {
		if next[template.Role] == template.EmptyData {
			t.Fatalf("%s test proposal did not change", template.Role)
		}
		role, key, empty := template.Role, template.DataKey, template.EmptyData
		validate := func(cm *corev1.ConfigMap) error {
			if len(cm.Data) != 1 || len(cm.BinaryData) != 0 || (cm.Data[key] != empty && cm.Data[key] != next[role]) {
				return fmt.Errorf("%s test ledger has invalid data", role)
			}
			return nil
		}
		if role == admissioncontract.Active {
			b.Active.Validate = validate
		} else {
			b.Queue.Validate = validate
		}
	}
	f.objects[admissioncontract.ActiveLedgerName] = installedLedger(t, b, b.Active, "active-original")
	f.objects[admissioncontract.QueueLedgerName] = installedLedger(t, b, b.Queue, "queue-original")
	setGenesis(t, f, b, admissioncontract.GenesisData{Contract: b.Receipt.Contract, Phase: admissioncontract.PhaseCommitted,
		ActiveLedgerUID: "active-original", QueueLedgerUID: "queue-original"}, boolPtr(true))
	binding := Binding{NamespaceUID: b.Receipt.Contract.NamespaceUID, GenesisUID: b.Receipt.GenesisUID,
		ActiveLedgerUID: "active-original", QueueLedgerUID: "queue-original"}
	guard, err := NewGuard(context.Background(), b, binding)
	if err != nil {
		t.Fatal(err)
	}
	return f, b, guard, next
}

func TestGuardRechecksExternalReceiptAtEachEdge(t *testing.T) {
	_, _, guard, _ := committedGuardFixture(t)
	checks := 0
	guard.ReceiptCheck = func(context.Context) error {
		checks++
		if checks > 2 {
			return ErrChanged
		}
		return nil
	}
	if err := guard.Check(context.Background()); err != nil || checks != 2 {
		t.Fatalf("receipt was not checked around committed pair: checks=%d err=%v", checks, err)
	}
	if err := guard.Check(context.Background()); !errors.Is(err, ErrChanged) || checks != 3 {
		t.Fatalf("changed receipt remained authorized: checks=%d err=%v", checks, err)
	}
}

func TestGuardedLedgerPatchUsesOriginalUIDAndResourceVersion(t *testing.T) {
	for _, tc := range []struct {
		role admissioncontract.Role
		name string
		key  string
	}{
		{admissioncontract.Active, admissioncontract.ActiveLedgerName, admissioncontract.ActiveLedgerDataKey},
		{admissioncontract.Queue, admissioncontract.QueueLedgerName, admissioncontract.QueueLedgerDataKey},
	} {
		t.Run(string(tc.role), func(t *testing.T) {
			f, b, guard, next := committedGuardFixture(t)
			original, err := f.GetConfigMap(context.Background(), b.Receipt.Namespace, tc.name)
			if err != nil {
				t.Fatal(err)
			}
			if err := guard.PatchLedgerData(context.Background(), original, tc.role, next[tc.role]); err != nil {
				t.Fatal(err)
			}
			updated, err := f.GetConfigMap(context.Background(), b.Receipt.Namespace, tc.name)
			if err != nil {
				t.Fatal(err)
			}
			if updated.UID != original.UID || updated.ResourceVersion == original.ResourceVersion ||
				updated.Data[tc.key] != next[tc.role] || f.patchCount[tc.name] != 1 {
				t.Fatalf("conditional patch did not retain original identity: before=%+v after=%+v patches=%d", original, updated, f.patchCount[tc.name])
			}
		})
	}
}

func TestGuardedLedgerPatchRefusesStaleOrReplacedState(t *testing.T) {
	for _, mode := range []string{"stale RV", "stale data", "replacement during patch", "queue lost before patch"} {
		t.Run(mode, func(t *testing.T) {
			f, b, guard, next := committedGuardFixture(t)
			original, err := f.GetConfigMap(context.Background(), b.Receipt.Namespace, admissioncontract.ActiveLedgerName)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "stale RV":
				f.objects[admissioncontract.ActiveLedgerName].ResourceVersion = "21"
			case "stale data":
				f.objects[admissioncontract.ActiveLedgerName].Data[admissioncontract.ActiveLedgerDataKey] = next[admissioncontract.Active]
			case "replacement during patch":
				f.beforePatch[admissioncontract.ActiveLedgerName] = func(f *fakeCore) {
					f.objects[admissioncontract.ActiveLedgerName].UID = types.UID("active-replacement")
				}
			case "queue lost before patch":
				delete(f.objects, admissioncontract.QueueLedgerName)
			}
			err = guard.PatchLedgerData(context.Background(), original, admissioncontract.Active, next[admissioncontract.Active])
			if err == nil {
				t.Fatalf("%s was allowed to patch", mode)
			}
			if mode == "queue lost before patch" {
				if f.patchCount[admissioncontract.ActiveLedgerName] != 0 {
					t.Fatalf("patched after pair loss: %d", f.patchCount[admissioncontract.ActiveLedgerName])
				}
			} else if mode == "replacement during patch" {
				if !errors.Is(err, ErrChanged) || f.patchCount[admissioncontract.ActiveLedgerName] != 1 {
					t.Fatalf("replacement was treated as retryable contention: patches=%d err=%v", f.patchCount[admissioncontract.ActiveLedgerName], err)
				}
			} else if !apierrors.IsConflict(err) || f.patchCount[admissioncontract.ActiveLedgerName] != 1 {
				t.Fatalf("stale RFC6902 test was not classified as one retryable conflict: patches=%d err=%v", f.patchCount[admissioncontract.ActiveLedgerName], err)
			}
		})
	}
}

func TestGuardedLedgerUnchanged422IsNotRetryableConflict(t *testing.T) {
	f, b, guard, next := committedGuardFixture(t)
	original, err := f.GetConfigMap(context.Background(), b.Receipt.Namespace, admissioncontract.ActiveLedgerName)
	if err != nil {
		t.Fatal(err)
	}
	f.patchEffects[admissioncontract.ActiveLedgerName] = []responseEffect{rejectInvalidBeforeWrite}
	err = guard.PatchLedgerData(context.Background(), original, admissioncontract.Active, next[admissioncontract.Active])
	if !apierrors.IsInvalid(err) || apierrors.IsConflict(err) || f.patchCount[admissioncontract.ActiveLedgerName] != 1 {
		t.Fatalf("unchanged original ledger turned arbitrary 422 into conflict: err=%v patches=%d", err, f.patchCount[admissioncontract.ActiveLedgerName])
	}
	current := f.objects[admissioncontract.ActiveLedgerName]
	if current.ResourceVersion != original.ResourceVersion || current.Data[admissioncontract.ActiveLedgerDataKey] != original.Data[admissioncontract.ActiveLedgerDataKey] {
		t.Fatalf("test 422 unexpectedly changed ledger: %+v", current)
	}
}

func TestGuardedLedgerConcurrentCASReportsOriginalPairConflict(t *testing.T) {
	for _, role := range []admissioncontract.Role{admissioncontract.Active, admissioncontract.Queue} {
		t.Run(string(role), func(t *testing.T) {
			f, b, guard, next := committedGuardFixture(t)
			name := admissioncontract.ActiveLedgerName
			if role == admissioncontract.Queue {
				name = admissioncontract.QueueLedgerName
			}
			original, err := f.GetConfigMap(context.Background(), b.Receipt.Namespace, name)
			if err != nil {
				t.Fatal(err)
			}
			start := make(chan struct{})
			results := make(chan error, 2)
			for range 2 {
				go func() {
					<-start
					results <- guard.PatchLedgerData(context.Background(), original, role, next[role])
				}()
			}
			close(start)
			first, second := <-results, <-results
			if !((first == nil && apierrors.IsConflict(second)) || (second == nil && apierrors.IsConflict(first))) {
				t.Fatalf("concurrent original-ledger CAS = (%v, %v), want one commit and one typed conflict", first, second)
			}
			if f.patchCount[name] != 2 {
				t.Fatalf("concurrent writers sent %d Patches, want one each", f.patchCount[name])
			}
		})
	}
}

func TestGuardedLedgerLostPatchResponseIsNotReplayed(t *testing.T) {
	f, b, guard, next := committedGuardFixture(t)
	original, err := f.GetConfigMap(context.Background(), b.Receipt.Namespace, admissioncontract.ActiveLedgerName)
	if err != nil {
		t.Fatal(err)
	}
	f.patchEffects[admissioncontract.ActiveLedgerName] = []responseEffect{loseAfterWrite}
	if err := guard.PatchLedgerData(context.Background(), original, admissioncontract.Active, next[admissioncontract.Active]); !errors.Is(err, errLostResponse) {
		t.Fatalf("lost response was treated as success: %v", err)
	}
	if f.patchCount[admissioncontract.ActiveLedgerName] != 1 || f.objects[admissioncontract.ActiveLedgerName].Data[admissioncontract.ActiveLedgerDataKey] != next[admissioncontract.Active] {
		t.Fatalf("lost response was replayed or not committed: patches=%d ledger=%+v", f.patchCount[admissioncontract.ActiveLedgerName], f.objects[admissioncontract.ActiveLedgerName])
	}
}

func TestGuardPinsStartupPairAndRefusesCommittedLoss(t *testing.T) {
	f, b := testBootstrap(t)
	binding, err := b.EnsureFresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	guard, err := NewGuard(context.Background(), b, binding)
	if err != nil {
		t.Fatal(err)
	}
	if err := guard.Check(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := guard.CheckLedger(f.objects[admissioncontract.ActiveLedgerName], admissioncontract.Active); err != nil {
		t.Fatal(err)
	}
	for _, role := range []admissioncontract.Role{admissioncontract.Active, admissioncontract.Queue} {
		name := admissioncontract.ActiveLedgerName
		if role == admissioncontract.Queue {
			name = admissioncontract.QueueLedgerName
		}
		original := f.objects[name]
		delete(f.objects, name)
		if err := guard.Check(context.Background()); err == nil {
			t.Fatalf("missing committed %s ledger qualified", role)
		}
		f.objects[name] = original.DeepCopy()
		f.objects[name].UID = types.UID("replacement")
		if err := guard.Check(context.Background()); err == nil {
			t.Fatalf("replacement committed %s ledger qualified", role)
		}
		if err := guard.CheckLedger(f.objects[name], role); err == nil {
			t.Fatalf("replacement %s ledger passed local UID check", role)
		}
		f.objects[name] = original
	}
}

func TestGuardCannotAdoptDifferentStartupBinding(t *testing.T) {
	_, b := testBootstrap(t)
	wrong := Binding{NamespaceUID: b.Receipt.Contract.NamespaceUID, GenesisUID: b.Receipt.GenesisUID,
		ActiveLedgerUID: "first", QueueLedgerUID: "second"}
	if _, err := NewGuard(context.Background(), b, wrong); err == nil {
		// The original Genesis has not committed; a supplied binding alone
		// cannot gain authority.
		t.Fatal("unobserved startup binding gained authority")
	}
	wrong.NamespaceUID = "other-namespace"
	if _, err := NewGuard(context.Background(), b, wrong); err == nil {
		t.Fatal("binding outside receipt qualified")
	}
	f, committedBootstrap := testBootstrap(t)
	binding, err := committedBootstrap.EnsureFresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wrong = binding
	wrong.QueueLedgerUID = "other-queue"
	if _, err := NewGuard(context.Background(), committedBootstrap, wrong); err == nil || f.patchCount[admissioncontract.GenesisName] != 3 {
		t.Fatalf("changed startup pair qualified or wrote: %v", err)
	}
}
