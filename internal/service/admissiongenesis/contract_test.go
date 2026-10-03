package admissiongenesis

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/cofy-x/kova/internal/admissioncontract"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func testReceipt() admissioncontract.Receipt {
	return admissioncontract.Receipt{
		Namespace: "jobs-57", GenesisName: admissioncontract.GenesisName, GenesisUID: "genesis-uid",
		Contract: admissioncontract.Contract{
			Version: 2, NamespaceUID: "namespace-uid", ReceiptNamespace: "receipts-57",
			ReceiptNamespaceUID: "receipt-namespace-uid", Generation: strings.Repeat("a", 32),
			ActiveLedgerName: admissioncontract.ActiveLedgerName, ActiveLedgerSchema: 1,
			QueueLedgerName: admissioncontract.QueueLedgerName, QueueLedgerSchema: 2,
			Limits: admissioncontract.Limits{MaxActiveJobs: 128, MaxActiveJobsPerRequester: 8, WorkerSlots: 65535,
				MaxQueuedJobs: 1000, MaxQueuedJobsPerRequester: 100},
		},
	}
}

func genesisObject(t *testing.T, r admissioncontract.Receipt, state admissioncontract.GenesisData, immutable *bool) *corev1.ConfigMap {
	t.Helper()
	encoded, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Namespace: r.Namespace, Name: r.GenesisName, UID: types.UID(r.GenesisUID), ResourceVersion: "opaque-rv",
	}, Data: map[string]string{admissioncontract.GenesisDataKey: string(encoded)}, Immutable: immutable}
}

func TestReceiptRejectsLossyOrDriftedContract(t *testing.T) {
	r := testReceipt()
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	copyRaw := bytes.Clone(raw)
	parsed, err := admissioncontract.ParseReceipt(raw)
	if err != nil || parsed != r {
		t.Fatalf("valid receipt: %#v, %v", parsed, err)
	}
	if !bytes.Equal(raw, copyRaw) {
		t.Fatal("decoder changed caller bytes")
	}
	for name, changed := range map[string][]byte{
		"duplicate":         bytes.Replace(raw, []byte(`"namespace":`), []byte(`"namespace":"other","namespace":`), 1),
		"escaped duplicate": bytes.Replace(raw, []byte(`"namespace":`), []byte(`"\u006eamespace":"other","namespace":`), 1),
		"unknown":           bytes.Replace(raw, []byte(`"contract":`), []byte(`"unknown":1,"contract":`), 1),
		"missing":           bytes.Replace(raw, []byte(`"genesisUID":"genesis-uid",`), nil, 1),
		"null":              bytes.Replace(raw, []byte(`"genesisUID":"genesis-uid"`), []byte(`"genesisUID":null`), 1),
		"cap drift":         bytes.Replace(raw, []byte(`"maxActiveJobs":128`), []byte(`"maxActiveJobs":129`), 1),
		"schema drift":      bytes.Replace(raw, []byte(`"activeLedgerSchema":1`), []byte(`"activeLedgerSchema":2`), 1),
		"trailing":          append(bytes.Clone(raw), []byte(` true`)...),
		"surrogate":         bytes.Replace(raw, []byte(`"namespace":"jobs-57"`), []byte(`"namespace":"\ud800"`), 1),
	} {
		t.Run(name, func(t *testing.T) {
			if got, err := admissioncontract.ParseReceipt(changed); err == nil || got != (admissioncontract.Receipt{}) {
				t.Fatalf("bad receipt accepted: %#v, %v", got, err)
			}
		})
	}
	if _, err := admissioncontract.ParseReceipt(append(raw, bytes.Repeat([]byte(" "), admissioncontract.MaxContractJSONBytes)...)); err == nil {
		t.Fatal("oversized receipt accepted")
	}
}

func TestGenesisProvisionalPinsAndImmutablePhase(t *testing.T) {
	r := testReceipt()
	state := admissioncontract.GenesisData{Contract: r.Contract, Phase: admissioncontract.PhaseInitializing}
	for _, immutable := range []*bool{nil, boolPtr(false)} {
		cm := genesisObject(t, r, state, immutable)
		if got, err := r.QualifyGenesis(cm); err != nil || got != state {
			t.Fatalf("initial Genesis: %#v, %v", got, err)
		}
		state.ActiveLedgerUID = "first-active-uid"
		cm = genesisObject(t, r, state, immutable)
		if got, err := r.QualifyGenesis(cm); err != nil || got != state {
			t.Fatalf("provisional Genesis: %#v, %v", got, err)
		}
	}
	committed := state
	committed.QueueLedgerUID = "first-queue-uid"
	committed.Phase = admissioncontract.PhaseCommitted
	if _, err := r.QualifyGenesis(genesisObject(t, r, committed, boolPtr(true))); err != nil {
		t.Fatal(err)
	}
	for name, cm := range map[string]*corev1.ConfigMap{
		"initial immutable": genesisObject(t, r, state, boolPtr(true)),
		"committed mutable": genesisObject(t, r, committed, nil),
		"incomplete commit": genesisObject(t, r, admissioncontract.GenesisData{Contract: r.Contract, Phase: admissioncontract.PhaseCommitted, ActiveLedgerUID: "first-active-uid"}, boolPtr(true)),
		"changed contract": func() *corev1.ConfigMap {
			bad := state
			bad.Contract.Limits.MaxActiveJobs = 1
			return genesisObject(t, r, bad, nil)
		}(),
		"new genesis UID": func() *corev1.ConfigMap {
			bad := genesisObject(t, r, state, nil)
			bad.UID = "replacement"
			return bad
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := r.QualifyGenesis(cm); err == nil {
				t.Fatal("invalid Genesis qualified")
			}
		})
	}
	cm := genesisObject(t, r, state, nil)
	cm.Data[admissioncontract.GenesisDataKey] = strings.Replace(cm.Data[admissioncontract.GenesisDataKey], `"phase":"Initializing"`, `"phase":"Initializing","phase":"Committed"`, 1)
	if _, err := r.QualifyGenesis(cm); err == nil {
		t.Fatal("duplicate phase qualified")
	}
}

func TestOriginalNamespaceAndLedgerIdentities(t *testing.T) {
	r := testReceipt()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: r.Namespace, UID: types.UID(r.Contract.NamespaceUID)},
		Status: corev1.NamespaceStatus{Phase: corev1.NamespaceActive}}
	if err := r.QualifyNamespace(ns); err != nil {
		t.Fatal(err)
	}
	ns.UID = "recreated"
	if err := r.QualifyNamespace(ns); err == nil {
		t.Fatal("same-name replacement Namespace qualified")
	}
	ns.UID = types.UID(r.Contract.NamespaceUID)
	ns.DeletionTimestamp = &metav1.Time{}
	if err := r.QualifyNamespace(ns); err == nil {
		t.Fatal("deleting Namespace qualified")
	}
	for _, role := range []admissioncontract.Role{admissioncontract.Active, admissioncontract.Queue} {
		cm, err := r.NewLedgerObject(role, "ledger.json", `{}`, strings.Repeat("b", 32))
		if err != nil {
			t.Fatal(err)
		}
		cm.UID, cm.ResourceVersion = "ledger-uid", "rv"
		if err := r.QualifyLedgerIdentity(cm, role); err != nil {
			t.Fatalf("%s ledger identity: %v", role, err)
		}
		bad := cm.DeepCopy()
		bad.Annotations["kova.cofy.dev/admission-generation"] = strings.Repeat("c", 32)
		if err := r.QualifyLedgerIdentity(bad, role); err == nil {
			t.Fatal("changed ledger generation qualified")
		}
		bad = cm.DeepCopy()
		bad.Annotations["kova.cofy.dev/admission-bootstrap"] = "bad"
		if err := r.QualifyLedgerIdentity(bad, role); err == nil {
			t.Fatal("malformed attempt qualified")
		}
		bad = cm.DeepCopy()
		bad.UID = ""
		if err := r.QualifyLedgerIdentity(bad, role); err == nil {
			t.Fatal("unidentified ledger qualified")
		}
		bad = cm.DeepCopy()
		bad.Annotations["other"] = "data"
		if err := r.QualifyLedgerIdentity(bad, role); err == nil {
			t.Fatal("extra ledger annotation qualified")
		}
	}
}

func boolPtr(value bool) *bool { return &value }
