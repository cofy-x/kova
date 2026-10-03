package recoverydisposal

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/cofy-x/kova/internal/service/recoverypermit"
)

func hash(c byte) string { return "sha256:" + strings.Repeat(string(c), 64) }

type fixture struct {
	auth          AuthorizationPayload
	closure       ClosurePayload
	authExpected  AuthorizationExpectation
	closeExpected ClosureExpectation
	roots         TrustRoots
	private       ed25519.PrivateKey
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	epoch := recoverypermit.EpochIdentity{
		Namespace: "old-runner", NamespaceUID: "runner-uid", ReceiptNamespace: "old-receipts",
		ReceiptNamespaceUID: "receipt-uid", GenesisName: "genesis", GenesisUID: "genesis-uid",
		Generation: strings.Repeat("a", 32), ActiveLedgerUID: "active-uid",
		QueueLedgerUID: "queue-uid", WorkerPoolID: "old-pool",
	}
	receipts := []recoverypermit.ReceiptRef{
		{Kind: "grant", Namespace: epoch.ReceiptNamespace, Name: "kova-grant-intent-" + strings.Repeat("b", 32), UID: "grant-uid", DataDigest: hash('a')},
		{Kind: "pod-create", Namespace: epoch.ReceiptNamespace, Name: "kova-pod-create-intent-" + strings.Repeat("c", 32), UID: "pod-receipt-uid", DataDigest: hash('b')},
		{Kind: "queue", Namespace: epoch.ReceiptNamespace, Name: "kova-admission-intent-" + strings.Repeat("d", 32), UID: "queue-receipt-uid", DataDigest: hash('c')},
	}
	setDigest, err := recoverypermit.DigestReceiptSet(epoch, receipts)
	if err != nil {
		t.Fatal(err)
	}
	dispositions := []BuildDisposition{
		{BuildName: "build-a", BuildUID: "build-a-uid", PodUID: "build-a-pod-uid", ReceiptLinks: []ReceiptLink{{Index: 0}, {Index: 1}}, Outcome: "terminal-result", ResultRecordDigest: hash('d'), NeverReplay: true},
		{BuildName: "build-b", ReceiptLinks: []ReceiptLink{{Index: 2}}, Outcome: "approved-unknown-discard", ApprovalDigest: hash('e'), NeverReplay: true},
	}
	physical := PhysicalRetirement{
		OldWorkerPoolID: "old-pool", OldCapacitySlots: 128,
		ServiceProcessesDigest: hash('1'), RunnerProcessesDigest: hash('2'),
		WorkerPoolDigest: hash('3'), NetworkIsolationDigest: hash('4'),
		InflightEffectsDigest: hash('5'), Assertion: retirementAssertion,
		RetiredAt: "2026-10-01T00:00:00Z",
	}
	auth := AuthorizationPayload{
		Version: version, Audience: authorizationAudience, Action: authorizationAction,
		Issuer: "external-operator", KeyID: "key-1", IncidentID: "incident-1", Source: epoch,
		StopIntentDigest: hash('6'), DrainPermitDigest: hash('7'), OccupancyReportDigest: hash('8'),
		ReceiptInventoryDigest: hash('9'), ReceiptSetDigest: setDigest, Receipts: receipts,
		Dispositions: dispositions, PhysicalRetirement: physical, SuccessorPlanDigest: hash('f'),
		IssuedAt: "2026-10-01T00:01:00Z",
	}
	successor := SuccessorIdentity{
		RunnerNamespace: "new-runner", RunnerNamespaceUID: "new-runner-uid",
		ReceiptNamespace: "new-receipts", ReceiptNamespaceUID: "new-receipts-uid",
		GenesisName: "genesis", GenesisUID: "new-genesis-uid", Generation: strings.Repeat("b", 32),
		WorkerPoolID: "new-pool", CapacitySlots: 128, CapacityProofDigest: hash('a'),
		RouteNamespace: "system", RouteName: "kova-route", RouteUID: "route-uid",
		RouteExpectedResourceVersion: "100", RouteCommittedResourceVersion: "101",
		RouteFromNamespaceUID: epoch.NamespaceUID, RouteToNamespaceUID: "new-runner-uid",
		RouteCASDigest: hash('b'),
	}
	runnerRetirement := NamespaceRetirement{
		Name: epoch.Namespace, OldUID: epoch.NamespaceUID,
		UIDAbsenceEvidenceDigest: hash('c'), NoReuseFenceDigest: hash('d'), NormalDeletionDigest: hash('e'),
	}
	receiptRetirement := NamespaceRetirement{
		Name: epoch.ReceiptNamespace, OldUID: epoch.ReceiptNamespaceUID,
		UIDAbsenceEvidenceDigest: hash('f'), NoReuseFenceDigest: hash('1'), NormalDeletionDigest: hash('2'),
	}
	closure := ClosurePayload{
		Version: version, Audience: closureAudience, Action: closureAction,
		Issuer: auth.Issuer, KeyID: auth.KeyID, IncidentID: auth.IncidentID,
		Source: epoch, RunnerRetirement: runnerRetirement, ReceiptRetirement: receiptRetirement,
		NamespaceAssertion: namespaceAssertion, SuccessorPlanDigest: auth.SuccessorPlanDigest,
		SingleSuccessorFenceDigest: hash('3'),
		Successor:                  successor, ObservedAt: "2026-10-01T00:02:00Z", IssuedAt: "2026-10-01T00:03:00Z",
	}
	return fixture{
		auth: auth, closure: closure, private: private,
		roots: TrustRoots{auth.Issuer: {auth.KeyID: public}},
		authExpected: AuthorizationExpectation{
			Issuer: auth.Issuer, KeyID: auth.KeyID, IncidentID: auth.IncidentID,
			Source: epoch, StopIntentDigest: auth.StopIntentDigest,
			DrainPermitDigest: auth.DrainPermitDigest, OccupancyReportDigest: auth.OccupancyReportDigest,
			ReceiptInventoryDigest: auth.ReceiptInventoryDigest, Receipts: receipts,
			Dispositions: dispositions, PhysicalRetirement: physical, SuccessorPlanDigest: auth.SuccessorPlanDigest,
		},
		closeExpected: ClosureExpectation{
			Issuer: closure.Issuer, KeyID: closure.KeyID, IncidentID: closure.IncidentID,
			Source: epoch, RunnerRetirement: runnerRetirement,
			ReceiptRetirement: receiptRetirement, SuccessorPlanDigest: closure.SuccessorPlanDigest,
			SingleSuccessorFenceDigest: closure.SingleSuccessorFenceDigest,
			Successor:                  successor,
		},
	}
}

func sign(t *testing.T, payload any, domain []byte, key ed25519.PrivateKey) string {
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	msg := append(append([]byte{}, domain...), data...)
	return base64.StdEncoding.EncodeToString(ed25519.Sign(key, msg))
}

func (f fixture) authorizationRaw(t *testing.T) []byte {
	t.Helper()
	raw, err := json.Marshal(AuthorizationEnvelope{Payload: f.auth, Signature: sign(t, f.auth, authorizationDomain, f.private)})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func (f fixture) closureRaw(t *testing.T) []byte {
	t.Helper()
	raw, err := json.Marshal(ClosureEnvelope{Payload: f.closure, Signature: sign(t, f.closure, closureDomain, f.private)})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func verifyFixtureAuthorization(t *testing.T, f fixture) VerifiedAuthorization {
	t.Helper()
	raw := f.authorizationRaw(t)
	f.authExpected.EnvelopeDigest = digestBytes(raw)
	got, err := VerifyAuthorization(raw, f.authExpected, f.roots)
	if err != nil {
		t.Fatal(err)
	}
	if got.ReceiptCount() != 3 || got.ReceiptSetDigest() != f.auth.ReceiptSetDigest || got.EnvelopeDigest() != f.authExpected.EnvelopeDigest {
		t.Fatalf("wrong verified authorization: %+v", got)
	}
	return got
}

func TestVerifyAuthorizationAndClosure(t *testing.T) {
	f := newFixture(t)
	auth := verifyFixtureAuthorization(t, f)
	f.closure.AuthorizationDigest = auth.EnvelopeDigest()
	f.closeExpected.AuthorizationDigest = auth.EnvelopeDigest()
	raw := f.closureRaw(t)
	f.closeExpected.EnvelopeDigest = digestBytes(raw)
	got, err := VerifyClosure(raw, auth, f.closeExpected, f.roots)
	if err != nil {
		t.Fatal(err)
	}
	if got.Successor != f.closure.Successor || got.AuthorizationDigest != auth.EnvelopeDigest() {
		t.Fatalf("wrong verified closure: %+v", got)
	}
}

func TestAuthorizationFailsClosed(t *testing.T) {
	tests := map[string]func(*fixture){
		"missing approval": func(f *fixture) {
			f.auth.Dispositions[1].ApprovalDigest = ""
			f.authExpected.Dispositions = f.auth.Dispositions
		},
		"missing pod uid": func(f *fixture) {
			f.auth.Dispositions[0].PodUID = ""
			f.authExpected.Dispositions = f.auth.Dispositions
		},
		"replay allowed": func(f *fixture) {
			f.auth.Dispositions[0].NeverReplay = false
			f.authExpected.Dispositions = f.auth.Dispositions
		},
		"duplicate receipt link": func(f *fixture) {
			f.auth.Dispositions[1].ReceiptLinks = []ReceiptLink{{Index: 0}}
			f.authExpected.Dispositions = f.auth.Dispositions
		},
		"uncovered receipt": func(f *fixture) {
			f.auth.Dispositions[1].ReceiptLinks = []ReceiptLink{{Index: 1}}
			f.authExpected.Dispositions = f.auth.Dispositions
		},
		"receipt count cap": func(f *fixture) {
			for i := 0; i < maxGrantReceipts; i++ {
				f.auth.Receipts = append(f.auth.Receipts, f.auth.Receipts[0])
			}
			f.authExpected.Receipts = f.auth.Receipts
		},
		"missing physical proof": func(f *fixture) {
			f.auth.PhysicalRetirement.InflightEffectsDigest = ""
			f.authExpected.PhysicalRetirement = f.auth.PhysicalRetirement
		},
		"wrong audience":           func(f *fixture) { f.auth.Audience = "kova-recovery-drain-v1" },
		"wrong action":             func(f *fixture) { f.auth.Action = "release-capacity" },
		"unsupported version":      func(f *fixture) { f.auth.Version = "2" },
		"issued before retirement": func(f *fixture) { f.auth.IssuedAt = "2025-10-01T00:01:00Z" },
	}
	for name, alter := range tests {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			alter(&f)
			f.authExpected.EnvelopeDigest = digestBytes(f.authorizationRaw(t))
			_, err := VerifyAuthorization(f.authorizationRaw(t), f.authExpected, f.roots)
			if !errors.Is(err, ErrUnqualified) {
				t.Fatalf("expected unqualified, got %v", err)
			}
		})
	}
}

func TestAuthorizationRejectsUnpinnedOrNoncanonicalEnvelope(t *testing.T) {
	f := newFixture(t)
	raw := f.authorizationRaw(t)
	f.authExpected.EnvelopeDigest = hash('0')
	if _, err := VerifyAuthorization(raw, f.authExpected, f.roots); !errors.Is(err, ErrUnqualified) {
		t.Fatalf("unpinned envelope: %v", err)
	}
	f.authExpected.EnvelopeDigest = digestBytes(raw)
	pretty := append([]byte("\n"), raw...)
	f.authExpected.EnvelopeDigest = digestBytes(pretty)
	if _, err := VerifyAuthorization(pretty, f.authExpected, f.roots); !errors.Is(err, ErrUnqualified) {
		t.Fatalf("noncanonical envelope: %v", err)
	}
	withUnknown := append(append([]byte{}, raw[:len(raw)-1]...), []byte(",\"unexpected\":true}")...)
	f.authExpected.EnvelopeDigest = digestBytes(withUnknown)
	if _, err := VerifyAuthorization(withUnknown, f.authExpected, f.roots); !errors.Is(err, ErrUnqualified) {
		t.Fatalf("unknown field: %v", err)
	}
}

func TestAuthorizationRejectsSignatureAndExternalMismatch(t *testing.T) {
	f := newFixture(t)
	raw := f.authorizationRaw(t)
	f.authExpected.EnvelopeDigest = digestBytes(raw)
	f.roots = TrustRoots{}
	if _, err := VerifyAuthorization(raw, f.authExpected, f.roots); !errors.Is(err, ErrUnqualified) {
		t.Fatalf("unknown issuer: %v", err)
	}
	f = newFixture(t)
	f.authExpected.StopIntentDigest = hash('0')
	raw = f.authorizationRaw(t)
	f.authExpected.EnvelopeDigest = digestBytes(raw)
	if _, err := VerifyAuthorization(raw, f.authExpected, f.roots); !errors.Is(err, ErrUnqualified) {
		t.Fatalf("unmatched stop digest: %v", err)
	}
	f = newFixture(t)
	f.authExpected.Receipts[0].UID = "other-uid"
	raw = f.authorizationRaw(t)
	f.authExpected.EnvelopeDigest = digestBytes(raw)
	if _, err := VerifyAuthorization(raw, f.authExpected, f.roots); !errors.Is(err, ErrUnqualified) {
		t.Fatalf("unmatched receipt UID: %v", err)
	}
}

func TestClosureFailsClosed(t *testing.T) {
	tests := map[string]func(*fixture){
		"old runner uid remains": func(f *fixture) {
			f.closure.Successor.RunnerNamespaceUID = f.auth.Source.NamespaceUID
			f.closeExpected.Successor = f.closure.Successor
		},
		"old receipt uid remains": func(f *fixture) {
			f.closure.Successor.ReceiptNamespaceUID = f.auth.Source.ReceiptNamespaceUID
			f.closeExpected.Successor = f.closure.Successor
		},
		"missing no-reuse fence": func(f *fixture) {
			f.closure.RunnerRetirement.NoReuseFenceDigest = ""
			f.closeExpected.RunnerRetirement = f.closure.RunnerRetirement
		},
		"missing normal deletion": func(f *fixture) {
			f.closure.ReceiptRetirement.NormalDeletionDigest = ""
			f.closeExpected.ReceiptRetirement = f.closure.ReceiptRetirement
		},
		"reused worker pool": func(f *fixture) {
			f.closure.Successor.WorkerPoolID = f.auth.Source.WorkerPoolID
			f.closeExpected.Successor = f.closure.Successor
		},
		"missing capacity proof": func(f *fixture) {
			f.closure.Successor.CapacityProofDigest = ""
			f.closeExpected.Successor = f.closure.Successor
		},
		"route CAS version not changed": func(f *fixture) {
			f.closure.Successor.RouteCommittedResourceVersion = f.closure.Successor.RouteExpectedResourceVersion
			f.closeExpected.Successor = f.closure.Successor
		},
		"route wrong old target": func(f *fixture) {
			f.closure.Successor.RouteFromNamespaceUID = "different-uid"
			f.closeExpected.Successor = f.closure.Successor
		},
		"missing route CAS": func(f *fixture) {
			f.closure.Successor.RouteCASDigest = ""
			f.closeExpected.Successor = f.closure.Successor
		},
		"missing successor fence": func(f *fixture) {
			f.closure.SingleSuccessorFenceDigest = ""
			f.closeExpected.SingleSuccessorFenceDigest = ""
		},
		"wrong action":       func(f *fixture) { f.closure.Action = "release-capacity" },
		"observed too early": func(f *fixture) { f.closure.ObservedAt = "2026-09-30T00:00:00Z" },
	}
	for name, alter := range tests {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			auth := verifyFixtureAuthorization(t, f)
			f.closure.AuthorizationDigest = auth.EnvelopeDigest()
			f.closeExpected.AuthorizationDigest = auth.EnvelopeDigest()
			alter(&f)
			raw := f.closureRaw(t)
			f.closeExpected.EnvelopeDigest = digestBytes(raw)
			_, err := VerifyClosure(raw, auth, f.closeExpected, f.roots)
			if !errors.Is(err, ErrUnqualified) {
				t.Fatalf("expected unqualified, got %v", err)
			}
		})
	}
	f := newFixture(t)
	f.closure.AuthorizationDigest = hash('a')
	f.closeExpected.AuthorizationDigest = f.closure.AuthorizationDigest
	raw := f.closureRaw(t)
	f.closeExpected.EnvelopeDigest = digestBytes(raw)
	if _, err := VerifyClosure(raw, VerifiedAuthorization{}, f.closeExpected, f.roots); !errors.Is(err, ErrUnqualified) {
		t.Fatalf("unverified prior authorization: %v", err)
	}
}
