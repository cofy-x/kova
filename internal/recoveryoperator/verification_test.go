package recoveryoperator

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cofy-x/kova/internal/service/recoverydisposal"
	"github.com/cofy-x/kova/internal/service/recoverypermit"
)

type verificationFixture struct {
	dir               string
	pinsPath          string
	authorizationPath string
	closurePath       string
	pins              VerificationPins
	authorization     recoverydisposal.AuthorizationEnvelope
	closure           recoverydisposal.ClosureEnvelope
	closureSigningKey ed25519.PrivateKey
}

func verificationHash(c string) string { return "sha256:" + strings.Repeat(c, 64) }

func writeVerificationJSON(t *testing.T, path string, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return raw
}

func signVerification(t *testing.T, value any, domain string, key ed25519.PrivateKey) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(ed25519.Sign(key, append([]byte(domain), raw...)))
}

func newVerificationFixture(t *testing.T) verificationFixture {
	t.Helper()
	dir := t.TempDir()
	f := verificationFixture{dir: dir, pinsPath: filepath.Join(dir, "verify-pins.json"),
		authorizationPath: filepath.Join(dir, "authorization.json"), closurePath: filepath.Join(dir, "closure.json")}
	authKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{6}, ed25519.SeedSize))
	f.closureSigningKey = ed25519.NewKeyFromSeed(bytes.Repeat([]byte{7}, ed25519.SeedSize))
	source := recoverypermit.EpochIdentity{Namespace: "old-runner", NamespaceUID: "old-runner-uid", ReceiptNamespace: "old-receipts",
		ReceiptNamespaceUID: "old-receipts-uid", GenesisName: "genesis", GenesisUID: "old-genesis-uid", Generation: strings.Repeat("a", 32),
		ActiveLedgerUID: "active-ledger-uid", QueueLedgerUID: "queue-ledger-uid", WorkerPoolID: "old-pool"}
	receipts := []recoverypermit.ReceiptRef{{Kind: "queue", Namespace: source.ReceiptNamespace,
		Name: "kova-admission-intent-" + strings.Repeat("b", 32), UID: "queue-receipt-uid", DataDigest: verificationHash("1")}}
	setDigest, err := recoverypermit.DigestReceiptSet(source, receipts)
	if err != nil {
		t.Fatal(err)
	}
	dispositions := []recoverydisposal.BuildDisposition{{BuildName: "old-build", ReceiptLinks: []recoverydisposal.ReceiptLink{{Index: 0}},
		Outcome: "approved-unknown-discard", ApprovalDigest: verificationHash("2"), NeverReplay: true}}
	physical := recoverydisposal.PhysicalRetirement{OldWorkerPoolID: source.WorkerPoolID, OldCapacitySlots: 2,
		ServiceProcessesDigest: verificationHash("3"), RunnerProcessesDigest: verificationHash("4"), WorkerPoolDigest: verificationHash("5"),
		NetworkIsolationDigest: verificationHash("6"), InflightEffectsDigest: verificationHash("7"),
		Assertion: "old-service-runner-workers-network-and-inflight-effects-cannot-resume-v1", RetiredAt: "2026-10-01T00:00:00Z"}
	auth := recoverydisposal.AuthorizationPayload{Version: "1", Audience: "kova-recovery-disposal-authorization-v1",
		Action: "dispose-old-epoch-liabilities-only", Issuer: "auth-issuer", KeyID: "auth-key", IncidentID: "incident-62",
		Source: source, StopIntentDigest: verificationHash("8"), DrainPermitDigest: verificationHash("9"),
		OccupancyReportDigest: verificationHash("a"), ReceiptInventoryDigest: verificationHash("b"), ReceiptSetDigest: setDigest,
		Receipts: receipts, Dispositions: dispositions, PhysicalRetirement: physical, SuccessorPlanDigest: verificationHash("c"),
		IssuedAt: "2026-10-01T00:01:00Z"}
	f.authorization = recoverydisposal.AuthorizationEnvelope{Payload: auth,
		Signature: signVerification(t, auth, "KOVA-RECOVERY-DISPOSAL-AUTHORIZATION-V1\x00", authKey)}
	authRaw := writeVerificationJSON(t, f.authorizationPath, f.authorization)
	retirement := recoverydisposal.NamespaceRetirement{Name: source.Namespace, OldUID: source.NamespaceUID,
		UIDAbsenceEvidenceDigest: verificationHash("d"), NoReuseFenceDigest: verificationHash("e"), NormalDeletionDigest: verificationHash("f")}
	receiptRetirement := recoverydisposal.NamespaceRetirement{Name: source.ReceiptNamespace, OldUID: source.ReceiptNamespaceUID,
		UIDAbsenceEvidenceDigest: verificationHash("1"), NoReuseFenceDigest: verificationHash("2"), NormalDeletionDigest: verificationHash("3")}
	successor := recoverydisposal.SuccessorIdentity{RunnerNamespace: "new-runner", RunnerNamespaceUID: "new-runner-uid",
		ReceiptNamespace: "new-receipts", ReceiptNamespaceUID: "new-receipts-uid", GenesisName: "genesis", GenesisUID: "new-genesis-uid",
		Generation: strings.Repeat("d", 32), WorkerPoolID: "new-pool", CapacitySlots: 2, CapacityProofDigest: verificationHash("4"),
		RouteNamespace: "system", RouteName: "kova-route", RouteUID: "route-uid", RouteExpectedResourceVersion: "100",
		RouteCommittedResourceVersion: "101", RouteFromNamespaceUID: source.NamespaceUID, RouteToNamespaceUID: "new-runner-uid",
		RouteCASDigest: verificationHash("5")}
	closePayload := recoverydisposal.ClosurePayload{Version: "1", Audience: "kova-recovery-disposal-closure-v1",
		Action: "certify-one-successor-after-old-uid-retirement-only", Issuer: "closure-issuer", KeyID: "closure-key", IncidentID: auth.IncidentID,
		AuthorizationDigest: digest(authRaw), Source: source, RunnerRetirement: retirement, ReceiptRetirement: receiptRetirement,
		NamespaceAssertion:  "old-runner-and-receipt-namespace-uids-absent-with-persistent-no-reuse-fences-v1",
		SuccessorPlanDigest: auth.SuccessorPlanDigest, SingleSuccessorFenceDigest: verificationHash("6"), Successor: successor,
		ObservedAt: "2026-10-01T00:02:00Z", IssuedAt: "2026-10-01T00:03:00Z"}
	f.closure = recoverydisposal.ClosureEnvelope{Payload: closePayload,
		Signature: signVerification(t, closePayload, "KOVA-RECOVERY-DISPOSAL-CLOSURE-V1\x00", f.closureSigningKey)}
	closureRaw := writeVerificationJSON(t, f.closurePath, f.closure)
	f.pins = VerificationPins{Version: "1", Action: "verify-closure",
		Authorization: &recoverydisposal.AuthorizationExpectation{EnvelopeDigest: digest(authRaw), Issuer: auth.Issuer, KeyID: auth.KeyID,
			IncidentID: auth.IncidentID, Source: source, StopIntentDigest: auth.StopIntentDigest, DrainPermitDigest: auth.DrainPermitDigest,
			OccupancyReportDigest: auth.OccupancyReportDigest, ReceiptInventoryDigest: auth.ReceiptInventoryDigest, Receipts: receipts,
			Dispositions: dispositions, PhysicalRetirement: physical, SuccessorPlanDigest: auth.SuccessorPlanDigest},
		AuthorizationRoots: []PublicKeyPin{{Issuer: auth.Issuer, KeyID: auth.KeyID, PublicKeyHex: hex.EncodeToString(authKey.Public().(ed25519.PublicKey))}},
		Closure: &recoverydisposal.ClosureExpectation{EnvelopeDigest: digest(closureRaw), Issuer: closePayload.Issuer,
			KeyID: closePayload.KeyID, IncidentID: closePayload.IncidentID, AuthorizationDigest: closePayload.AuthorizationDigest,
			Source: source, RunnerRetirement: retirement, ReceiptRetirement: receiptRetirement,
			SuccessorPlanDigest: closePayload.SuccessorPlanDigest, SingleSuccessorFenceDigest: closePayload.SingleSuccessorFenceDigest,
			Successor: successor},
		ClosureRoots: []PublicKeyPin{{Issuer: closePayload.Issuer, KeyID: closePayload.KeyID,
			PublicKeyHex: hex.EncodeToString(f.closureSigningKey.Public().(ed25519.PublicKey))}}}
	writeVerificationJSON(t, f.pinsPath, f.pins)
	return f
}

func (f verificationFixture) pinsDigest(t *testing.T) string {
	t.Helper()
	return digest(writeVerificationJSON(t, f.pinsPath, f.pins))
}

func TestVerifyRecordsAreOfflineAndDoNotClaimIncidentClosure(t *testing.T) {
	f := newVerificationFixture(t)
	report, err := verifyRecords("verify-closure", f.pinsPath, f.pinsDigest(t), f.authorizationPath, f.closurePath)
	if err != nil {
		t.Fatal(err)
	}
	if report.Stage != "closure-verified" || report.ProofScope != "schema-and-signature-only; external-evidence-not-observed" ||
		report.IncidentID != "incident-62" || report.ReceiptCount != 1 || report.ClosureDigest != f.pins.Closure.EnvelopeDigest ||
		report.Successor == nil || report.Successor.RunnerNamespaceUID != "new-runner-uid" || report.ExternalEvidenceObserved ||
		report.IncidentClosed || report.CapacityReleased || report.SuccessorCreated {
		t.Fatalf("incorrect report: %+v", report)
	}
	f.pins.Action, f.pins.Closure, f.pins.ClosureRoots = "verify-authorization", nil, nil
	report, err = verifyRecords("verify-authorization", f.pinsPath, f.pinsDigest(t), f.authorizationPath, "")
	if err != nil {
		t.Fatal(err)
	}
	if report.Stage != "authorization-verified" || report.ClosureDigest != "" || report.Successor != nil || report.IncidentClosed {
		t.Fatalf("incorrect authorization report: %+v", report)
	}
}

func TestVerifyRecordsRejectUnqualifiedInputs(t *testing.T) {
	tests := map[string]func(*testing.T, *verificationFixture) string{
		"wrong whole pins digest": func(t *testing.T, f *verificationFixture) string { return verificationHash("0") },
		"noncanonical pins": func(t *testing.T, f *verificationFixture) string {
			raw := writeVerificationJSON(t, f.pinsPath, f.pins)
			if err := os.WriteFile(f.pinsPath, append(raw, '\n'), 0600); err != nil {
				t.Fatal(err)
			}
			return digest(append(raw, '\n'))
		},
		"unknown pins field": func(t *testing.T, f *verificationFixture) string {
			raw := writeVerificationJSON(t, f.pinsPath, f.pins)
			raw = append(raw[:len(raw)-1], []byte(`,"unexpected":true}`)...)
			if err := os.WriteFile(f.pinsPath, raw, 0600); err != nil {
				t.Fatal(err)
			}
			return digest(raw)
		},
		"unprivate pins": func(t *testing.T, f *verificationFixture) string {
			if err := os.Chmod(f.pinsPath, 0644); err != nil {
				t.Fatal(err)
			}
			return digest(mustReadVerification(t, f.pinsPath))
		},
		"unprivate envelope": func(t *testing.T, f *verificationFixture) string {
			if err := os.Chmod(f.closurePath, 0644); err != nil {
				t.Fatal(err)
			}
			return f.pinsDigest(t)
		},
		"wrong authorization expectation": func(t *testing.T, f *verificationFixture) string {
			f.pins.Authorization.PhysicalRetirement.OldCapacitySlots++
			return f.pinsDigest(t)
		},
		"wrong authorization root": func(t *testing.T, f *verificationFixture) string {
			f.pins.AuthorizationRoots[0].PublicKeyHex = f.pins.ClosureRoots[0].PublicKeyHex
			return f.pinsDigest(t)
		},
		"wrong closure root": func(t *testing.T, f *verificationFixture) string {
			f.pins.ClosureRoots[0].PublicKeyHex = f.pins.AuthorizationRoots[0].PublicKeyHex
			return f.pinsDigest(t)
		},
		"tampered signed closure": func(t *testing.T, f *verificationFixture) string {
			f.closure.Payload.Successor.CapacitySlots++
			f.pins.Closure.Successor.CapacitySlots++
			f.pins.Closure.EnvelopeDigest = digest(writeVerificationJSON(t, f.closurePath, f.closure))
			return f.pinsDigest(t)
		},
		"wrong closure authorization binding": func(t *testing.T, f *verificationFixture) string {
			f.pins.Closure.AuthorizationDigest = verificationHash("f")
			return f.pinsDigest(t)
		},
		"missing closure roots": func(t *testing.T, f *verificationFixture) string {
			f.pins.ClosureRoots = nil
			return f.pinsDigest(t)
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			f := newVerificationFixture(t)
			pin := mutate(t, &f)
			if _, err := verifyRecords("verify-closure", f.pinsPath, pin, f.authorizationPath, f.closurePath); !errors.Is(err, ErrInput) {
				t.Fatalf("expected redacted input error, got %v", err)
			}
		})
	}
}

func mustReadVerification(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestVerificationCLIEmitsOnlyQualifiedJSON(t *testing.T) {
	f := newVerificationFixture(t)
	var output bytes.Buffer
	app := NewCLIApp()
	app.Writer = &output
	if err := app.Run([]string{"kova-recovery", "verify-closure", "--pins", f.pinsPath, "--pins-digest", f.pinsDigest(t),
		"--authorization", f.authorizationPath, "--closure", f.closurePath}); err != nil {
		t.Fatal(err)
	}
	var report VerificationReport
	if json.Unmarshal(output.Bytes(), &report) != nil || report.Stage != "closure-verified" || report.IncidentClosed || report.ExternalEvidenceObserved {
		t.Fatalf("incorrect CLI JSON: %q", output.String())
	}
	output.Reset()
	if err := app.Run([]string{"kova-recovery", "verify-closure", "--pins", f.pinsPath, "--pins-digest", verificationHash("0"),
		"--authorization", f.authorizationPath, "--closure", f.closurePath}); !errors.Is(err, ErrInput) || output.Len() != 0 {
		t.Fatalf("failed verification emitted output or wrong error: %v %q", err, output.String())
	}
	f.pins.Action, f.pins.Closure, f.pins.ClosureRoots = "verify-authorization", nil, nil
	output.Reset()
	if err := app.Run([]string{"kova-recovery", "verify-authorization", "--pins", f.pinsPath, "--pins-digest", f.pinsDigest(t),
		"--authorization", f.authorizationPath}); err != nil {
		t.Fatal(err)
	}
	report = VerificationReport{}
	if json.Unmarshal(output.Bytes(), &report) != nil || report.Stage != "authorization-verified" || report.ClosureDigest != "" {
		t.Fatalf("incorrect authorization CLI JSON: %q", output.String())
	}
}
