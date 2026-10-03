package recoverydisposal

import (
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
)

type executionFixture struct {
	payload ExecutionPlanPayload
	roots   TrustRoots
	private ed25519.PrivateKey
}

func newExecutionFixture(t *testing.T) executionFixture {
	t.Helper()
	f := newFixture(t)
	p := ExecutionPlanPayload{
		Version: ExecutionPlanVersion, Audience: ExecutionPlanAudience, Action: ExecutionPlanAction,
		Issuer: f.auth.Issuer, KeyID: f.auth.KeyID, IncidentID: f.auth.IncidentID, Source: f.auth.Source,
		Cluster:             ExecutionClusterIdentity{SystemNamespace: "kube-system", SystemNamespaceUID: "system-uid", APIIdentityDigest: hash('1')},
		AuthorizationDigest: hash('2'), IssuedAt: "2026-10-01T00:03:00Z",
		Evidence: ExecutionEvidence{
			PhysicalRetirement: f.auth.PhysicalRetirement, StopIntentDigest: hash('3'), InflightBarrierDigest: hash('4'),
			FullArchiveManifestDigest: hash('5'), StorageIdentityDigest: hash('6'), ArchiveAssertion: ExecutionArchiveAssertion,
			ArchiveBytes: 8192, InventoryObserverDigest: hash('7'), InventoryAssertion: ExecutionInventoryAssertion,
			RunnerNoReuseFenceDigest: hash('8'), ReceiptNoReuseFenceDigest: hash('9'), NoReuseAssertion: ExecutionNoReuseAssertion,
			ObservedAt: "2026-10-01T00:02:00Z",
		},
		Limits: ExecutionLimits{MaxObjects: 64, MaxArchiveObjects: 64, MaxDispositions: 64, MaxCalls: 256, MaxObjectArchiveBytes: 4096, MaxArchiveBytes: 65536},
		Dispositions: []ExecutionDisposition{
			{ID: "admin", BuildName: "build-admin", OriginalBuildUID: "admin-build-uid", Outcome: "approved-unknown-discard", ApprovalDigest: hash('a'), NeverReplay: true},
			{ID: "lost", BuildName: "build-lost", OriginalBuildUID: "lost-build-uid", OriginalPodUID: "lost-pod-uid", Outcome: "approved-unknown-discard", ApprovalDigest: hash('b'), NeverReplay: true},
			{ID: "terminal", BuildName: "build-terminal", OriginalBuildUID: "terminal-build-uid", OriginalPodUID: "terminal-pod-uid", Outcome: "terminal-result", ResultRecordDigest: hash('c'), NeverReplay: true},
		},
	}
	add := func(role, disposition, name, uid string) {
		target := ExecutionTarget{
			Resource: ExecutionResource{Version: "v1", Resource: "configmaps"}, Namespace: p.Source.Namespace,
			NamespaceUID: p.Source.NamespaceUID, Name: name, UID: uid, Role: role, DispositionID: disposition,
			ArchiveResourceVersion: "123", ArchiveDigest: hash('d'), ArchiveBytes: 128,
			QualificationVersion: ExecutionQualificationVersion, QualificationDigest: hash('e'), AllowedKovaFinalizers: []string{},
		}
		switch role {
		case "original-build", "build-tombstone":
			target.Resource = ExecutionResource{Group: kovav1.Group, Version: kovav1.Version, Resource: "kovabuilds"}
			target.AllowedKovaFinalizers = []string{executionHoldFinalizer}
			if role == "original-build" {
				target.AllowedKovaFinalizers = []string{kovav1.CleanupFinalizer, executionHoldFinalizer}
			}
		case "original-pod", "pod-tombstone":
			target.Resource.Resource = "pods"
			target.AllowedKovaFinalizers = []string{executionHoldFinalizer}
		case "queue-receipt", "grant-receipt", "pod-create-receipt":
			target.Namespace, target.NamespaceUID = p.Source.ReceiptNamespace, p.Source.ReceiptNamespaceUID
		case "occupancy-attempt":
			target.AllowedKovaFinalizers = []string{executionHoldFinalizer}
		}
		p.Targets = append(p.Targets, target)
	}
	add("original-build", "admin", "build-admin", "admin-build-uid") // deliberately no receipt
	add("build-tombstone", "lost", "build-lost", "tombstone-build-uid")
	add("pod-tombstone", "lost", "lost-pod", "tombstone-pod-uid")
	add("original-build", "terminal", "build-terminal", "terminal-build-uid")
	add("original-pod", "terminal", "terminal-pod", "terminal-pod-uid")
	add("queue-receipt", "terminal", "queue-receipt", "queue-receipt-uid")
	add("grant-receipt", "terminal", "grant-receipt", "grant-receipt-uid")
	add("pod-create-receipt", "terminal", "pod-receipt", "pod-receipt-uid")
	add("occupancy-attempt", "lost", "occupancy-attempt", "occupancy-attempt-uid")
	add("stop-intent", "", "stop-intent", "stop-intent-uid")
	add("genesis", "", p.Source.GenesisName, p.Source.GenesisUID)
	add("active-ledger", "", "active-ledger", p.Source.ActiveLedgerUID)
	add("queue-ledger", "", "queue-ledger", p.Source.QueueLedgerUID)
	sortExecutionTargets(p.Targets)
	// The full archive also includes foreign/non-actionable records such as
	// kube-root-ca.crt. Being archived must not turn those into deletion targets.
	p.Evidence.ArchiveObjectCount = len(p.Targets) + 3
	return executionFixture{payload: p, roots: f.roots, private: f.private}
}

func sortExecutionTargets(targets []ExecutionTarget) {
	slices.SortFunc(targets, func(a, b ExecutionTarget) int { return strings.Compare(executionTargetKey(a), executionTargetKey(b)) })
}

func (f executionFixture) raw(t *testing.T, domain []byte) []byte {
	t.Helper()
	raw, err := json.Marshal(ExecutionPlanEnvelope{Payload: f.payload, Signature: sign(t, f.payload, domain, f.private)})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// Test fixtures simulate an independently supplied pin even for malformed
// statements. Never use this pattern to derive a production expectation from
// an incoming untrusted envelope.
func executionExpectation(t *testing.T, p ExecutionPlanPayload, raw []byte) ExecutionPlanExpectation {
	t.Helper()
	payloadRaw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	return ExecutionPlanExpectation{EnvelopeDigest: digestBytes(raw), PlanDigest: digestBytes(payloadRaw), Payload: cloneExecutionPayload(p)}
}

func targetByRole(t *testing.T, p *ExecutionPlanPayload, role string) *ExecutionTarget {
	t.Helper()
	for i := range p.Targets {
		if p.Targets[i].Role == role {
			return &p.Targets[i]
		}
	}
	t.Fatalf("missing fixture role %q", role)
	return nil
}

func TestExecutionPlanIndependentUnreceiptedAndTombstoneInventory(t *testing.T) {
	f := newExecutionFixture(t)
	raw := f.raw(t, executionPlanDomain)
	expected := executionExpectation(t, f.payload, raw)
	got, err := VerifyExecutionPlan(raw, expected, f.roots)
	if err != nil || !got.Valid() || got.EnvelopeDigest() != expected.EnvelopeDigest || got.PlanDigest() != expected.PlanDigest {
		t.Fatalf("independent plan rejected: valid=%v err=%v", got.Valid(), err)
	}
	if got.Payload().Evidence.ArchiveObjectCount <= len(got.Payload().Targets) {
		t.Fatal("full archive must be allowed to include non-actionable records")
	}
	digest, err := DigestExecutionPlan(f.payload)
	if err != nil || digest != expected.PlanDigest {
		t.Fatalf("whole-plan digest: %q %v", digest, err)
	}
	if (VerifiedExecutionPlan{}).Valid() {
		t.Fatal("zero plan must not be sealed")
	}
}

func TestExecutionPlanEmptyTargetSetAndTerminalWithoutPod(t *testing.T) {
	f := newExecutionFixture(t)
	f.payload.Targets = []ExecutionTarget{}
	f.payload.Dispositions = []ExecutionDisposition{}
	raw := f.raw(t, executionPlanDomain)
	if _, err := VerifyExecutionPlan(raw, executionExpectation(t, f.payload, raw), f.roots); err != nil {
		t.Fatalf("complete archive containing no actionable targets: %v", err)
	}
	f = newExecutionFixture(t)
	f.payload.Dispositions[0].Outcome = "terminal-result"
	f.payload.Dispositions[0].ApprovalDigest = ""
	f.payload.Dispositions[0].ResultRecordDigest = hash('f')
	raw = f.raw(t, executionPlanDomain)
	if _, err := VerifyExecutionPlan(raw, executionExpectation(t, f.payload, raw), f.roots); err != nil {
		t.Fatalf("terminal admin build before any Pod existed: %v", err)
	}
}

func TestExecutionPlanDefensiveCopies(t *testing.T) {
	f := newExecutionFixture(t)
	raw := f.raw(t, executionPlanDomain)
	expected := executionExpectation(t, f.payload, raw)
	got, err := VerifyExecutionPlan(raw, expected, f.roots)
	if err != nil {
		t.Fatal(err)
	}
	original := got.Payload()
	expected.Payload.Dispositions[0].BuildName = "changed-input"
	targetByRole(t, &expected.Payload, "original-build").AllowedKovaFinalizers[0] = "foreign/input"
	copy := got.Payload()
	copy.Dispositions[0].BuildName = "changed-output"
	copy.Targets[0].Name = "changed-output"
	targetByRole(t, &copy, "original-build").AllowedKovaFinalizers[0] = "foreign/output"
	before, _ := json.Marshal(original)
	after, _ := json.Marshal(got.Payload())
	if string(before) != string(after) {
		t.Fatal("verified plan aliases caller or getter memory")
	}
}

func TestExecutionPlanMalformedStatementsFailClosed(t *testing.T) {
	tests := map[string]func(*ExecutionPlanPayload){
		"version":                       func(p *ExecutionPlanPayload) { p.Version = "2" },
		"audience":                      func(p *ExecutionPlanPayload) { p.Audience = authorizationAudience },
		"action":                        func(p *ExecutionPlanPayload) { p.Action = "delete-namespace" },
		"incident":                      func(p *ExecutionPlanPayload) { p.IncidentID = "bad\nincident" },
		"cluster name":                  func(p *ExecutionPlanPayload) { p.Cluster.SystemNamespace = "default" },
		"cluster uid":                   func(p *ExecutionPlanPayload) { p.Cluster.SystemNamespaceUID = "" },
		"api identity":                  func(p *ExecutionPlanPayload) { p.Cluster.APIIdentityDigest = "" },
		"equal namespace uid":           func(p *ExecutionPlanPayload) { p.Source.ReceiptNamespaceUID = p.Source.NamespaceUID },
		"system namespace targeted":     func(p *ExecutionPlanPayload) { p.Source.Namespace = "kube-system" },
		"system namespace uid targeted": func(p *ExecutionPlanPayload) { p.Source.NamespaceUID = p.Cluster.SystemNamespaceUID },
		"worker identity": func(p *ExecutionPlanPayload) {
			p.Source.WorkerPoolID = "bad?pool"
			p.Evidence.PhysicalRetirement.OldWorkerPoolID = "bad?pool"
		},
		"external authorization":   func(p *ExecutionPlanPayload) { p.AuthorizationDigest = "" },
		"physical inflight":        func(p *ExecutionPlanPayload) { p.Evidence.PhysicalRetirement.InflightEffectsDigest = "" },
		"api inflight barrier":     func(p *ExecutionPlanPayload) { p.Evidence.InflightBarrierDigest = "" },
		"archive manifest":         func(p *ExecutionPlanPayload) { p.Evidence.FullArchiveManifestDigest = "" },
		"archive storage":          func(p *ExecutionPlanPayload) { p.Evidence.StorageIdentityDigest = "" },
		"archive assertion":        func(p *ExecutionPlanPayload) { p.Evidence.ArchiveAssertion = "inside-old-namespace" },
		"archive count":            func(p *ExecutionPlanPayload) { p.Evidence.ArchiveObjectCount = len(p.Targets) - 1 },
		"observer":                 func(p *ExecutionPlanPayload) { p.Evidence.InventoryObserverDigest = "" },
		"filtered inventory":       func(p *ExecutionPlanPayload) { p.Evidence.InventoryAssertion = "labelled-objects-only" },
		"runner fence":             func(p *ExecutionPlanPayload) { p.Evidence.RunnerNoReuseFenceDigest = "" },
		"receipt fence":            func(p *ExecutionPlanPayload) { p.Evidence.ReceiptNoReuseFenceDigest = "" },
		"no reuse assertion":       func(p *ExecutionPlanPayload) { p.Evidence.NoReuseAssertion = "timeout-expired" },
		"early observation":        func(p *ExecutionPlanPayload) { p.Evidence.ObservedAt = "2020-01-01T00:00:00Z" },
		"early issuance":           func(p *ExecutionPlanPayload) { p.IssuedAt = p.Evidence.PhysicalRetirement.RetiredAt },
		"bad timestamp":            func(p *ExecutionPlanPayload) { p.IssuedAt = "tomorrow" },
		"nil targets":              func(p *ExecutionPlanPayload) { p.Targets = nil },
		"nil dispositions":         func(p *ExecutionPlanPayload) { p.Dispositions = nil },
		"missing original uid":     func(p *ExecutionPlanPayload) { p.Dispositions[2].OriginalBuildUID = "" },
		"duplicate original uid":   func(p *ExecutionPlanPayload) { p.Dispositions[1].OriginalBuildUID = p.Dispositions[0].OriginalBuildUID },
		"duplicate disposition id": func(p *ExecutionPlanPayload) { p.Dispositions[1].ID = p.Dispositions[0].ID },
		"unsorted dispositions": func(p *ExecutionPlanPayload) {
			p.Dispositions[0], p.Dispositions[1] = p.Dispositions[1], p.Dispositions[0]
		},
		"unknown lacks approval":       func(p *ExecutionPlanPayload) { p.Dispositions[0].ApprovalDigest = "" },
		"terminal lacks result":        func(p *ExecutionPlanPayload) { p.Dispositions[2].ResultRecordDigest = "" },
		"ambiguous outcome":            func(p *ExecutionPlanPayload) { p.Dispositions[0].ResultRecordDigest = hash('1') },
		"replay":                       func(p *ExecutionPlanPayload) { p.Dispositions[0].NeverReplay = false },
		"unknown role":                 func(p *ExecutionPlanPayload) { p.Targets[0].Role = "any-object" },
		"namespace gvr":                func(p *ExecutionPlanPayload) { p.Targets[0].Resource.Resource = "namespaces" },
		"subresource":                  func(p *ExecutionPlanPayload) { p.Targets[0].Resource.Resource = "configmaps/status" },
		"foreign namespace":            func(p *ExecutionPlanPayload) { p.Targets[0].Namespace = "new-runner" },
		"replacement namespace":        func(p *ExecutionPlanPayload) { p.Targets[0].NamespaceUID = "new-uid" },
		"empty target uid":             func(p *ExecutionPlanPayload) { p.Targets[0].UID = "" },
		"missing rv":                   func(p *ExecutionPlanPayload) { p.Targets[0].ArchiveResourceVersion = "" },
		"missing archive digest":       func(p *ExecutionPlanPayload) { p.Targets[0].ArchiveDigest = "" },
		"qualification version":        func(p *ExecutionPlanPayload) { p.Targets[0].QualificationVersion = "future" },
		"missing qualification digest": func(p *ExecutionPlanPayload) { p.Targets[0].QualificationDigest = "" },
		"nil finalizers":               func(p *ExecutionPlanPayload) { p.Targets[0].AllowedKovaFinalizers = nil },
		"foreign finalizer":            func(p *ExecutionPlanPayload) { p.Targets[0].AllowedKovaFinalizers = []string{"foreign/finalizer"} },
		"cleanup on pod": func(p *ExecutionPlanPayload) {
			targetByRole(t, p, "original-pod").AllowedKovaFinalizers = []string{kovav1.CleanupFinalizer}
		},
		"hold on receipt": func(p *ExecutionPlanPayload) {
			targetByRole(t, p, "queue-receipt").AllowedKovaFinalizers = []string{executionHoldFinalizer}
		},
		"duplicate finalizer": func(p *ExecutionPlanPayload) {
			targetByRole(t, p, "original-build").AllowedKovaFinalizers = []string{executionHoldFinalizer, executionHoldFinalizer}
		},
		"unsorted finalizers": func(p *ExecutionPlanPayload) {
			targetByRole(t, p, "original-build").AllowedKovaFinalizers = []string{executionHoldFinalizer, kovav1.CleanupFinalizer}
		},
		"missing disposition":      func(p *ExecutionPlanPayload) { targetByRole(t, p, "original-build").DispositionID = "missing" },
		"control with disposition": func(p *ExecutionPlanPayload) { targetByRole(t, p, "genesis").DispositionID = "admin" },
		"build original replaced":  func(p *ExecutionPlanPayload) { targetByRole(t, p, "original-build").UID = "replacement-uid" },
		"build tombstone is original": func(p *ExecutionPlanPayload) {
			targetByRole(t, p, "build-tombstone").UID = p.Dispositions[1].OriginalBuildUID
		},
		"pod tombstone is original": func(p *ExecutionPlanPayload) {
			targetByRole(t, p, "pod-tombstone").UID = p.Dispositions[1].OriginalPodUID
		},
		"tombstone steals another original": func(p *ExecutionPlanPayload) {
			targetByRole(t, p, "build-tombstone").UID = p.Dispositions[2].OriginalPodUID
		},
		"genesis uid mismatch": func(p *ExecutionPlanPayload) { targetByRole(t, p, "genesis").UID = "replacement-genesis" },
		"ledger uid mismatch":  func(p *ExecutionPlanPayload) { targetByRole(t, p, "active-ledger").UID = "replacement-ledger" },
		"duplicate target": func(p *ExecutionPlanPayload) {
			p.Targets = append(p.Targets, p.Targets[0])
			sortExecutionTargets(p.Targets)
		},
		"duplicate target uid": func(p *ExecutionPlanPayload) {
			targetByRole(t, p, "stop-intent").UID = targetByRole(t, p, "occupancy-attempt").UID
		},
		"same address different uid": func(p *ExecutionPlanPayload) {
			extra := *targetByRole(t, p, "stop-intent")
			extra.UID = "other-uid"
			p.Targets = append(p.Targets, extra)
			sortExecutionTargets(p.Targets)
		},
		"unsorted targets": func(p *ExecutionPlanPayload) { p.Targets[0], p.Targets[1] = p.Targets[1], p.Targets[0] },
	}
	for name, alter := range tests {
		t.Run(name, func(t *testing.T) {
			f := newExecutionFixture(t)
			alter(&f.payload)
			raw := f.raw(t, executionPlanDomain)
			if got, err := VerifyExecutionPlan(raw, executionExpectation(t, f.payload, raw), f.roots); !errors.Is(err, ErrUnqualified) || got.Valid() {
				t.Fatalf("invalid independently signed statement accepted: valid=%v err=%v", got.Valid(), err)
			}
		})
	}
}

func TestExecutionPlanBudgetsFailClosed(t *testing.T) {
	tests := map[string]func(*ExecutionPlanPayload){
		"objects zero":                       func(p *ExecutionPlanPayload) { p.Limits.MaxObjects = 0 },
		"objects hard cap":                   func(p *ExecutionPlanPayload) { p.Limits.MaxObjects = maxExecutionObjects + 1 },
		"objects caller cap":                 func(p *ExecutionPlanPayload) { p.Limits.MaxObjects = len(p.Targets) - 1 },
		"archive object caller cap":          func(p *ExecutionPlanPayload) { p.Evidence.ArchiveObjectCount = p.Limits.MaxArchiveObjects + 1 },
		"archive object zero cap":            func(p *ExecutionPlanPayload) { p.Limits.MaxArchiveObjects = 0 },
		"archive object hard cap":            func(p *ExecutionPlanPayload) { p.Limits.MaxArchiveObjects = maxExecutionArchiveObjects + 1 },
		"dispositions zero":                  func(p *ExecutionPlanPayload) { p.Limits.MaxDispositions = 0 },
		"dispositions hard cap":              func(p *ExecutionPlanPayload) { p.Limits.MaxDispositions = maxExecutionDispositions + 1 },
		"dispositions caller cap":            func(p *ExecutionPlanPayload) { p.Limits.MaxDispositions = len(p.Dispositions) - 1 },
		"calls zero":                         func(p *ExecutionPlanPayload) { p.Limits.MaxCalls = 0 },
		"calls hard cap":                     func(p *ExecutionPlanPayload) { p.Limits.MaxCalls = maxExecutionCalls + 1 },
		"object bytes zero":                  func(p *ExecutionPlanPayload) { p.Targets[0].ArchiveBytes = 0 },
		"object bytes negative":              func(p *ExecutionPlanPayload) { p.Targets[0].ArchiveBytes = -1 },
		"object bytes caller cap":            func(p *ExecutionPlanPayload) { p.Targets[0].ArchiveBytes = p.Limits.MaxObjectArchiveBytes + 1 },
		"object bytes hard cap":              func(p *ExecutionPlanPayload) { p.Limits.MaxObjectArchiveBytes = maxExecutionObjectBytes + 1 },
		"archive bytes zero":                 func(p *ExecutionPlanPayload) { p.Evidence.ArchiveBytes = 0 },
		"archive bytes caller cap":           func(p *ExecutionPlanPayload) { p.Evidence.ArchiveBytes = p.Limits.MaxArchiveBytes + 1 },
		"archive bytes hard cap":             func(p *ExecutionPlanPayload) { p.Limits.MaxArchiveBytes = maxExecutionArchiveBytes + 1 },
		"archive bytes smaller than targets": func(p *ExecutionPlanPayload) { p.Evidence.ArchiveBytes = int64(len(p.Targets))*128 - 1 },
		"aggregate smaller than per-object":  func(p *ExecutionPlanPayload) { p.Limits.MaxArchiveBytes = p.Limits.MaxObjectArchiveBytes - 1 },
	}
	for name, alter := range tests {
		t.Run(name, func(t *testing.T) {
			f := newExecutionFixture(t)
			alter(&f.payload)
			if _, err := DigestExecutionPlan(f.payload); !errors.Is(err, ErrUnqualified) {
				t.Fatalf("invalid budget accepted: %v", err)
			}
		})
	}
	f := newExecutionFixture(t)
	f.payload.Limits = ExecutionLimits{MaxObjects: maxExecutionObjects, MaxArchiveObjects: maxExecutionArchiveObjects, MaxDispositions: maxExecutionDispositions, MaxCalls: maxExecutionCalls, MaxObjectArchiveBytes: maxExecutionObjectBytes, MaxArchiveBytes: maxExecutionArchiveBytes}
	f.payload.Evidence.ArchiveObjectCount = maxExecutionArchiveObjects
	f.payload.Evidence.ArchiveBytes = maxExecutionArchiveBytes
	f.payload.Targets[0].ArchiveBytes = maxExecutionObjectBytes
	if _, err := DigestExecutionPlan(f.payload); err != nil {
		t.Fatalf("inclusive budget maxima: %v", err)
	}
}

func TestExecutionPlanRejectsUnpinnedInputsAndWrongSignatures(t *testing.T) {
	f := newExecutionFixture(t)
	raw := f.raw(t, executionPlanDomain)
	base := executionExpectation(t, f.payload, raw)
	tests := map[string]func(*ExecutionPlanExpectation){
		"envelope digest":        func(e *ExecutionPlanExpectation) { e.EnvelopeDigest = hash('0') },
		"plan digest":            func(e *ExecutionPlanExpectation) { e.PlanDigest = hash('0') },
		"independent incident":   func(e *ExecutionPlanExpectation) { e.Payload.IncidentID = "other-incident" },
		"independent evidence":   func(e *ExecutionPlanExpectation) { e.Payload.Evidence.InflightBarrierDigest = hash('0') },
		"independent target uid": func(e *ExecutionPlanExpectation) { targetByRole(t, &e.Payload, "stop-intent").UID = "other-uid" },
		"independent archive":    func(e *ExecutionPlanExpectation) { e.Payload.Evidence.FullArchiveManifestDigest = hash('0') },
	}
	for name, alter := range tests {
		t.Run(name, func(t *testing.T) {
			e := base
			e.Payload = cloneExecutionPayload(base.Payload)
			alter(&e)
			// Even if the changed independent payload has its own correct digest,
			// it must still agree with the signed incoming statement.
			if name != "plan digest" {
				if digest, err := DigestExecutionPlan(e.Payload); err == nil {
					e.PlanDigest = digest
				}
			}
			if _, err := VerifyExecutionPlan(raw, e, f.roots); !errors.Is(err, ErrUnqualified) {
				t.Fatalf("unpinned input accepted: %v", err)
			}
		})
	}
	for _, domain := range [][]byte{authorizationDomain, closureDomain, []byte("KOVA-RECOVERY-DRAIN-PERMIT-V1\x00")} {
		t.Run(fmt.Sprintf("domain-%q", domain), func(t *testing.T) {
			raw := f.raw(t, domain)
			if _, err := VerifyExecutionPlan(raw, executionExpectation(t, f.payload, raw), f.roots); !errors.Is(err, ErrUnqualified) {
				t.Fatalf("cross-domain signature accepted: %v", err)
			}
		})
	}
	if _, err := VerifyExecutionPlan(raw, base, TrustRoots{}); !errors.Is(err, ErrUnqualified) {
		t.Fatalf("unknown issuer accepted: %v", err)
	}
	other := newExecutionFixture(t)
	if _, err := VerifyExecutionPlan(raw, base, other.roots); !errors.Is(err, ErrUnqualified) {
		t.Fatalf("wrong key accepted: %v", err)
	}
}

func TestExecutionPlanRejectsNoncanonicalAndOversizedEnvelopes(t *testing.T) {
	f := newExecutionFixture(t)
	raw := f.raw(t, executionPlanDomain)
	inputs := map[string][]byte{
		"empty":           {},
		"whitespace":      append([]byte("\n"), raw...),
		"unknown field":   []byte(strings.TrimSuffix(string(raw), "}") + `,"unknown":true}`),
		"duplicate field": []byte(strings.Replace(string(raw), `"version":"1"`, `"version":"1","version":"1"`, 1)),
		"trailing value":  append(slices.Clone(raw), []byte("{}")...),
		"oversized":       []byte(strings.Repeat(" ", maxExecutionEnvelopeBytes+1)),
	}
	for name, input := range inputs {
		t.Run(name, func(t *testing.T) {
			if _, err := VerifyExecutionPlan(input, executionExpectation(t, f.payload, input), f.roots); !errors.Is(err, ErrUnqualified) {
				t.Fatalf("malformed envelope accepted: %v", err)
			}
		})
	}
}

func TestExecutionPlanRejectsEverySourceUIDAlias(t *testing.T) {
	for i := 0; i < 6; i++ {
		for j := i + 1; j < 6; j++ {
			t.Run(fmt.Sprintf("%d-%d", i, j), func(t *testing.T) {
				f := newExecutionFixture(t)
				p := &f.payload
				uids := []*string{&p.Cluster.SystemNamespaceUID, &p.Source.NamespaceUID, &p.Source.ReceiptNamespaceUID,
					&p.Source.GenesisUID, &p.Source.ActiveLedgerUID, &p.Source.QueueLedgerUID}
				*uids[j] = *uids[i]
				raw := f.raw(t, executionPlanDomain)
				if _, err := VerifyExecutionPlan(raw, executionExpectation(t, f.payload, raw), f.roots); !errors.Is(err, ErrUnqualified) {
					t.Fatalf("aliased source identities accepted: %v", err)
				}
			})
		}
	}
}

func TestExecutionPlanWorkerIdentityUsesAdmissionContract(t *testing.T) {
	for _, worker := range []string{"kind-fixture/namespace", "Pool._:/@+-123", strings.Repeat("a", 128)} {
		f := newExecutionFixture(t)
		f.payload.Source.WorkerPoolID = worker
		f.payload.Evidence.PhysicalRetirement.OldWorkerPoolID = worker
		if _, err := DigestExecutionPlan(f.payload); err != nil {
			t.Errorf("valid worker identity %q: %v", worker, err)
		}
	}
	for _, worker := range []string{"bad pool", "pool\n", "pool😱", strings.Repeat("a", 129)} {
		f := newExecutionFixture(t)
		f.payload.Source.WorkerPoolID = worker
		f.payload.Evidence.PhysicalRetirement.OldWorkerPoolID = worker
		if _, err := DigestExecutionPlan(f.payload); !errors.Is(err, ErrUnqualified) {
			t.Errorf("invalid worker identity accepted: %v", err)
		}
	}
}

func TestExecutionPlanDecodeDoesNotEchoOrExpandPayload(t *testing.T) {
	f := newExecutionFixture(t)
	// These hashes are intentionally pinned in the fixture, so the pre-decode
	// envelope digest check alone cannot hide a parser-expansion regression.
	// The RawMessage path never turns these arrays into ExecutionTargets.
	for name, payload := range map[string]string{
		"numeric overflow":   `{"limits":{"maxCalls":` + strings.Repeat("9", 256*1024) + `}}`,
		"many empty targets": `{"targets":[` + strings.Repeat(`{},`, 100000) + `{}` + `]}`,
	} {
		t.Run(name, func(t *testing.T) {
			raw := []byte(`{"payload":` + payload + `,"signature":"untrusted"}`)
			_, err := VerifyExecutionPlan(raw, executionExpectation(t, f.payload, raw), f.roots)
			if !errors.Is(err, ErrUnqualified) || err.Error() != ErrUnqualified.Error() {
				t.Fatal("malformed envelope did not return the fixed bounded error")
			}
		})
	}
}

func TestExecutionPlanMaxTargetCountAndIndependentArchiveBudget(t *testing.T) {
	f := newExecutionFixture(t)
	target := *targetByRole(t, &f.payload, "occupancy-attempt")
	f.payload.Targets = make([]ExecutionTarget, maxExecutionObjects)
	for i := range f.payload.Targets {
		target.Name = fmt.Sprintf("occupancy-%04d", i)
		target.UID = fmt.Sprintf("occupancy-uid-%04d", i)
		f.payload.Targets[i] = target
	}
	f.payload.Limits.MaxObjects = maxExecutionObjects
	f.payload.Limits.MaxArchiveObjects = maxExecutionArchiveObjects
	f.payload.Limits.MaxArchiveBytes = maxExecutionArchiveBytes
	f.payload.Evidence.ArchiveObjectCount = maxExecutionObjects
	f.payload.Evidence.ArchiveBytes = int64(maxExecutionObjects) * target.ArchiveBytes
	raw := f.raw(t, executionPlanDomain)
	if len(raw) >= maxExecutionEnvelopeBytes {
		t.Fatalf("inclusive target maximum cannot fit in bounded envelope: %d bytes", len(raw))
	}
	if _, err := VerifyExecutionPlan(raw, executionExpectation(t, f.payload, raw), f.roots); err != nil {
		t.Fatalf("inclusive physical target maximum: %v", err)
	}
	f.payload.Targets = append(f.payload.Targets, target)
	if _, err := DigestExecutionPlan(f.payload); !errors.Is(err, ErrUnqualified) {
		t.Fatalf("target maximum overflow: %v", err)
	}
	f = newExecutionFixture(t)
	f.payload.Limits.MaxObjects = len(f.payload.Targets)
	// Foreign/non-actionable archive records have a separate ceiling.
	if _, err := DigestExecutionPlan(f.payload); err != nil {
		t.Fatalf("independent larger full archive: %v", err)
	}
}
