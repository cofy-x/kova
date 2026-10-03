package recoverypermit

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func fixturePermit(t *testing.T) (DrainPayload, Expectation, TrustRoots, ed25519.PrivateKey) {
	t.Helper()
	private := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{7}, ed25519.SeedSize))
	public := private.Public().(ed25519.PublicKey)
	epoch := EpochIdentity{
		Namespace: "runner-old", NamespaceUID: "namespace-original-uid",
		ReceiptNamespace: "receipts-old", ReceiptNamespaceUID: "receipts-original-uid",
		GenesisName: "kova-service-admission-genesis", GenesisUID: "genesis-original-uid",
		Generation: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", ActiveLedgerUID: "active-original-uid",
		QueueLedgerUID: "queue-original-uid", WorkerPoolID: "buildkit-old-pool",
	}
	stop := StopIntentRef{Namespace: "runner-old", Name: "stop-incident-one", UID: "stop-original-uid", DataDigest: "sha256:" + strings.Repeat("c", 64)}
	receipts := []ReceiptRef{
		{Kind: "grant", Namespace: "receipts-old", Name: "kova-grant-intent-" + strings.Repeat("1", 32), UID: "grant-original-uid", DataDigest: "sha256:" + strings.Repeat("d", 64)},
		{Kind: "pod-create", Namespace: "receipts-old", Name: "kova-pod-create-intent-" + strings.Repeat("2", 32), UID: "pod-intent-original-uid", DataDigest: "sha256:" + strings.Repeat("e", 64)},
		{Kind: "queue", Namespace: "receipts-old", Name: "kova-admission-intent-" + strings.Repeat("3", 32), UID: "queue-original-uid", DataDigest: "sha256:" + strings.Repeat("f", 64)},
	}
	setDigest, err := DigestReceiptSet(epoch, receipts)
	if err != nil {
		t.Fatal(err)
	}
	payload := DrainPayload{
		Version: permitVersion, Audience: permitAudience, Action: permitAction,
		Issuer: "environment-operator", KeyID: "key-one", IncidentID: "incident-one",
		StopIntent: stop, Epoch: epoch, ReceiptCount: len(receipts), ReceiptSetDigest: setDigest,
		WorkerRetirement: WorkerPoolRetired{
			Type: workerProofType, PoolID: epoch.WorkerPoolID, Epoch: epoch.Generation,
			WorkerIDs: []string{"worker-a", "worker-b"}, CapacitySlots: 16,
			Disposition: "terminated", EvidenceDigest: "sha256:" + strings.Repeat("b", 64),
			Assertion: workerAssertion, CutoffAt: "2020-01-01T00:00:00Z",
		},
		OldWritersStopped: OldWritersStopped{
			Type: writersProofType, StopIntent: stop,
			IngressRoutes:      []KubeObjectRef{{Namespace: "control-old", Name: "kova-old-route", UID: "route-original-uid"}},
			ControlDeployments: []KubeObjectRef{{Namespace: "control-old", Name: "kova-service", UID: "deployment-original-uid"}},
			ServiceProcesses: []ServiceProcessRef{{
				Pod:         KubeObjectRef{Namespace: "control-old", Name: "kova-service-old", UID: "service-pod-original-uid"},
				ContainerID: "containerd://old-container", ProcessID: "old-process-generation",
			}},
			EvidenceDigest: "sha256:" + strings.Repeat("a", 64), Assertion: writersAssertion,
			RetiredAt: "2020-01-01T00:00:00Z",
		},
		IssuedAt: "2020-01-01T00:00:01Z",
	}
	expected := Expectation{
		IncidentID: payload.IncidentID, StopIntent: stop, Epoch: epoch,
		Receipts: receipts, WorkerIDs: []string{"worker-a", "worker-b"}, CapacitySlots: 16,
		IngressRoutes:      append([]KubeObjectRef(nil), payload.OldWritersStopped.IngressRoutes...),
		ControlDeployments: append([]KubeObjectRef(nil), payload.OldWritersStopped.ControlDeployments...),
		ServiceProcesses:   append([]ServiceProcessRef(nil), payload.OldWritersStopped.ServiceProcesses...),
	}
	return payload, expected, TrustRoots{"environment-operator": {"key-one": public}}, private
}

func signedEnvelope(t *testing.T, payload DrainPayload, key ed25519.PrivateKey) ([]byte, string) {
	t.Helper()
	rawPayload, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	message := append(append([]byte{}, signatureDomain...), rawPayload...)
	sig := ed25519.Sign(key, message)
	raw, err := json.Marshal(Envelope{Payload: payload, Signature: base64.StdEncoding.EncodeToString(sig)})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	return raw, "sha256:" + hex.EncodeToString(sum[:])
}

func TestVerifyDrainPermitExactSignedBindings(t *testing.T) {
	payload, expected, roots, key := fixturePermit(t)
	raw, digest := signedEnvelope(t, payload, key)
	expected.PermitDigest = digest
	evidence, err := VerifyDrainPermit(raw, expected, roots)
	if err != nil {
		t.Fatal(err)
	}
	if evidence.IncidentID != expected.IncidentID || evidence.PermitDigest != digest ||
		evidence.ReceiptSetDigest != payload.ReceiptSetDigest || evidence.ReceiptCount != len(expected.Receipts) ||
		evidence.WorkerPoolID != expected.Epoch.WorkerPoolID || evidence.Disposition != "terminated" {
		t.Fatalf("wrong drain evidence: %#v", evidence)
	}
	// A historical cutoff is valid as signed audit data; wall time alone is
	// neither a timeout nor a physical retirement proof.
}

func TestDrainPermitDecodeErrorDoesNotEchoEnvelope(t *testing.T) {
	_, expected, roots, _ := fixturePermit(t)
	expected.PermitDigest = "sha256:" + strings.Repeat("a", 64)
	marker := strings.Repeat("123456789", 512)
	raw := []byte(`{"payload":{"receiptCount":` + marker + `}}`)
	_, err := VerifyDrainPermit(raw, expected, roots)
	if !errors.Is(err, ErrUnqualified) || len(err.Error()) > 160 || strings.Contains(err.Error(), "123456789") {
		t.Fatal("invalid permit body escaped through decoder error")
	}
}

func TestVerifyDrainPermitRejectsChangedPayloadEvenWhenResigned(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*DrainPayload)
	}{
		{name: "new-capacity action", mutate: func(p *DrainPayload) { p.Action = "admit-new-capacity" }},
		{name: "wrong incident", mutate: func(p *DrainPayload) { p.IncidentID = "incident-two" }},
		{name: "replacement namespace UID", mutate: func(p *DrainPayload) { p.Epoch.NamespaceUID = "replacement-uid" }},
		{name: "replacement Genesis UID", mutate: func(p *DrainPayload) { p.Epoch.GenesisUID = "replacement-uid" }},
		{name: "replacement ledger UID", mutate: func(p *DrainPayload) { p.Epoch.ActiveLedgerUID = "replacement-uid" }},
		{name: "different stop intent", mutate: func(p *DrainPayload) { p.StopIntent.UID = "replacement-uid" }},
		{name: "wrong receipt count", mutate: func(p *DrainPayload) { p.ReceiptCount-- }},
		{name: "wrong receipt digest", mutate: func(p *DrainPayload) { p.ReceiptSetDigest = "sha256:" + strings.Repeat("0", 64) }},
		{name: "opaque worker proof", mutate: func(p *DrainPayload) { p.WorkerRetirement.Type = "opaque-statement" }},
		{name: "different pool", mutate: func(p *DrainPayload) { p.WorkerRetirement.PoolID = "other-pool" }},
		{name: "different worker", mutate: func(p *DrainPayload) { p.WorkerRetirement.WorkerIDs[1] = "worker-c" }},
		{name: "different capacity", mutate: func(p *DrainPayload) { p.WorkerRetirement.CapacitySlots = 17 }},
		{name: "no isolation assertion", mutate: func(p *DrainPayload) { p.WorkerRetirement.Assertion = "unknown" }},
		{name: "missing evidence digest", mutate: func(p *DrainPayload) { p.WorkerRetirement.EvidenceDigest = "" }},
		{name: "wrong worker epoch", mutate: func(p *DrainPayload) { p.WorkerRetirement.Epoch = strings.Repeat("f", 32) }},
		{name: "unsupported disposition", mutate: func(p *DrainPayload) { p.WorkerRetirement.Disposition = "paused" }},
		{name: "missing writer assertion", mutate: func(p *DrainPayload) { p.OldWritersStopped.Assertion = "unknown" }},
		{name: "replacement ingress route", mutate: func(p *DrainPayload) { p.OldWritersStopped.IngressRoutes[0].UID = "replacement-uid" }},
		{name: "replacement control deployment", mutate: func(p *DrainPayload) { p.OldWritersStopped.ControlDeployments[0].UID = "replacement-uid" }},
		{name: "omitted service process", mutate: func(p *DrainPayload) { p.OldWritersStopped.ServiceProcesses = nil }},
		{name: "wrong writer evidence", mutate: func(p *DrainPayload) { p.OldWritersStopped.EvidenceDigest = "" }},
		{name: "wrong writer stop intent", mutate: func(p *DrainPayload) { p.OldWritersStopped.StopIntent.UID = "replacement-uid" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload, expected, roots, key := fixturePermit(t)
			tc.mutate(&payload)
			raw, digest := signedEnvelope(t, payload, key)
			expected.PermitDigest = digest // even a freshly pinned valid signature cannot change the contract.
			if _, err := VerifyDrainPermit(raw, expected, roots); !errors.Is(err, ErrUnqualified) {
				t.Fatalf("changed signed payload qualified: %v", err)
			}
		})
	}
}

func TestVerifyDrainPermitRejectsWrongKeySignatureAndDigest(t *testing.T) {
	payload, expected, roots, key := fixturePermit(t)
	raw, digest := signedEnvelope(t, payload, key)
	expected.PermitDigest = digest
	if _, err := VerifyDrainPermit(raw, expected, roots); err != nil {
		t.Fatal(err)
	}
	badExpected := expected
	badExpected.PermitDigest = "sha256:" + strings.Repeat("0", 64)
	if _, err := VerifyDrainPermit(raw, badExpected, roots); !errors.Is(err, ErrUnqualified) {
		t.Fatalf("unpinned permit qualified: %v", err)
	}
	if _, err := VerifyDrainPermit(raw, expected, TrustRoots{}); !errors.Is(err, ErrUnqualified) {
		t.Fatalf("unallowed issuer qualified: %v", err)
	}
	otherKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{8}, ed25519.SeedSize))
	roots["environment-operator"]["key-one"] = otherKey.Public().(ed25519.PublicKey)
	if _, err := VerifyDrainPermit(raw, expected, roots); !errors.Is(err, ErrUnqualified) {
		t.Fatalf("wrong public key qualified: %v", err)
	}
	roots["environment-operator"]["key-one"] = key.Public().(ed25519.PublicKey)
	var envelope Envelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	sig, _ := base64.StdEncoding.DecodeString(envelope.Signature)
	sig[0] ^= 1
	envelope.Signature = base64.StdEncoding.EncodeToString(sig)
	tampered, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(tampered)
	expected.PermitDigest = "sha256:" + hex.EncodeToString(sum[:])
	if _, err := VerifyDrainPermit(tampered, expected, roots); !errors.Is(err, ErrUnqualified) {
		t.Fatalf("bad signature qualified despite exact envelope pin: %v", err)
	}
}

func TestVerifyDrainPermitRejectsIncompleteOrUnorderedReceiptSet(t *testing.T) {
	payload, expected, roots, key := fixturePermit(t)
	raw, digest := signedEnvelope(t, payload, key)
	expected.PermitDigest = digest
	expected.Receipts = expected.Receipts[:2]
	if _, err := VerifyDrainPermit(raw, expected, roots); !errors.Is(err, ErrUnqualified) {
		t.Fatalf("omitted receipt qualified: %v", err)
	}
	_, expected, roots, key = fixturePermit(t)
	expected.Receipts[0], expected.Receipts[1] = expected.Receipts[1], expected.Receipts[0]
	if _, err := DigestReceiptSet(expected.Epoch, expected.Receipts); !errors.Is(err, ErrUnqualified) {
		t.Fatalf("unordered receipt set hashed: %v", err)
	}
	expected.Receipts[0] = expected.Receipts[1]
	if _, err := DigestReceiptSet(expected.Epoch, expected.Receipts); !errors.Is(err, ErrUnqualified) {
		t.Fatalf("duplicate receipt set hashed: %v", err)
	}
	expected.Receipts = []ReceiptRef{expected.Receipts[0], expected.Receipts[0]}
	expected.Receipts[1].UID = "replacement-uid"
	if _, err := DigestReceiptSet(expected.Epoch, expected.Receipts); !errors.Is(err, ErrUnqualified) {
		t.Fatalf("same-name replacement set hashed: %v", err)
	}
}

func TestVerifyDrainPermitRequiresCanonicalBoundedEncoding(t *testing.T) {
	payload, expected, roots, key := fixturePermit(t)
	raw, _ := signedEnvelope(t, payload, key)
	spaced := append([]byte("\n"), raw...)
	sum := sha256.Sum256(spaced)
	expected.PermitDigest = "sha256:" + hex.EncodeToString(sum[:])
	if _, err := VerifyDrainPermit(spaced, expected, roots); !errors.Is(err, ErrUnqualified) {
		t.Fatalf("noncanonical envelope qualified: %v", err)
	}
	tooLarge := bytes.Repeat([]byte{'x'}, maxPermitBytes+1)
	expected.PermitDigest = "sha256:" + strings.Repeat("0", 64)
	if _, err := VerifyDrainPermit(tooLarge, expected, roots); !errors.Is(err, ErrUnqualified) {
		t.Fatalf("oversized envelope qualified: %v", err)
	}
}
