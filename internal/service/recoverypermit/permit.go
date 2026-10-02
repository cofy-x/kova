// Package recoverypermit verifies independently issued, drain-only recovery
// attestations. It has no signer, Kubernetes writer, capacity-release method,
// or permission to replay uncertain work. Signature verification authenticates
// the issuer and exact binding; it cannot prove physical worker quiescence.
package recoverypermit

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	permitVersion     = "1"
	permitAudience    = "kova-recovery-drain-v1"
	permitAction      = "drain-discard-only"
	workerProofType   = "worker-pool-retired-v1"
	workerAssertion   = "no-old-buildkit-execution-or-network-path-can-resume-v1"
	writersProofType  = "old-writers-stopped-v1"
	writersAssertion  = "ingress-frozen-old-processes-joined-no-uninstrumented-writers-or-post-confirmation-effects-v1"
	maxPermitBytes    = 128 * 1024
	maxReceiptRefs    = 4096
	maxWorkerIDs      = 512
	maxWriterObjects  = 256
	maxServicePods    = 512
	maxOpaqueIDBytes  = 256
	maxWorkerIDBytes  = 128
	maxIncidentIDSize = 128
)

var ErrUnqualified = errors.New("unqualified external drain permit")

var signatureDomain = []byte("KOVA-RECOVERY-DRAIN-PERMIT-V1\x00")

// EpochIdentity is the original, incident-pinned namespace and Genesis
// selection, not a lookup of whatever namespace or ledger exists now.
type EpochIdentity struct {
	Namespace       string `json:"namespace"`
	NamespaceUID    string `json:"namespaceUid"`
	GenesisName     string `json:"genesisName"`
	GenesisUID      string `json:"genesisUid"`
	Generation      string `json:"generation"`
	ActiveLedgerUID string `json:"activeLedgerUid"`
	QueueLedgerUID  string `json:"queueLedgerUid"`
	WorkerPoolID    string `json:"workerPoolId"`
}

func (e EpochIdentity) validate() error {
	if len(validation.IsDNS1123Label(e.Namespace)) != 0 ||
		len(validation.IsDNS1123Subdomain(e.GenesisName)) != 0 ||
		!safeID(e.NamespaceUID, maxOpaqueIDBytes) || !safeID(e.GenesisUID, maxOpaqueIDBytes) ||
		!safeID(e.ActiveLedgerUID, maxOpaqueIDBytes) || !safeID(e.QueueLedgerUID, maxOpaqueIDBytes) ||
		!lowerHex(e.Generation, 32) || !safeID(e.WorkerPoolID, maxWorkerIDBytes) {
		return ErrUnqualified
	}
	return nil
}

// StopIntentRef is an operator-pinned durable stop intent outside this
// process. The verifier checks only its identity/digest binding; a later
// executor must directly qualify the actual immutable object.
type StopIntentRef struct {
	Namespace  string `json:"namespace"`
	Name       string `json:"name"`
	UID        string `json:"uid"`
	DataDigest string `json:"dataDigest"`
}

func (s StopIntentRef) validate() error {
	if len(validation.IsDNS1123Label(s.Namespace)) != 0 ||
		len(validation.IsDNS1123Subdomain(s.Name)) != 0 ||
		!safeID(s.UID, maxOpaqueIDBytes) || !digestSHA256(s.DataDigest) {
		return ErrUnqualified
	}
	return nil
}

// ReceiptRef is one exact immutable pre-effect receipt. The ordered set must
// be enumerated and qualified by a separate, stopped-epoch observer before
// calling VerifyDrainPermit; a signed hash cannot establish completeness by
// itself.
type ReceiptRef struct {
	Kind       string `json:"kind"`
	Namespace  string `json:"namespace"`
	Name       string `json:"name"`
	UID        string `json:"uid"`
	DataDigest string `json:"dataDigest"`
}

func (r ReceiptRef) validate(namespace string) error {
	var prefix string
	switch r.Kind {
	case "queue":
		prefix = "kova-admission-intent-"
	case "grant":
		prefix = "kova-grant-intent-"
	case "pod-create":
		prefix = "kova-pod-create-intent-"
	default:
		return ErrUnqualified
	}
	if r.Namespace != namespace || !strings.HasPrefix(r.Name, prefix) ||
		!lowerHex(strings.TrimPrefix(r.Name, prefix), 32) ||
		!safeID(r.UID, maxOpaqueIDBytes) || !digestSHA256(r.DataDigest) {
		return ErrUnqualified
	}
	return nil
}

func lessReceipt(a, b ReceiptRef) bool {
	if a.Kind != b.Kind {
		return a.Kind < b.Kind
	}
	if a.Namespace != b.Namespace {
		return a.Namespace < b.Namespace
	}
	if a.Name != b.Name {
		return a.Name < b.Name
	}
	// Two UIDs for one kind/name indicate a replaced receipt, not two
	// independent liabilities that may be safely summarized together.
	return false
}

// DigestReceiptSet hashes the complete caller-qualified list in strict
// (kind, namespace, name) order. Missing, duplicate, or re-ordered refs
// fail closed. The digest binds the list supplied; it cannot discover an
// omitted receipt without independent API enumeration.
func DigestReceiptSet(namespace string, refs []ReceiptRef) (string, error) {
	if len(validation.IsDNS1123Label(namespace)) != 0 || len(refs) > maxReceiptRefs {
		return "", ErrUnqualified
	}
	for i, ref := range refs {
		if err := ref.validate(namespace); err != nil || i > 0 && !lessReceipt(refs[i-1], ref) {
			return "", ErrUnqualified
		}
	}
	if refs == nil {
		refs = []ReceiptRef{}
	}
	raw, err := json.Marshal(struct {
		Version  string       `json:"version"`
		Receipts []ReceiptRef `json:"receipts"`
	}{Version: permitVersion, Receipts: refs})
	if err != nil {
		return "", ErrUnqualified
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

// WorkerPoolRetired is a typed, platform-neutral statement issued by an
// external operator. EvidenceDigest pins the environment's detailed proof.
// CutoffAt is for audit only, never a clock-based quiescence inference.
type WorkerPoolRetired struct {
	Type           string   `json:"type"`
	PoolID         string   `json:"poolId"`
	Epoch          string   `json:"epoch"`
	WorkerIDs      []string `json:"workerIds"`
	CapacitySlots  int      `json:"capacitySlots"`
	Disposition    string   `json:"disposition"` // terminated or isolated
	EvidenceDigest string   `json:"evidenceDigest"`
	Assertion      string   `json:"assertion"`
	CutoffAt       string   `json:"cutoffAt"`
}

// KubeObjectRef pins the old route/control object, including its original UID.
type KubeObjectRef struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	UID       string `json:"uid"`
}

func (o KubeObjectRef) validate() error {
	if len(validation.IsDNS1123Label(o.Namespace)) != 0 ||
		len(validation.IsDNS1123Subdomain(o.Name)) != 0 ||
		!safeID(o.UID, maxOpaqueIDBytes) {
		return ErrUnqualified
	}
	return nil
}

func orderedObjects(values []KubeObjectRef, limit int) bool {
	if len(values) == 0 || len(values) > limit {
		return false
	}
	for i, value := range values {
		if value.validate() != nil || i > 0 &&
			(values[i-1].Namespace > value.Namespace ||
				values[i-1].Namespace == value.Namespace && values[i-1].Name >= value.Name) {
			return false
		}
	}
	return true
}

// ServiceProcessRef pins every old Service Pod/process generation that could
// receive a receipt confirmation and issue a later CR or Pod side effect.
type ServiceProcessRef struct {
	Pod         KubeObjectRef `json:"pod"`
	ContainerID string        `json:"containerId"`
	ProcessID   string        `json:"processId"`
}

func orderedProcesses(values []ServiceProcessRef) bool {
	if len(values) == 0 || len(values) > maxServicePods {
		return false
	}
	for i, value := range values {
		if value.Pod.validate() != nil || !safeID(value.ContainerID, maxOpaqueIDBytes) ||
			!safeID(value.ProcessID, maxOpaqueIDBytes) ||
			i > 0 && (values[i-1].Pod.Namespace > value.Pod.Namespace ||
				values[i-1].Pod.Namespace == value.Pod.Namespace && values[i-1].Pod.Name >= value.Pod.Name) {
			return false
		}
	}
	return true
}

// OldWritersStopped is an external, typed assertion about ingress and the
// old Service/API writers. It is required before a direct receipt inventory
// can be treated as complete; a StopIntentRef alone is insufficient. Its
// RetiredAt timestamp is audit data, not a quiescence timeout.
type OldWritersStopped struct {
	Type               string              `json:"type"`
	StopIntent         StopIntentRef       `json:"stopIntent"`
	IngressRoutes      []KubeObjectRef     `json:"ingressRoutes"`
	ControlDeployments []KubeObjectRef     `json:"controlDeployments"`
	ServiceProcesses   []ServiceProcessRef `json:"serviceProcesses"`
	EvidenceDigest     string              `json:"evidenceDigest"`
	Assertion          string              `json:"assertion"`
	RetiredAt          string              `json:"retiredAt"`
}

func (w OldWritersStopped) validate(stop StopIntentRef) error {
	if w.Type != writersProofType || w.StopIntent != stop ||
		!orderedObjects(w.IngressRoutes, maxWriterObjects) ||
		!orderedObjects(w.ControlDeployments, maxWriterObjects) ||
		!orderedProcesses(w.ServiceProcesses) ||
		!digestSHA256(w.EvidenceDigest) || w.Assertion != writersAssertion {
		return ErrUnqualified
	}
	if _, err := time.Parse(time.RFC3339Nano, w.RetiredAt); err != nil {
		return ErrUnqualified
	}
	return nil
}

func (w WorkerPoolRetired) validate(epoch EpochIdentity) error {
	if w.Type != workerProofType || w.PoolID != epoch.WorkerPoolID || w.Epoch != epoch.Generation ||
		len(w.WorkerIDs) == 0 || len(w.WorkerIDs) > maxWorkerIDs ||
		w.CapacitySlots < 1 || w.CapacitySlots > 65535 ||
		(w.Disposition != "terminated" && w.Disposition != "isolated") ||
		!digestSHA256(w.EvidenceDigest) || w.Assertion != workerAssertion {
		return ErrUnqualified
	}
	if _, err := time.Parse(time.RFC3339Nano, w.CutoffAt); err != nil {
		return ErrUnqualified
	}
	for i, id := range w.WorkerIDs {
		if !safeID(id, maxWorkerIDBytes) || i > 0 && w.WorkerIDs[i-1] >= id {
			return ErrUnqualified
		}
	}
	return nil
}

// DrainPayload can authorize only an externally attested discard disposition
// for already bounded old liabilities. It cannot authorize new capacity,
// submission, retry, replay, or deletion of incident tombstones.
type DrainPayload struct {
	Version           string            `json:"version"`
	Audience          string            `json:"audience"`
	Action            string            `json:"action"`
	Issuer            string            `json:"issuer"`
	KeyID             string            `json:"keyId"`
	IncidentID        string            `json:"incidentId"`
	StopIntent        StopIntentRef     `json:"stopIntent"`
	Epoch             EpochIdentity     `json:"epoch"`
	ReceiptCount      int               `json:"receiptCount"`
	ReceiptSetDigest  string            `json:"receiptSetDigest"`
	WorkerRetirement  WorkerPoolRetired `json:"workerRetirement"`
	OldWritersStopped OldWritersStopped `json:"oldWritersStopped"`
	IssuedAt          string            `json:"issuedAt"`
}

type Envelope struct {
	Payload   DrainPayload `json:"payload"`
	Signature string       `json:"signature"` // base64-encoded Ed25519 signature
}

// Expectation must be independently pinned by an operator and by exact API
// qualification of the old-epoch receipt set before the permit is verified.
// Computing PermitDigest from the just-received untrusted envelope defeats
// this pin and is not a supported recovery procedure.
type Expectation struct {
	PermitDigest       string
	IncidentID         string
	StopIntent         StopIntentRef
	Epoch              EpochIdentity
	Receipts           []ReceiptRef
	WorkerIDs          []string
	CapacitySlots      int
	IngressRoutes      []KubeObjectRef
	ControlDeployments []KubeObjectRef
	ServiceProcesses   []ServiceProcessRef
}

// TrustRoots is a configured immutable issuer/key-ID allowlist containing
// public keys only. Kova never stores a corresponding private key.
type TrustRoots map[string]map[string]ed25519.PublicKey

// DrainEvidence is verification output, not permission to start or free work.
type DrainEvidence struct {
	IncidentID       string
	PermitDigest     string
	ReceiptSetDigest string
	ReceiptCount     int
	WorkerPoolID     string
	Disposition      string
}

// VerifyDrainPermit requires canonical JSON, an exact operator-pinned permit
// digest, a complete ordered receipt set, a typed worker retirement statement,
// and an allowed Ed25519 issuer/key. Physical worker retirement and receipt
// completeness still require external, directly observed evidence.
func VerifyDrainPermit(raw []byte, expected Expectation, roots TrustRoots) (DrainEvidence, error) {
	if len(raw) == 0 || len(raw) > maxPermitBytes || !digestSHA256(expected.PermitDigest) ||
		!safeID(expected.IncidentID, maxIncidentIDSize) || expected.StopIntent.validate() != nil ||
		expected.Epoch.validate() != nil || expected.CapacitySlots < 1 || expected.CapacitySlots > 65535 ||
		len(expected.WorkerIDs) == 0 || len(expected.WorkerIDs) > maxWorkerIDs ||
		!orderedObjects(expected.IngressRoutes, maxWriterObjects) ||
		!orderedObjects(expected.ControlDeployments, maxWriterObjects) ||
		!orderedProcesses(expected.ServiceProcesses) {
		return DrainEvidence{}, ErrUnqualified
	}
	var envelope Envelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return DrainEvidence{}, fmt.Errorf("%w: decode: %v", ErrUnqualified, err)
	}
	canonical, err := json.Marshal(envelope)
	if err != nil || !bytes.Equal(raw, canonical) {
		return DrainEvidence{}, fmt.Errorf("%w: noncanonical envelope", ErrUnqualified)
	}
	sum := sha256.Sum256(canonical)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	if digest != expected.PermitDigest {
		return DrainEvidence{}, fmt.Errorf("%w: permit digest mismatch", ErrUnqualified)
	}
	p := envelope.Payload
	if p.Version != permitVersion || p.Audience != permitAudience || p.Action != permitAction ||
		!safeID(p.Issuer, maxOpaqueIDBytes) || !safeID(p.KeyID, maxOpaqueIDBytes) ||
		p.IncidentID != expected.IncidentID || p.StopIntent != expected.StopIntent || p.Epoch != expected.Epoch ||
		p.StopIntent.validate() != nil || p.Epoch.validate() != nil ||
		p.ReceiptCount != len(expected.Receipts) || p.ReceiptCount > maxReceiptRefs ||
		!digestSHA256(p.ReceiptSetDigest) || p.WorkerRetirement.validate(p.Epoch) != nil ||
		p.OldWritersStopped.validate(p.StopIntent) != nil ||
		p.WorkerRetirement.CapacitySlots != expected.CapacitySlots ||
		!reflect.DeepEqual(p.WorkerRetirement.WorkerIDs, expected.WorkerIDs) ||
		!reflect.DeepEqual(p.OldWritersStopped.IngressRoutes, expected.IngressRoutes) ||
		!reflect.DeepEqual(p.OldWritersStopped.ControlDeployments, expected.ControlDeployments) ||
		!reflect.DeepEqual(p.OldWritersStopped.ServiceProcesses, expected.ServiceProcesses) {
		return DrainEvidence{}, ErrUnqualified
	}
	issuedAt, err := time.Parse(time.RFC3339Nano, p.IssuedAt)
	if err != nil {
		return DrainEvidence{}, ErrUnqualified
	}
	cutoffAt, _ := time.Parse(time.RFC3339Nano, p.WorkerRetirement.CutoffAt)
	writersAt, _ := time.Parse(time.RFC3339Nano, p.OldWritersStopped.RetiredAt)
	if issuedAt.Before(cutoffAt) || issuedAt.Before(writersAt) { // audit ordering, never quiescence proof.
		return DrainEvidence{}, ErrUnqualified
	}
	for i, id := range expected.WorkerIDs {
		if !safeID(id, maxWorkerIDBytes) || i > 0 && expected.WorkerIDs[i-1] >= id {
			return DrainEvidence{}, ErrUnqualified
		}
	}
	setDigest, err := DigestReceiptSet(expected.Epoch.Namespace, expected.Receipts)
	if err != nil || setDigest != p.ReceiptSetDigest {
		return DrainEvidence{}, ErrUnqualified
	}
	publicKeys := roots[p.Issuer]
	publicKey := publicKeys[p.KeyID]
	if len(publicKey) != ed25519.PublicKeySize {
		return DrainEvidence{}, ErrUnqualified
	}
	signature, err := base64.StdEncoding.DecodeString(envelope.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize ||
		base64.StdEncoding.EncodeToString(signature) != envelope.Signature {
		return DrainEvidence{}, ErrUnqualified
	}
	payloadRaw, err := json.Marshal(p)
	if err != nil {
		return DrainEvidence{}, ErrUnqualified
	}
	message := make([]byte, 0, len(signatureDomain)+len(payloadRaw))
	message = append(message, signatureDomain...)
	message = append(message, payloadRaw...)
	if !ed25519.Verify(publicKey, message, signature) {
		return DrainEvidence{}, ErrUnqualified
	}
	return DrainEvidence{
		IncidentID: p.IncidentID, PermitDigest: digest,
		ReceiptSetDigest: setDigest, ReceiptCount: p.ReceiptCount,
		WorkerPoolID: p.WorkerRetirement.PoolID, Disposition: p.WorkerRetirement.Disposition,
	}, nil
}

func digestSHA256(value string) bool {
	return strings.HasPrefix(value, "sha256:") && lowerHex(strings.TrimPrefix(value, "sha256:"), 64)
}

func lowerHex(value string, size int) bool {
	if len(value) != size {
		return false
	}
	for _, c := range []byte(value) {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func safeID(value string, max int) bool {
	if value == "" || len(value) > max || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return false
		}
	}
	return true
}
