// Package recoverydisposal verifies externally issued, bounded old-epoch
// disposal and closure records. It deliberately has no signer, Kubernetes
// client, deletion method, route writer, or capacity-release method. A valid
// signature binds evidence; it cannot itself prove physical retirement,
// receipt-set completeness, or that a future identity fence will be honored.
package recoverydisposal

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

	"github.com/cofy-x/kova/internal/service/recoverypermit"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	version               = "1"
	authorizationAudience = "kova-recovery-disposal-authorization-v1"
	authorizationAction   = "dispose-old-epoch-liabilities-only"
	closureAudience       = "kova-recovery-disposal-closure-v1"
	closureAction         = "certify-one-successor-after-old-uid-retirement-only"
	retirementAssertion   = "old-service-runner-workers-network-and-inflight-effects-cannot-resume-v1"
	namespaceAssertion    = "old-runner-and-receipt-namespace-uids-absent-with-persistent-no-reuse-fences-v1"
	maxEnvelopeBytes      = 2 * 1024 * 1024
	maxReceipts           = 2048
	maxQueueReceipts      = 1000
	maxGrantReceipts      = 128
	maxPodReceipts        = 256
	maxDispositions       = 2048
	maxIDBytes            = 256
	maxIncidentIDBytes    = 128
	maxCapacitySlots      = 65535
)

var (
	ErrUnqualified      = errors.New("unqualified external disposal record")
	authorizationDomain = []byte("KOVA-RECOVERY-DISPOSAL-AUTHORIZATION-V1\x00")
	closureDomain       = []byte("KOVA-RECOVERY-DISPOSAL-CLOSURE-V1\x00")
)

// TrustRoots contains configured public keys only, keyed by issuer and key ID.
// No private signing key belongs in Kova.
type TrustRoots map[string]map[string]ed25519.PublicKey

// ReceiptLink refers to an index in AuthorizationPayload.Receipts. Every
// receipt must be assigned to exactly one build disposition, and each build
// may have at most one queue, grant, and pod-create receipt.
type ReceiptLink struct {
	Index int `json:"index"`
}

// BuildDisposition is one explicitly decided old-epoch liability. A terminal
// result must pin both the KovaBuild and Pod UIDs and an external terminal
// record. If either effect was absent or uncertain, only an independently
// approved Unknown/discard outcome qualifies. Neither outcome permits replay.
type BuildDisposition struct {
	BuildName          string        `json:"buildName"`
	BuildUID           string        `json:"buildUid"`
	PodUID             string        `json:"podUid"`
	ReceiptLinks       []ReceiptLink `json:"receiptLinks"`
	Outcome            string        `json:"outcome"` // terminal-result or approved-unknown-discard
	ResultRecordDigest string        `json:"resultRecordDigest"`
	ApprovalDigest     string        `json:"approvalDigest"`
	NeverReplay        bool          `json:"neverReplay"`
}

// PhysicalRetirement binds externally observed retirement of all old side-
// effect paths. Evidence digests must name durable records that a separate
// observer can inspect. RetiredAt is audit ordering, never a timeout proof.
type PhysicalRetirement struct {
	OldWorkerPoolID        string `json:"oldWorkerPoolId"`
	OldCapacitySlots       int    `json:"oldCapacitySlots"`
	ServiceProcessesDigest string `json:"serviceProcessesDigest"`
	RunnerProcessesDigest  string `json:"runnerProcessesDigest"`
	WorkerPoolDigest       string `json:"workerPoolDigest"`
	NetworkIsolationDigest string `json:"networkIsolationDigest"`
	InflightEffectsDigest  string `json:"inflightEffectsDigest"`
	Assertion              string `json:"assertion"`
	RetiredAt              string `json:"retiredAt"`
}

// AuthorizationPayload is issued before disposal. It binds the complete
// directly qualified receipt inventory and one disposition per covered build.
// ReceiptSetDigest uses recoverypermit.DigestReceiptSet. The stop, drain, and
// occupancy digests are incident-pinned inputs, not facts inferred here.
type AuthorizationPayload struct {
	Version                string                       `json:"version"`
	Audience               string                       `json:"audience"`
	Action                 string                       `json:"action"`
	Issuer                 string                       `json:"issuer"`
	KeyID                  string                       `json:"keyId"`
	IncidentID             string                       `json:"incidentId"`
	Source                 recoverypermit.EpochIdentity `json:"source"`
	StopIntentDigest       string                       `json:"stopIntentDigest"`
	DrainPermitDigest      string                       `json:"drainPermitDigest"`
	OccupancyReportDigest  string                       `json:"occupancyReportDigest"`
	ReceiptInventoryDigest string                       `json:"receiptInventoryDigest"`
	ReceiptSetDigest       string                       `json:"receiptSetDigest"`
	Receipts               []recoverypermit.ReceiptRef  `json:"receipts"`
	Dispositions           []BuildDisposition           `json:"dispositions"`
	PhysicalRetirement     PhysicalRetirement           `json:"physicalRetirement"`
	SuccessorPlanDigest    string                       `json:"successorPlanDigest"`
	IssuedAt               string                       `json:"issuedAt"`
}

type AuthorizationEnvelope struct {
	Payload   AuthorizationPayload `json:"payload"`
	Signature string               `json:"signature"`
}

// AuthorizationExpectation is independently pinned by the operator and the
// stopped-epoch observer. Deriving EnvelopeDigest, receipt refs, dispositions,
// or evidence digests from the just-received envelope defeats this boundary.
type AuthorizationExpectation struct {
	EnvelopeDigest         string
	Issuer                 string
	KeyID                  string
	IncidentID             string
	Source                 recoverypermit.EpochIdentity
	StopIntentDigest       string
	DrainPermitDigest      string
	OccupancyReportDigest  string
	ReceiptInventoryDigest string
	Receipts               []recoverypermit.ReceiptRef
	Dispositions           []BuildDisposition
	PhysicalRetirement     PhysicalRetirement
	SuccessorPlanDigest    string
}

// VerifiedAuthorization is proof of successful schema/signature verification,
// not a Kubernetes mutation permit. The unexported seal prevents callers from
// constructing a valid value without VerifyAuthorization.
type VerifiedAuthorization struct {
	incidentID          string
	envelopeDigest      string
	receiptSetDigest    string
	receiptCount        int
	source              recoverypermit.EpochIdentity
	successorPlanDigest string
	issuedAt            string
	valid               bool
}

func (v VerifiedAuthorization) IncidentID() string                   { return v.incidentID }
func (v VerifiedAuthorization) EnvelopeDigest() string               { return v.envelopeDigest }
func (v VerifiedAuthorization) ReceiptSetDigest() string             { return v.receiptSetDigest }
func (v VerifiedAuthorization) ReceiptCount() int                    { return v.receiptCount }
func (v VerifiedAuthorization) Source() recoverypermit.EpochIdentity { return v.source }
func (v VerifiedAuthorization) SuccessorPlanDigest() string          { return v.successorPlanDigest }

// VerifyAuthorization rejects unknown, incomplete, oversized, noncanonical,
// unsigned, and unpinned records. It never attempts to discover old receipts
// or verify the physical contents of an external evidence record.
func VerifyAuthorization(raw []byte, expected AuthorizationExpectation, roots TrustRoots) (VerifiedAuthorization, error) {
	if !envelopeSizeOK(raw) || !digestSHA256(expected.EnvelopeDigest) ||
		!safeID(expected.Issuer, maxIDBytes) || !safeID(expected.KeyID, maxIDBytes) ||
		!safeID(expected.IncidentID, maxIncidentIDBytes) || !validEpoch(expected.Source) ||
		!digestSHA256(expected.StopIntentDigest) || !digestSHA256(expected.DrainPermitDigest) ||
		!digestSHA256(expected.OccupancyReportDigest) || !digestSHA256(expected.ReceiptInventoryDigest) ||
		!digestSHA256(expected.SuccessorPlanDigest) ||
		!validPhysicalRetirement(expected.PhysicalRetirement, expected.Source) {
		return VerifiedAuthorization{}, ErrUnqualified
	}
	var envelope AuthorizationEnvelope
	if err := decodeCanonical(raw, &envelope); err != nil {
		return VerifiedAuthorization{}, err
	}
	digest := digestBytes(raw)
	if digest != expected.EnvelopeDigest {
		return VerifiedAuthorization{}, ErrUnqualified
	}
	p := envelope.Payload
	if p.Version != version || p.Audience != authorizationAudience || p.Action != authorizationAction ||
		p.Issuer != expected.Issuer || p.KeyID != expected.KeyID ||
		p.IncidentID != expected.IncidentID || p.Source != expected.Source ||
		p.StopIntentDigest != expected.StopIntentDigest || p.DrainPermitDigest != expected.DrainPermitDigest ||
		p.OccupancyReportDigest != expected.OccupancyReportDigest ||
		p.ReceiptInventoryDigest != expected.ReceiptInventoryDigest ||
		p.SuccessorPlanDigest != expected.SuccessorPlanDigest ||
		!reflect.DeepEqual(p.Receipts, expected.Receipts) ||
		!reflect.DeepEqual(p.Dispositions, expected.Dispositions) ||
		p.PhysicalRetirement != expected.PhysicalRetirement {
		return VerifiedAuthorization{}, ErrUnqualified
	}
	setDigest, err := validateInventory(p.Source, p.Receipts)
	if err != nil || p.ReceiptSetDigest != setDigest ||
		validateDispositions(p.Receipts, p.Dispositions) != nil {
		return VerifiedAuthorization{}, ErrUnqualified
	}
	issuedAt, err := time.Parse(time.RFC3339Nano, p.IssuedAt)
	if err != nil {
		return VerifiedAuthorization{}, ErrUnqualified
	}
	retiredAt, _ := time.Parse(time.RFC3339Nano, p.PhysicalRetirement.RetiredAt)
	if issuedAt.Before(retiredAt) || !verifySignature(p, envelope.Signature, authorizationDomain, p.Issuer, p.KeyID, roots) {
		return VerifiedAuthorization{}, ErrUnqualified
	}
	return VerifiedAuthorization{
		incidentID: p.IncidentID, envelopeDigest: digest, receiptSetDigest: setDigest,
		receiptCount: len(p.Receipts), source: p.Source, successorPlanDigest: p.SuccessorPlanDigest,
		issuedAt: p.IssuedAt, valid: true,
	}, nil
}

// NamespaceRetirement records a normal deletion of one exact old UID. A
// durable identity fence is required because an absence observation alone
// says nothing about future accidental reuse of the same namespace name.
type NamespaceRetirement struct {
	Name                     string `json:"name"`
	OldUID                   string `json:"oldUid"`
	UIDAbsenceEvidenceDigest string `json:"uidAbsenceEvidenceDigest"`
	NoReuseFenceDigest       string `json:"noReuseFenceDigest"`
	NormalDeletionDigest     string `json:"normalDeletionDigest"`
}

// SuccessorIdentity is a single, exact replacement epoch and its capacity
// allocation. RouteCASDigest must attest a resourceVersion compare-and-swap
// from the old runner UID to this successor UID. The route object's own UID
// may remain unchanged during that update.
type SuccessorIdentity struct {
	RunnerNamespace               string `json:"runnerNamespace"`
	RunnerNamespaceUID            string `json:"runnerNamespaceUid"`
	ReceiptNamespace              string `json:"receiptNamespace"`
	ReceiptNamespaceUID           string `json:"receiptNamespaceUid"`
	GenesisName                   string `json:"genesisName"`
	GenesisUID                    string `json:"genesisUid"`
	Generation                    string `json:"generation"`
	WorkerPoolID                  string `json:"workerPoolId"`
	CapacitySlots                 int    `json:"capacitySlots"`
	CapacityProofDigest           string `json:"capacityProofDigest"`
	RouteNamespace                string `json:"routeNamespace"`
	RouteName                     string `json:"routeName"`
	RouteUID                      string `json:"routeUid"`
	RouteExpectedResourceVersion  string `json:"routeExpectedResourceVersion"`
	RouteCommittedResourceVersion string `json:"routeCommittedResourceVersion"`
	RouteFromNamespaceUID         string `json:"routeFromNamespaceUid"`
	RouteToNamespaceUID           string `json:"routeToNamespaceUid"`
	RouteCASDigest                string `json:"routeCasDigest"`
}

// ClosurePayload is a second, differently scoped external statement issued
// after both old Namespace UIDs have retired. It binds one exact successor and
// a persistent, single-assignment external fence keyed by the authorization
// digest. A local verifier cannot prove global uniqueness from one envelope;
// the fence record and route CAS must be checked directly. This is not itself
// authorization to create the successor or release capacity.
type ClosurePayload struct {
	Version                    string                       `json:"version"`
	Audience                   string                       `json:"audience"`
	Action                     string                       `json:"action"`
	Issuer                     string                       `json:"issuer"`
	KeyID                      string                       `json:"keyId"`
	IncidentID                 string                       `json:"incidentId"`
	AuthorizationDigest        string                       `json:"authorizationDigest"`
	Source                     recoverypermit.EpochIdentity `json:"source"`
	RunnerRetirement           NamespaceRetirement          `json:"runnerRetirement"`
	ReceiptRetirement          NamespaceRetirement          `json:"receiptRetirement"`
	NamespaceAssertion         string                       `json:"namespaceAssertion"`
	SuccessorPlanDigest        string                       `json:"successorPlanDigest"`
	SingleSuccessorFenceDigest string                       `json:"singleSuccessorFenceDigest"`
	Successor                  SuccessorIdentity            `json:"successor"`
	ObservedAt                 string                       `json:"observedAt"`
	IssuedAt                   string                       `json:"issuedAt"`
}

type ClosureEnvelope struct {
	Payload   ClosurePayload `json:"payload"`
	Signature string         `json:"signature"`
}

// ClosureExpectation must pin the exact post-termination evidence and
// successor from independently qualified records, not from the envelope.
type ClosureExpectation struct {
	EnvelopeDigest             string
	Issuer                     string
	KeyID                      string
	IncidentID                 string
	AuthorizationDigest        string
	Source                     recoverypermit.EpochIdentity
	RunnerRetirement           NamespaceRetirement
	ReceiptRetirement          NamespaceRetirement
	SuccessorPlanDigest        string
	SingleSuccessorFenceDigest string
	Successor                  SuccessorIdentity
}

type VerifiedClosure struct {
	IncidentID          string
	EnvelopeDigest      string
	AuthorizationDigest string
	Successor           SuccessorIdentity
}

// VerifyClosure requires a previously verified authorization and an exact,
// independently pinned closure envelope. It validates shape and signatures;
// a separate external observer must verify namespace absence, no-reuse fences,
// normal deletion, capacity allocation, and the route CAS proof.
func VerifyClosure(raw []byte, prior VerifiedAuthorization, expected ClosureExpectation, roots TrustRoots) (VerifiedClosure, error) {
	if !prior.valid || !envelopeSizeOK(raw) || !digestSHA256(expected.EnvelopeDigest) ||
		!safeID(expected.Issuer, maxIDBytes) || !safeID(expected.KeyID, maxIDBytes) ||
		!digestSHA256(expected.AuthorizationDigest) || !digestSHA256(expected.SuccessorPlanDigest) ||
		expected.AuthorizationDigest != prior.envelopeDigest || expected.IncidentID != prior.incidentID ||
		expected.Source != prior.source || expected.SuccessorPlanDigest != prior.successorPlanDigest ||
		!validNamespaceRetirement(expected.RunnerRetirement, prior.source.Namespace, prior.source.NamespaceUID) ||
		!validNamespaceRetirement(expected.ReceiptRetirement, prior.source.ReceiptNamespace, prior.source.ReceiptNamespaceUID) ||
		!digestSHA256(expected.SingleSuccessorFenceDigest) ||
		!validSuccessor(expected.Successor, prior.source) {
		return VerifiedClosure{}, ErrUnqualified
	}
	var envelope ClosureEnvelope
	if err := decodeCanonical(raw, &envelope); err != nil {
		return VerifiedClosure{}, err
	}
	digest := digestBytes(raw)
	p := envelope.Payload
	if digest != expected.EnvelopeDigest || p.Version != version || p.Audience != closureAudience ||
		p.Action != closureAction || p.Issuer != expected.Issuer || p.KeyID != expected.KeyID ||
		p.IncidentID != expected.IncidentID || p.AuthorizationDigest != expected.AuthorizationDigest ||
		p.Source != expected.Source || p.RunnerRetirement != expected.RunnerRetirement ||
		p.ReceiptRetirement != expected.ReceiptRetirement || p.NamespaceAssertion != namespaceAssertion ||
		p.SuccessorPlanDigest != expected.SuccessorPlanDigest ||
		p.SingleSuccessorFenceDigest != expected.SingleSuccessorFenceDigest || p.Successor != expected.Successor {
		return VerifiedClosure{}, ErrUnqualified
	}
	observedAt, err := time.Parse(time.RFC3339Nano, p.ObservedAt)
	if err != nil {
		return VerifiedClosure{}, ErrUnqualified
	}
	issuedAt, err := time.Parse(time.RFC3339Nano, p.IssuedAt)
	if err != nil {
		return VerifiedClosure{}, ErrUnqualified
	}
	priorAt, err := time.Parse(time.RFC3339Nano, prior.issuedAt)
	if err != nil || observedAt.Before(priorAt) || issuedAt.Before(observedAt) ||
		!verifySignature(p, envelope.Signature, closureDomain, p.Issuer, p.KeyID, roots) {
		return VerifiedClosure{}, ErrUnqualified
	}
	return VerifiedClosure{IncidentID: p.IncidentID, EnvelopeDigest: digest,
		AuthorizationDigest: p.AuthorizationDigest, Successor: p.Successor}, nil
}

func validateInventory(epoch recoverypermit.EpochIdentity, refs []recoverypermit.ReceiptRef) (string, error) {
	if refs == nil || len(refs) == 0 || len(refs) > maxReceipts {
		return "", ErrUnqualified
	}
	var queues, grants, pods int
	for _, ref := range refs {
		switch ref.Kind {
		case "queue":
			queues++
		case "grant":
			grants++
		case "pod-create":
			pods++
		default:
			return "", ErrUnqualified
		}
	}
	if queues > maxQueueReceipts || grants > maxGrantReceipts || pods > maxPodReceipts {
		return "", ErrUnqualified
	}
	return recoverypermit.DigestReceiptSet(epoch, refs)
}

func validateDispositions(refs []recoverypermit.ReceiptRef, dispositions []BuildDisposition) error {
	if len(dispositions) == 0 || len(dispositions) > maxDispositions || len(dispositions) > len(refs) {
		return ErrUnqualified
	}
	used := make([]bool, len(refs))
	buildUIDs := make(map[string]bool, len(dispositions))
	podUIDs := make(map[string]bool, len(dispositions))
	for i, d := range dispositions {
		if len(validation.IsDNS1123Subdomain(d.BuildName)) != 0 ||
			i > 0 && dispositions[i-1].BuildName >= d.BuildName || !d.NeverReplay ||
			len(d.ReceiptLinks) == 0 || len(d.ReceiptLinks) > 3 ||
			(d.BuildUID != "" && !safeID(d.BuildUID, maxIDBytes)) ||
			(d.PodUID != "" && !safeID(d.PodUID, maxIDBytes)) ||
			(d.PodUID != "" && d.BuildUID == "") ||
			(d.BuildUID != "" && buildUIDs[d.BuildUID]) ||
			(d.PodUID != "" && podUIDs[d.PodUID]) {
			return ErrUnqualified
		}
		if d.BuildUID != "" {
			buildUIDs[d.BuildUID] = true
		}
		if d.PodUID != "" {
			podUIDs[d.PodUID] = true
		}
		switch d.Outcome {
		case "terminal-result":
			if d.BuildUID == "" || d.PodUID == "" || !digestSHA256(d.ResultRecordDigest) || d.ApprovalDigest != "" {
				return ErrUnqualified
			}
		case "approved-unknown-discard":
			if !digestSHA256(d.ApprovalDigest) || d.ResultRecordDigest != "" {
				return ErrUnqualified
			}
		default:
			return ErrUnqualified
		}
		kinds := map[string]bool{}
		previous := -1
		for _, link := range d.ReceiptLinks {
			if link.Index <= previous || link.Index < 0 || link.Index >= len(refs) || used[link.Index] || kinds[refs[link.Index].Kind] {
				return ErrUnqualified
			}
			previous = link.Index
			used[link.Index] = true
			kinds[refs[link.Index].Kind] = true
		}
	}
	for _, assigned := range used {
		if !assigned {
			return ErrUnqualified
		}
	}
	return nil
}

func validPhysicalRetirement(p PhysicalRetirement, epoch recoverypermit.EpochIdentity) bool {
	if p.OldWorkerPoolID != epoch.WorkerPoolID || p.OldCapacitySlots < 1 || p.OldCapacitySlots > maxCapacitySlots ||
		!digestSHA256(p.ServiceProcessesDigest) || !digestSHA256(p.RunnerProcessesDigest) ||
		!digestSHA256(p.WorkerPoolDigest) || !digestSHA256(p.NetworkIsolationDigest) ||
		!digestSHA256(p.InflightEffectsDigest) || p.Assertion != retirementAssertion {
		return false
	}
	_, err := time.Parse(time.RFC3339Nano, p.RetiredAt)
	return err == nil
}

func validNamespaceRetirement(n NamespaceRetirement, name, oldUID string) bool {
	return n.Name == name && n.OldUID == oldUID &&
		digestSHA256(n.UIDAbsenceEvidenceDigest) && digestSHA256(n.NoReuseFenceDigest) &&
		digestSHA256(n.NormalDeletionDigest)
}

func validSuccessor(s SuccessorIdentity, old recoverypermit.EpochIdentity) bool {
	return len(validation.IsDNS1123Label(s.RunnerNamespace)) == 0 &&
		len(validation.IsDNS1123Label(s.ReceiptNamespace)) == 0 &&
		s.RunnerNamespace != s.ReceiptNamespace &&
		s.RunnerNamespace != old.Namespace && s.RunnerNamespace != old.ReceiptNamespace &&
		s.ReceiptNamespace != old.Namespace && s.ReceiptNamespace != old.ReceiptNamespace &&
		s.RunnerNamespaceUID != old.NamespaceUID && s.ReceiptNamespaceUID != old.ReceiptNamespaceUID &&
		s.RunnerNamespaceUID != s.ReceiptNamespaceUID &&
		s.RunnerNamespaceUID != old.ReceiptNamespaceUID && s.ReceiptNamespaceUID != old.NamespaceUID &&
		s.GenesisUID != old.GenesisUID && s.Generation != old.Generation &&
		s.WorkerPoolID != old.WorkerPoolID &&
		s.CapacitySlots > 0 && s.CapacitySlots <= maxCapacitySlots &&
		len(validation.IsDNS1123Subdomain(s.GenesisName)) == 0 &&
		len(validation.IsDNS1123Label(s.RouteNamespace)) == 0 &&
		len(validation.IsDNS1123Subdomain(s.RouteName)) == 0 &&
		s.RouteFromNamespaceUID == old.NamespaceUID && s.RouteToNamespaceUID == s.RunnerNamespaceUID &&
		s.RouteExpectedResourceVersion != s.RouteCommittedResourceVersion &&
		safeID(s.RunnerNamespaceUID, maxIDBytes) && safeID(s.ReceiptNamespaceUID, maxIDBytes) &&
		safeID(s.GenesisUID, maxIDBytes) && lowerHex(s.Generation, 32) &&
		safeID(s.WorkerPoolID, maxIDBytes) && safeID(s.RouteUID, maxIDBytes) &&
		safeID(s.RouteExpectedResourceVersion, maxIDBytes) &&
		safeID(s.RouteCommittedResourceVersion, maxIDBytes) &&
		safeID(s.RouteFromNamespaceUID, maxIDBytes) &&
		safeID(s.RouteToNamespaceUID, maxIDBytes) && digestSHA256(s.CapacityProofDigest) &&
		digestSHA256(s.RouteCASDigest)
}

func validEpoch(e recoverypermit.EpochIdentity) bool {
	return len(validation.IsDNS1123Label(e.Namespace)) == 0 &&
		len(validation.IsDNS1123Label(e.ReceiptNamespace)) == 0 && e.Namespace != e.ReceiptNamespace &&
		len(validation.IsDNS1123Subdomain(e.GenesisName)) == 0 &&
		safeID(e.NamespaceUID, maxIDBytes) && safeID(e.ReceiptNamespaceUID, maxIDBytes) &&
		safeID(e.GenesisUID, maxIDBytes) && lowerHex(e.Generation, 32) &&
		safeID(e.ActiveLedgerUID, maxIDBytes) && safeID(e.QueueLedgerUID, maxIDBytes) &&
		safeID(e.WorkerPoolID, maxIDBytes)
}

func envelopeSizeOK(raw []byte) bool { return len(raw) > 0 && len(raw) <= maxEnvelopeBytes }

func decodeCanonical(raw []byte, value any) error {
	if err := json.Unmarshal(raw, value); err != nil {
		return fmt.Errorf("%w: decode: %v", ErrUnqualified, err)
	}
	canonical, err := json.Marshal(value)
	if err != nil || !bytes.Equal(raw, canonical) {
		return fmt.Errorf("%w: noncanonical envelope", ErrUnqualified)
	}
	return nil
}

func verifySignature(payload any, encoded string, domain []byte, issuer, keyID string, roots TrustRoots) bool {
	key := roots[issuer][keyID]
	if len(key) != ed25519.PublicKeySize {
		return false
	}
	signature, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || len(signature) != ed25519.SignatureSize || base64.StdEncoding.EncodeToString(signature) != encoded {
		return false
	}
	payloadRaw, err := json.Marshal(payload)
	if err != nil {
		return false
	}
	message := append(append([]byte{}, domain...), payloadRaw...)
	return ed25519.Verify(key, message, signature)
}

func digestBytes(raw []byte) string {
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
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
