package recoverydisposal

import (
	"bytes"
	"encoding/json"
	"slices"
	"strings"
	"time"

	"github.com/cofy-x/kova/internal/admissioncontract"
	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/recoverypermit"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	ExecutionPlanVersion  = "1"
	ExecutionPlanAudience = "kova-recovery-disposal-execution-plan-v1"
	ExecutionPlanAction   = "describe-exact-old-epoch-object-disposal-only"
	// QualificationVersion identifies an independently observed, role-specific
	// object qualification record. This schema does not compute that record.
	ExecutionQualificationVersion = "kova-disposal-object-qualification-v1"
	ExecutionArchiveAssertion     = "complete-archive-durable-outside-both-old-namespaces-v1"
	ExecutionInventoryAssertion   = "complete-unfiltered-old-epoch-physical-target-inventory-v1"
	ExecutionNoReuseAssertion     = "both-old-namespace-names-persistently-fenced-against-reuse-v1"

	maxExecutionEnvelopeBytes  = 16 * 1024 * 1024
	maxExecutionObjects        = 8192
	maxExecutionArchiveObjects = 8192
	maxExecutionDispositions   = 2048
	maxExecutionCalls          = 65536
	maxExecutionObjectBytes    = 16 * 1024 * 1024
	maxExecutionArchiveBytes   = 1024 * 1024 * 1024
	executionHoldFinalizer     = "kova.cofy.dev/recovery-hold"
)

var executionPlanDomain = []byte("KOVA-RECOVERY-DISPOSAL-EXECUTION-PLAN-V1\x00")

// ExecutionClusterIdentity is an independently pinned cluster identity, not
// a kubeconfig context name. APIIdentityDigest names an external API identity
// record; this package neither fetches it nor proves its contents.
type ExecutionClusterIdentity struct {
	SystemNamespace    string `json:"systemNamespace"`
	SystemNamespaceUID string `json:"systemNamespaceUid"`
	APIIdentityDigest  string `json:"apiIdentityDigest"`
}

// ExecutionEvidence binds external records. InflightBarrierDigest must cover
// already admitted API-server/proxy requests as well as client processes;
// Namespace Terminating, an empty List, or a killed client is not that proof.
// FullArchiveManifestDigest covers results, dispositions, source evidence and
// all inventoried objects, including direct/admin objects without receipts.
// The archive may also contain non-target objects owned by external operators.
// StorageIdentityDigest identifies caller-owned storage without introducing a
// provider, credential, URI fetcher, or Kova-owned storage lifecycle.
type ExecutionEvidence struct {
	PhysicalRetirement        PhysicalRetirement `json:"physicalRetirement"`
	StopIntentDigest          string             `json:"stopIntentDigest"`
	InflightBarrierDigest     string             `json:"inflightBarrierDigest"`
	FullArchiveManifestDigest string             `json:"fullArchiveManifestDigest"`
	StorageIdentityDigest     string             `json:"storageIdentityDigest"`
	ArchiveAssertion          string             `json:"archiveAssertion"`
	ArchiveObjectCount        int                `json:"archiveObjectCount"`
	ArchiveBytes              int64              `json:"archiveBytes"`
	InventoryObserverDigest   string             `json:"inventoryObserverDigest"`
	InventoryAssertion        string             `json:"inventoryAssertion"`
	RunnerNoReuseFenceDigest  string             `json:"runnerNoReuseFenceDigest"`
	ReceiptNoReuseFenceDigest string             `json:"receiptNoReuseFenceDigest"`
	NoReuseAssertion          string             `json:"noReuseAssertion"`
	ObservedAt                string             `json:"observedAt"`
}

// ExecutionLimits are independently selected ceilings, not estimates inferred
// from active admission capacity. MaxObjects bounds actionable targets while
// MaxArchiveObjects independently bounds the complete archive, including
// non-actionable objects. MaxCalls bounds executor API-interface calls, not
// transport wire attempts or a promise that execution can finish within it.
// Overflow must block the whole plan; these limits never permit silently
// truncating or re-batching an
// allegedly complete inventory.
type ExecutionLimits struct {
	MaxObjects            int   `json:"maxObjects"`
	MaxArchiveObjects     int   `json:"maxArchiveObjects"`
	MaxDispositions       int   `json:"maxDispositions"`
	MaxCalls              int   `json:"maxCalls"`
	MaxObjectArchiveBytes int64 `json:"maxObjectArchiveBytes"`
	MaxArchiveBytes       int64 `json:"maxArchiveBytes"`
}

// ExecutionDisposition describes an economic liability separately from
// physical targets. Receipt links are deliberately not required: an admin CR
// and a build cancelled before Pod creation are still liabilities. Original
// UIDs are historical identities, never the UID of a replacement tombstone.
// An approved unknown may have no observed original UID. Neither outcome
// authorizes replay, capacity release, or construction of a successor.
type ExecutionDisposition struct {
	ID                 string `json:"id"`
	BuildName          string `json:"buildName"`
	OriginalBuildUID   string `json:"originalBuildUid"`
	OriginalPodUID     string `json:"originalPodUid"`
	Outcome            string `json:"outcome"`
	ResultRecordDigest string `json:"resultRecordDigest"`
	ApprovalDigest     string `json:"approvalDigest"`
	NeverReplay        bool   `json:"neverReplay"`
}

// ExecutionResource is a closed allowlist of namespaced resources. No
// Namespace, subresource, wildcard, or dynamically selected GVR is accepted.
type ExecutionResource struct {
	Group    string `json:"group"`
	Version  string `json:"version"`
	Resource string `json:"resource"`
}

// ExecutionTarget names one occupant in the complete pre-disposal inventory.
// Only one UID may occupy a GVR/namespace/name in that inventory; historical
// originals belong in ExecutionDisposition, not in a second same-name target.
// ArchiveResourceVersion is an observation, not a reusable future CAS token.
// The separate executor requires fresh direct UID/RV/body qualification, both
// exact old namespaces Terminating, a distinct mutation grant, and conditional
// per-object operations.
// AllowedKovaFinalizers is a sorted subset, never permission to erase all
// finalizers. Foreign finalizers are not included or authorized here.
type ExecutionTarget struct {
	Resource               ExecutionResource `json:"resource"`
	Namespace              string            `json:"namespace"`
	NamespaceUID           string            `json:"namespaceUid"`
	Name                   string            `json:"name"`
	UID                    string            `json:"uid"`
	Role                   string            `json:"role"`
	DispositionID          string            `json:"dispositionId"`
	ArchiveResourceVersion string            `json:"archiveResourceVersion"`
	ArchiveDigest          string            `json:"archiveDigest"`
	ArchiveBytes           int64             `json:"archiveBytes"`
	QualificationVersion   string            `json:"qualificationVersion"`
	QualificationDigest    string            `json:"qualificationDigest"`
	AllowedKovaFinalizers  []string          `json:"allowedKovaFinalizers"`
}

// ExecutionPlanPayload is an independent statement, not v1 authorization
// converted into a mutation permit. AuthorizationDigest binds an independently
// approved incident decision which can cover unreceipted liabilities; it need
// not be a receipt-centric AuthorizationEnvelope. Targets are sorted by the
// tuple (group, version, resource, namespace, name), dispositions by ID, with
// duplicate physical addresses and UIDs rejected rather than normalized.
type ExecutionPlanPayload struct {
	Version             string                       `json:"version"`
	Audience            string                       `json:"audience"`
	Action              string                       `json:"action"`
	Issuer              string                       `json:"issuer"`
	KeyID               string                       `json:"keyId"`
	IncidentID          string                       `json:"incidentId"`
	Cluster             ExecutionClusterIdentity     `json:"cluster"`
	Source              recoverypermit.EpochIdentity `json:"source"`
	AuthorizationDigest string                       `json:"authorizationDigest"`
	Evidence            ExecutionEvidence            `json:"evidence"`
	Limits              ExecutionLimits              `json:"limits"`
	Dispositions        []ExecutionDisposition       `json:"dispositions"`
	Targets             []ExecutionTarget            `json:"targets"`
	IssuedAt            string                       `json:"issuedAt"`
}

type ExecutionPlanEnvelope struct {
	Payload   ExecutionPlanPayload `json:"payload"`
	Signature string               `json:"signature"`
}

// ExecutionPlanExpectation is pinned independently by the incident authority
// and direct observer. Payload, PlanDigest (canonical whole-payload SHA-256),
// and EnvelopeDigest must not be copied from the envelope under verification.
// Matching external assertions is not evidence that their facts are true.
type ExecutionPlanExpectation struct {
	EnvelopeDigest string
	PlanDigest     string
	Payload        ExecutionPlanPayload
}

// VerifiedExecutionPlan certifies schema, signature and independent pin
// agreement only. It is NOT a mutation permit or proof of archive durability,
// inventory completeness, physical retirement, in-flight quiescence, namespace
// termination, no-reuse fences, or any successor. It exposes no API client,
// executor, capacity release, signer, or external storage operation.
type VerifiedExecutionPlan struct {
	valid          bool
	planDigest     string
	envelopeDigest string
	payload        ExecutionPlanPayload
}

func (v VerifiedExecutionPlan) Valid() bool            { return v.valid }
func (v VerifiedExecutionPlan) PlanDigest() string     { return v.planDigest }
func (v VerifiedExecutionPlan) EnvelopeDigest() string { return v.envelopeDigest }

// Payload returns a defensive copy, including every nested finalizer slice.
func (v VerifiedExecutionPlan) Payload() ExecutionPlanPayload {
	return cloneExecutionPayload(v.payload)
}

// DigestExecutionPlan validates the bounded schema and hashes its entire
// canonical JSON payload. It does not sign it or establish independent trust.
func DigestExecutionPlan(p ExecutionPlanPayload) (string, error) {
	if !validExecutionPlan(p) {
		return "", ErrUnqualified
	}
	raw, err := json.Marshal(p)
	if err != nil || len(raw) > maxExecutionEnvelopeBytes {
		return "", ErrUnqualified
	}
	return digestBytes(raw), nil
}

// VerifyExecutionPlan is deliberately pure: no Kubernetes/API reads or writes
// and no evidence fetching. The signature domain is distinct from v1 disposal,
// closure and drain. Zero, malformed, noncanonical and unpinned inputs fail
// closed, including duplicate JSON fields and unknown fields.
func VerifyExecutionPlan(raw []byte, expected ExecutionPlanExpectation, roots TrustRoots) (VerifiedExecutionPlan, error) {
	if len(raw) == 0 || len(raw) > maxExecutionEnvelopeBytes ||
		!digestSHA256(expected.EnvelopeDigest) || !digestSHA256(expected.PlanDigest) ||
		digestBytes(raw) != expected.EnvelopeDigest {
		return VerifiedExecutionPlan{}, ErrUnqualified
	}
	pinnedDigest, err := DigestExecutionPlan(expected.Payload)
	if err != nil || pinnedDigest != expected.PlanDigest {
		return VerifiedExecutionPlan{}, ErrUnqualified
	}
	// Decode only the bounded outer envelope. A malicious JSON array of many
	// empty targets must not expand into arbitrarily many typed structs, even
	// if an operator mistakenly pinned its whole-envelope digest. The only
	// typed payload used below is the independently bounded expectation.
	var envelope struct {
		Payload   json.RawMessage `json:"payload"`
		Signature string          `json:"signature"`
	}
	if err := decodeCanonical(raw, &envelope); err != nil {
		// Never echo untrusted tokens or numeric overflow payloads from a
		// decoder diagnostic, even if a shared decoder changes in the future.
		return VerifiedExecutionPlan{}, ErrUnqualified
	}
	pinnedRaw, _ := json.Marshal(expected.Payload) // already validated and marshaled above
	if !bytes.Equal(envelope.Payload, pinnedRaw) ||
		!verifySignature(expected.Payload, envelope.Signature, executionPlanDomain,
			expected.Payload.Issuer, expected.Payload.KeyID, roots) {
		return VerifiedExecutionPlan{}, ErrUnqualified
	}
	return VerifiedExecutionPlan{valid: true, planDigest: pinnedDigest,
		envelopeDigest: expected.EnvelopeDigest, payload: cloneExecutionPayload(expected.Payload)}, nil
}

func cloneExecutionPayload(p ExecutionPlanPayload) ExecutionPlanPayload {
	p.Dispositions = slices.Clone(p.Dispositions)
	p.Targets = slices.Clone(p.Targets)
	for i := range p.Targets {
		p.Targets[i].AllowedKovaFinalizers = slices.Clone(p.Targets[i].AllowedKovaFinalizers)
	}
	return p
}

func validExecutionPlan(p ExecutionPlanPayload) bool {
	if p.Version != ExecutionPlanVersion || p.Audience != ExecutionPlanAudience || p.Action != ExecutionPlanAction ||
		!safeID(p.Issuer, maxIDBytes) || !safeID(p.KeyID, maxIDBytes) || !safeID(p.IncidentID, maxIncidentIDBytes) ||
		!validEpoch(p.Source) || !admissioncontract.ValidWorkerPoolID(p.Source.WorkerPoolID) ||
		!distinctExecutionSourceUIDs(p) ||
		p.Cluster.SystemNamespace != "kube-system" || !safeID(p.Cluster.SystemNamespaceUID, maxIDBytes) ||
		p.Source.Namespace == p.Cluster.SystemNamespace || p.Source.ReceiptNamespace == p.Cluster.SystemNamespace ||
		p.Cluster.SystemNamespaceUID == p.Source.NamespaceUID || p.Cluster.SystemNamespaceUID == p.Source.ReceiptNamespaceUID ||
		!digestSHA256(p.Cluster.APIIdentityDigest) || !digestSHA256(p.AuthorizationDigest) ||
		!validExecutionLimits(p.Limits) || !validExecutionEvidence(p) ||
		p.Targets == nil || len(p.Targets) > p.Limits.MaxObjects ||
		p.Dispositions == nil || len(p.Dispositions) > p.Limits.MaxDispositions ||
		p.Evidence.ArchiveObjectCount < len(p.Targets) {
		return false
	}
	dispositions := make(map[string]ExecutionDisposition, len(p.Dispositions))
	originalUIDs := map[string]bool{}
	for i, d := range p.Dispositions {
		if !validExecutionDisposition(d) || (i > 0 && p.Dispositions[i-1].ID >= d.ID) {
			return false
		}
		for _, uid := range []string{d.OriginalBuildUID, d.OriginalPodUID} {
			if uid != "" {
				if originalUIDs[uid] || reservedExecutionUID(p, uid) || executionControlUIDRole(p, uid) != "" {
					return false
				}
				originalUIDs[uid] = true
			}
		}
		dispositions[d.ID] = d
	}
	seenUIDs := map[string]bool{}
	var archiveBytes int64
	for i, target := range p.Targets {
		if !validExecutionTarget(p, target, dispositions) || seenUIDs[target.UID] ||
			(i > 0 && executionTargetKey(p.Targets[i-1]) >= executionTargetKey(target)) ||
			(originalUIDs[target.UID] && target.Role != "original-build" && target.Role != "original-pod") {
			return false
		}
		seenUIDs[target.UID] = true
		archiveBytes += target.ArchiveBytes // bounded per item and count, far below int64 overflow
		if archiveBytes > p.Evidence.ArchiveBytes {
			return false
		}
	}
	return true
}

func validExecutionLimits(l ExecutionLimits) bool {
	return l.MaxObjects > 0 && l.MaxObjects <= maxExecutionObjects &&
		l.MaxArchiveObjects > 0 && l.MaxArchiveObjects <= maxExecutionArchiveObjects &&
		l.MaxDispositions > 0 && l.MaxDispositions <= maxExecutionDispositions &&
		l.MaxCalls > 0 && l.MaxCalls <= maxExecutionCalls &&
		l.MaxObjectArchiveBytes > 0 && l.MaxObjectArchiveBytes <= maxExecutionObjectBytes &&
		l.MaxArchiveBytes >= l.MaxObjectArchiveBytes && l.MaxArchiveBytes <= maxExecutionArchiveBytes
}

func validExecutionEvidence(p ExecutionPlanPayload) bool {
	e := p.Evidence
	if !validPhysicalRetirement(e.PhysicalRetirement, p.Source) || !digestSHA256(e.StopIntentDigest) ||
		!digestSHA256(e.InflightBarrierDigest) || !digestSHA256(e.FullArchiveManifestDigest) ||
		!digestSHA256(e.StorageIdentityDigest) || e.ArchiveAssertion != ExecutionArchiveAssertion ||
		e.ArchiveObjectCount < 0 || e.ArchiveObjectCount > p.Limits.MaxArchiveObjects ||
		e.ArchiveBytes < 1 || e.ArchiveBytes > p.Limits.MaxArchiveBytes ||
		!digestSHA256(e.InventoryObserverDigest) || e.InventoryAssertion != ExecutionInventoryAssertion ||
		!digestSHA256(e.RunnerNoReuseFenceDigest) || !digestSHA256(e.ReceiptNoReuseFenceDigest) ||
		e.NoReuseAssertion != ExecutionNoReuseAssertion {
		return false
	}
	retired, _ := time.Parse(time.RFC3339Nano, e.PhysicalRetirement.RetiredAt)
	observed, err := time.Parse(time.RFC3339Nano, e.ObservedAt)
	if err != nil || observed.Before(retired) {
		return false
	}
	issued, err := time.Parse(time.RFC3339Nano, p.IssuedAt)
	return err == nil && !issued.Before(observed)
}

func validExecutionDisposition(d ExecutionDisposition) bool {
	if !safeID(d.ID, maxIDBytes) || len(validation.IsDNS1123Subdomain(d.BuildName)) != 0 || !d.NeverReplay ||
		(d.OriginalBuildUID != "" && !safeID(d.OriginalBuildUID, maxIDBytes)) ||
		(d.OriginalPodUID != "" && (!safeID(d.OriginalPodUID, maxIDBytes) || d.OriginalBuildUID == "")) {
		return false
	}
	switch d.Outcome {
	case "terminal-result":
		return d.OriginalBuildUID != "" && digestSHA256(d.ResultRecordDigest) && d.ApprovalDigest == ""
	case "approved-unknown-discard":
		return digestSHA256(d.ApprovalDigest) && d.ResultRecordDigest == ""
	default:
		return false
	}
}

func validExecutionTarget(p ExecutionPlanPayload, t ExecutionTarget, dispositions map[string]ExecutionDisposition) bool {
	if len(validation.IsDNS1123Subdomain(t.Name)) != 0 || !safeID(t.UID, maxIDBytes) || reservedExecutionUID(p, t.UID) ||
		!safeID(t.ArchiveResourceVersion, maxIDBytes) || !digestSHA256(t.ArchiveDigest) ||
		t.ArchiveBytes < 1 || t.ArchiveBytes > p.Limits.MaxObjectArchiveBytes ||
		t.QualificationVersion != ExecutionQualificationVersion || !digestSHA256(t.QualificationDigest) ||
		t.AllowedKovaFinalizers == nil || len(t.AllowedKovaFinalizers) > 2 {
		return false
	}
	if (t.Namespace != p.Source.Namespace || t.NamespaceUID != p.Source.NamespaceUID) &&
		(t.Namespace != p.Source.ReceiptNamespace || t.NamespaceUID != p.Source.ReceiptNamespaceUID) {
		return false
	}
	if role := executionControlUIDRole(p, t.UID); role != "" && t.Role != role {
		return false
	}
	d, hasDisposition := dispositions[t.DispositionID]
	resource := ExecutionResource{Version: "v1", Resource: "configmaps"}
	requiresDisposition := true
	switch t.Role {
	case "original-build", "build-tombstone":
		resource = ExecutionResource{Group: kovav1.Group, Version: kovav1.Version, Resource: "kovabuilds"}
		if t.Namespace != p.Source.Namespace || !hasDisposition || t.Name != d.BuildName ||
			(t.Role == "original-build" && t.UID != d.OriginalBuildUID) ||
			(t.Role == "build-tombstone" && t.UID == d.OriginalBuildUID) {
			return false
		}
	case "original-pod", "pod-tombstone":
		resource.Resource = "pods"
		if t.Namespace != p.Source.Namespace || !hasDisposition ||
			(t.Role == "original-pod" && t.UID != d.OriginalPodUID) ||
			(t.Role == "pod-tombstone" && t.UID == d.OriginalPodUID) {
			return false
		}
	case "queue-receipt", "grant-receipt", "pod-create-receipt":
		if t.Namespace != p.Source.ReceiptNamespace {
			return false
		}
	case "occupancy-attempt":
		if t.Namespace != p.Source.Namespace {
			return false
		}
	case "genesis":
		requiresDisposition = false
		if t.Namespace != p.Source.Namespace || t.Name != p.Source.GenesisName || t.UID != p.Source.GenesisUID {
			return false
		}
	case "active-ledger", "queue-ledger":
		requiresDisposition = false
		if t.Namespace != p.Source.Namespace || (t.Role == "active-ledger" && t.UID != p.Source.ActiveLedgerUID) ||
			(t.Role == "queue-ledger" && t.UID != p.Source.QueueLedgerUID) {
			return false
		}
	case "stop-intent":
		requiresDisposition = false
	default:
		return false
	}
	if t.Resource != resource || (requiresDisposition && !hasDisposition) || (!requiresDisposition && t.DispositionID != "") {
		return false
	}
	for i, finalizer := range t.AllowedKovaFinalizers {
		if i > 0 && t.AllowedKovaFinalizers[i-1] >= finalizer {
			return false
		}
		switch finalizer {
		case kovav1.CleanupFinalizer:
			if t.Role != "original-build" {
				return false
			}
		case executionHoldFinalizer:
			if t.Role != "original-build" && t.Role != "build-tombstone" && t.Role != "original-pod" &&
				t.Role != "pod-tombstone" && t.Role != "occupancy-attempt" {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func reservedExecutionUID(p ExecutionPlanPayload, uid string) bool {
	return uid == p.Cluster.SystemNamespaceUID || uid == p.Source.NamespaceUID || uid == p.Source.ReceiptNamespaceUID
}

func executionTargetKey(t ExecutionTarget) string {
	return strings.Join([]string{t.Resource.Group, t.Resource.Version, t.Resource.Resource, t.Namespace, t.Name}, "\x00")
}

func executionControlUIDRole(p ExecutionPlanPayload, uid string) string {
	switch uid {
	case p.Source.GenesisUID:
		return "genesis"
	case p.Source.ActiveLedgerUID:
		return "active-ledger"
	case p.Source.QueueLedgerUID:
		return "queue-ledger"
	default:
		return ""
	}
}

func distinctExecutionSourceUIDs(p ExecutionPlanPayload) bool {
	seen := map[string]bool{}
	for _, uid := range []string{p.Cluster.SystemNamespaceUID, p.Source.NamespaceUID, p.Source.ReceiptNamespaceUID,
		p.Source.GenesisUID, p.Source.ActiveLedgerUID, p.Source.QueueLedgerUID} {
		if seen[uid] {
			return false
		}
		seen[uid] = true
	}
	return true
}
