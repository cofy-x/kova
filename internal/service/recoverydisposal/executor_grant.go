package recoverydisposal

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/cofy-x/kova/internal/service/recoverypermit"
)

const (
	MutationGrantVersion  = "1"
	MutationGrantAudience = "kova-recovery-disposal-mutation-grant-v1"
	MutationGrantAction   = "conditionally-dispose-exact-terminating-old-epoch-targets-only"
	maxMutationGrantBytes = 32 * 1024
	maxMutationGrantAge   = 15 * time.Minute
)

var mutationGrantDomain = []byte("KOVA-RECOVERY-DISPOSAL-MUTATION-GRANT-V1\x00")

// MutationGrantPayload is a separately reviewed execution authorization, not
// an ExecutionPlanPayload converted into permission. ReviewDigest names the
// external authority's review of retirement, in-flight quiescence, durable
// out-of-namespace archival, and persistent namespace-name non-reuse fences.
// Kova cannot prove those external facts, issue this grant, or choose a trust
// root. The grant does not authorize namespace deletion, replay or capacity.
type MutationGrantPayload struct {
	Version            string                       `json:"version"`
	Audience           string                       `json:"audience"`
	Action             string                       `json:"action"`
	Issuer             string                       `json:"issuer"`
	KeyID              string                       `json:"keyId"`
	IncidentID         string                       `json:"incidentId"`
	Cluster            ExecutionClusterIdentity     `json:"cluster"`
	Source             recoverypermit.EpochIdentity `json:"source"`
	PlanDigest         string                       `json:"planDigest"`
	PlanEnvelopeDigest string                       `json:"planEnvelopeDigest"`
	ReviewDigest       string                       `json:"reviewDigest"`
	NotBefore          string                       `json:"notBefore"`
	ExpiresAt          string                       `json:"expiresAt"`
}

type MutationGrantEnvelope struct {
	Payload   MutationGrantPayload `json:"payload"`
	Signature string               `json:"signature"`
}

// MutationGrantExpectation is pinned independently by the execution authority.
// Neither its envelope digest nor its payload may be learned from the grant
// being verified. Execution trust roots are selected separately from plan
// trust roots; successful plan verification alone conveys no authority.
type MutationGrantExpectation struct {
	EnvelopeDigest string
	Payload        MutationGrantPayload
}

type VerifiedMutationGrant struct {
	valid          bool
	envelopeDigest string
	payload        MutationGrantPayload
	notBefore      time.Time
	expiresAt      time.Time
}

func (v VerifiedMutationGrant) Valid() bool            { return v.valid }
func (v VerifiedMutationGrant) EnvelopeDigest() string { return v.envelopeDigest }
func (v VerifiedMutationGrant) Payload() MutationGrantPayload {
	return v.payload // no reference-valued fields
}

// VerifyMutationGrant verifies an independently pinned, bounded, canonical
// signature in a distinct domain. Execute rechecks the actual clock before
// every API call; verification at an earlier time never extends the grant.
func VerifyMutationGrant(raw []byte, expected MutationGrantExpectation, roots TrustRoots, plan VerifiedExecutionPlan, now time.Time) (VerifiedMutationGrant, error) {
	if !plan.valid || len(raw) == 0 || len(raw) > maxMutationGrantBytes ||
		!digestSHA256(expected.EnvelopeDigest) || digestBytes(raw) != expected.EnvelopeDigest ||
		!validMutationGrant(expected.Payload, plan) {
		return VerifiedMutationGrant{}, ErrUnqualified
	}
	var envelope struct {
		Payload   json.RawMessage `json:"payload"`
		Signature string          `json:"signature"`
	}
	if decodeCanonical(raw, &envelope) != nil {
		return VerifiedMutationGrant{}, ErrUnqualified
	}
	pinned, err := json.Marshal(expected.Payload)
	if err != nil || !bytes.Equal(envelope.Payload, pinned) ||
		!verifySignature(expected.Payload, envelope.Signature, mutationGrantDomain,
			expected.Payload.Issuer, expected.Payload.KeyID, roots) {
		return VerifiedMutationGrant{}, ErrUnqualified
	}
	start, _ := time.Parse(time.RFC3339Nano, expected.Payload.NotBefore)
	end, _ := time.Parse(time.RFC3339Nano, expected.Payload.ExpiresAt)
	grant := VerifiedMutationGrant{valid: true, envelopeDigest: expected.EnvelopeDigest,
		payload: expected.Payload, notBefore: start, expiresAt: end}
	if !grant.current(now) {
		return VerifiedMutationGrant{}, ErrUnqualified
	}
	return grant, nil
}

func validMutationGrant(p MutationGrantPayload, plan VerifiedExecutionPlan) bool {
	if p.Version != MutationGrantVersion || p.Audience != MutationGrantAudience || p.Action != MutationGrantAction ||
		!safeID(p.Issuer, maxIDBytes) || !safeID(p.KeyID, maxIDBytes) ||
		p.IncidentID != plan.payload.IncidentID || p.Cluster != plan.payload.Cluster || p.Source != plan.payload.Source ||
		p.PlanDigest != plan.planDigest || p.PlanEnvelopeDigest != plan.envelopeDigest || !digestSHA256(p.ReviewDigest) {
		return false
	}
	start, err := time.Parse(time.RFC3339Nano, p.NotBefore)
	if err != nil {
		return false
	}
	end, err := time.Parse(time.RFC3339Nano, p.ExpiresAt)
	issued, _ := time.Parse(time.RFC3339Nano, plan.payload.IssuedAt)
	return err == nil && !start.Before(issued) && end.After(start) && end.Sub(start) <= maxMutationGrantAge
}

func (v VerifiedMutationGrant) current(now time.Time) bool {
	return v.valid && !now.Before(v.notBefore) && now.Before(v.expiresAt)
}
