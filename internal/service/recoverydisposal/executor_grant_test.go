package recoverydisposal

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestMutationGrantIndependentDomainPinAndTimeBounds(t *testing.T) {
	f := newExecutionFixture(t)
	planRaw := f.raw(t, executionPlanDomain)
	plan, err := VerifyExecutionPlan(planRaw, executionExpectation(t, f.payload, planRaw), f.roots)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	base := MutationGrantPayload{
		Version: MutationGrantVersion, Audience: MutationGrantAudience, Action: MutationGrantAction,
		Issuer: f.payload.Issuer, KeyID: f.payload.KeyID, IncidentID: f.payload.IncidentID,
		Cluster: f.payload.Cluster, Source: f.payload.Source, PlanDigest: plan.PlanDigest(), PlanEnvelopeDigest: plan.EnvelopeDigest(),
		ReviewDigest: hash('a'), NotBefore: now.Add(-time.Minute).Format(time.RFC3339Nano), ExpiresAt: now.Add(time.Minute).Format(time.RFC3339Nano),
	}
	rawFor := func(p MutationGrantPayload, domain []byte) []byte {
		raw, err := json.Marshal(MutationGrantEnvelope{Payload: p, Signature: sign(t, p, domain, f.private)})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	raw := rawFor(base, mutationGrantDomain)
	expected := MutationGrantExpectation{EnvelopeDigest: digestBytes(raw), Payload: base}
	verified, err := VerifyMutationGrant(raw, expected, f.roots, plan, now)
	if err != nil || !verified.Valid() || verified.EnvelopeDigest() != expected.EnvelopeDigest || verified.Payload() != base {
		t.Fatalf("independent grant rejected: %#v %v", verified, err)
	}
	if (VerifiedMutationGrant{}).Valid() {
		t.Fatal("zero grant is not execution authority")
	}
	cases := map[string]func(*MutationGrantPayload){
		"wrong domain action": func(p *MutationGrantPayload) { p.Action = ExecutionPlanAction },
		"wrong audience":      func(p *MutationGrantPayload) { p.Audience = ExecutionPlanAudience },
		"wrong version":       func(p *MutationGrantPayload) { p.Version = "2" },
		"wrong incident":      func(p *MutationGrantPayload) { p.IncidentID = "another-incident" },
		"wrong cluster":       func(p *MutationGrantPayload) { p.Cluster.SystemNamespaceUID = "another-system" },
		"wrong source":        func(p *MutationGrantPayload) { p.Source.NamespaceUID = "another-runner" },
		"wrong plan":          func(p *MutationGrantPayload) { p.PlanDigest = hash('b') },
		"wrong envelope":      func(p *MutationGrantPayload) { p.PlanEnvelopeDigest = hash('b') },
		"unreviewed":          func(p *MutationGrantPayload) { p.ReviewDigest = "" },
		"future grant":        func(p *MutationGrantPayload) { p.NotBefore = now.Add(time.Second).Format(time.RFC3339Nano) },
		"expired":             func(p *MutationGrantPayload) { p.ExpiresAt = now.Format(time.RFC3339Nano) },
		"empty interval":      func(p *MutationGrantPayload) { p.ExpiresAt = p.NotBefore },
		"too broad window":    func(p *MutationGrantPayload) { p.ExpiresAt = now.Add(maxMutationGrantAge).Format(time.RFC3339Nano) },
		"before plan issued":  func(p *MutationGrantPayload) { p.NotBefore = "2026-09-01T00:00:00Z" },
		"invalid date":        func(p *MutationGrantPayload) { p.ExpiresAt = "never" },
		"invalid issuer":      func(p *MutationGrantPayload) { p.Issuer = "unsafe issuer" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			p := base
			mutate(&p)
			raw := rawFor(p, mutationGrantDomain)
			got, err := VerifyMutationGrant(raw, MutationGrantExpectation{EnvelopeDigest: digestBytes(raw), Payload: p}, f.roots, plan, now)
			if !errors.Is(err, ErrUnqualified) || got.Valid() {
				t.Fatal("unqualified grant accepted")
			}
		})
	}
	for _, domain := range [][]byte{executionPlanDomain, authorizationDomain, closureDomain} {
		wrong := rawFor(base, domain)
		if _, err := VerifyMutationGrant(wrong, MutationGrantExpectation{EnvelopeDigest: digestBytes(wrong), Payload: base}, f.roots, plan, now); !errors.Is(err, ErrUnqualified) {
			t.Fatal("cross-domain signature accepted")
		}
	}
	if _, err := VerifyMutationGrant(raw, expected, TrustRoots{}, plan, now); !errors.Is(err, ErrUnqualified) {
		t.Fatal("untrusted execution signer accepted")
	}
	if _, err := VerifyMutationGrant(raw, expected, f.roots, VerifiedExecutionPlan{}, now); !errors.Is(err, ErrUnqualified) {
		t.Fatal("grant accepted without sealed plan")
	}
	if _, err := VerifyMutationGrant(raw, expected, f.roots, plan, now.Add(2*time.Minute)); !errors.Is(err, ErrUnqualified) {
		t.Fatal("stale grant accepted")
	}
	badPin := expected
	badPin.EnvelopeDigest = hash('c')
	if _, err := VerifyMutationGrant(raw, badPin, f.roots, plan, now); !errors.Is(err, ErrUnqualified) {
		t.Fatal("unpinned envelope accepted")
	}
}

func TestMutationGrantRejectsNoncanonicalUnknownAndOversizedEncoding(t *testing.T) {
	f := newExecutorFixture(t)
	// The independent fixture authority is only available to tests, never to
	// the production executor. Re-sign with a separate test root and pin.
	public, private, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	p := f.grant.payload
	p.Issuer = "execute-authority"
	p.KeyID = "execute-key"
	roots := TrustRoots{p.Issuer: {p.KeyID: public}}
	raw, err := json.Marshal(MutationGrantEnvelope{Payload: p, Signature: sign(t, p, mutationGrantDomain, private)})
	if err != nil {
		t.Fatal(err)
	}
	variants := [][]byte{
		append(bytes.Clone(raw), ' '),
		bytes.Replace(raw, []byte(`"version":"1"`), []byte(`"version":"1","version":"1"`), 1),
		append([]byte(`{"extra":"untrusted",`), raw[1:]...),
		bytes.Replace(raw, []byte(`"reviewDigest":`), []byte(`"unknownField":"x","reviewDigest":`), 1),
		[]byte(strings.Repeat("x", maxMutationGrantBytes+1)),
	}
	for _, variant := range variants {
		got, err := VerifyMutationGrant(variant, MutationGrantExpectation{EnvelopeDigest: digestBytes(variant), Payload: p}, roots, f.plan, time.Now())
		if !errors.Is(err, ErrUnqualified) || got.Valid() {
			t.Fatal("malformed grant accepted")
		}
	}
	expected := MutationGrantExpectation{EnvelopeDigest: digestBytes(raw), Payload: p}
	got, err := VerifyMutationGrant(raw, expected, roots, f.plan, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	expected.Payload.ReviewDigest = hash('e')
	copy := got.Payload()
	copy.ReviewDigest = hash('e')
	if got.Payload().ReviewDigest != p.ReviewDigest {
		t.Fatal("verified grant changed through input/output copy")
	}
}
