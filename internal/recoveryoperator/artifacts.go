// Package recoveryoperator supplies an explicit, finite operator entrypoint to
// the exact-object disposal executor. It owns no signer, archive lifecycle,
// physical-retirement detector, namespace deletion or successor routing.
package recoveryoperator

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"time"

	"github.com/cofy-x/kova/internal/service/recoverydisposal"
)

const (
	maxPlanBytes          = 16 * 1024 * 1024
	maxGrantBytes         = 32 * 1024
	maxArchiveBundleBytes = 64*1024*1024 + 8193
	maxCredentialBytes    = 256 * 1024
)

var ErrInput = errors.New("recovery operator input is not independently qualified")

// PublicKeyPin contains only an externally selected Ed25519 public key.
// Plan and mutation roots are supplied independently, never copied from an
// incoming envelope. This entrypoint deliberately has no private signing key.
type PublicKeyPin struct {
	Issuer       string `json:"issuer"`
	KeyID        string `json:"keyId"`
	PublicKeyHex string `json:"publicKeyHex"`
}

// Pins is a canonical local input from the incident authority, not synthesized
// by this command. Its whole-file digest must be supplied independently.
// Existing signed plan/grant schemas remain authoritative; connection binds
// the real TLS endpoint to the plan's otherwise opaque APIIdentityDigest.
type Pins struct {
	Plan       recoverydisposal.ExecutionPlanExpectation `json:"plan"`
	Grant      recoverydisposal.MutationGrantExpectation `json:"grant"`
	PlanRoots  []PublicKeyPin                            `json:"planRoots"`
	GrantRoots []PublicKeyPin                            `json:"grantRoots"`
	Connection ConnectionRecord                          `json:"connection"`
}

type Files struct {
	PinsPath     string
	PinsDigest   string
	PlanPath     string
	GrantPath    string
	ArchivesPath string
}

// Prepared is sealed by Prepare. It contains sensitive archive bodies and
// must never be logged or serialized. Report exposes only bounded summaries.
type Prepared struct {
	plan       recoverydisposal.VerifiedExecutionPlan
	grant      recoverydisposal.VerifiedMutationGrant
	connection ConnectionRecord
	archives   []json.RawMessage
	report     recoverydisposal.ArchivePreflightReport
}

func (p Prepared) Report() recoverydisposal.ArchivePreflightReport { return p.report }

// Prepare performs all local qualification before a client can be connected.
// It neither proves external evidence assertions nor issues any API request.
func Prepare(ctx context.Context, files Files, now time.Time) (Prepared, error) {
	if ctx == nil || ctx.Err() != nil || !validDigest(files.PinsDigest) {
		return Prepared{}, ErrInput
	}
	raw, err := readRegularFile(files.PinsPath, maxPlanBytes, false)
	if err != nil || digest(raw) != files.PinsDigest {
		return Prepared{}, ErrInput
	}
	var pins Pins
	if decodeCanonical(raw, &pins) != nil || !validConnection(pins.Connection) ||
		digestCanonical(pins.Connection) != pins.Plan.Payload.Cluster.APIIdentityDigest ||
		pins.Connection.SystemNamespaceUID != pins.Plan.Payload.Cluster.SystemNamespaceUID {
		return Prepared{}, ErrInput
	}
	planRoots, err := roots(pins.PlanRoots)
	if err != nil {
		return Prepared{}, ErrInput
	}
	grantRoots, err := roots(pins.GrantRoots)
	if err != nil {
		return Prepared{}, ErrInput
	}
	raw, err = readRegularFile(files.PlanPath, maxPlanBytes, false)
	if err != nil {
		return Prepared{}, ErrInput
	}
	plan, err := recoverydisposal.VerifyExecutionPlan(raw, pins.Plan, planRoots)
	if err != nil {
		return Prepared{}, ErrInput
	}
	raw, err = readRegularFile(files.GrantPath, maxGrantBytes, false)
	if err != nil {
		return Prepared{}, ErrInput
	}
	grant, err := recoverydisposal.VerifyMutationGrant(raw, pins.Grant, grantRoots, plan, now)
	if err != nil {
		return Prepared{}, ErrInput
	}
	raw, err = readRegularFile(files.ArchivesPath, maxArchiveBundleBytes, true)
	if err != nil {
		return Prepared{}, ErrInput
	}
	var archives []json.RawMessage
	archives, err = decodeArchives(raw, len(plan.Payload().Targets), plan.Payload().Limits.MaxObjectArchiveBytes)
	if err != nil {
		return Prepared{}, ErrInput
	}
	report, err := recoverydisposal.PreflightArchives(ctx, plan, archives)
	if err != nil {
		return Prepared{}, ErrInput
	}
	return Prepared{plan: plan, grant: grant, connection: pins.Connection, archives: archives, report: report}, nil
}

func roots(pins []PublicKeyPin) (recoverydisposal.TrustRoots, error) {
	if len(pins) == 0 || len(pins) > 64 {
		return nil, ErrInput
	}
	r := recoverydisposal.TrustRoots{}
	previous := ""
	for _, pin := range pins {
		if !safeToken(pin.Issuer) || !safeToken(pin.KeyID) {
			return nil, ErrInput
		}
		key := pin.Issuer + "\x00" + pin.KeyID
		if key <= previous {
			return nil, ErrInput
		}
		previous = key
		pub, err := hex.DecodeString(pin.PublicKeyHex)
		if err != nil || len(pub) != ed25519.PublicKeySize || hex.EncodeToString(pub) != pin.PublicKeyHex {
			return nil, ErrInput
		}
		if r[pin.Issuer] == nil {
			r[pin.Issuer] = map[string]ed25519.PublicKey{}
		}
		r[pin.Issuer][pin.KeyID] = pub
	}
	return r, nil
}

func readRegularFile(path string, limit int64, private bool) ([]byte, error) {
	if path == "" {
		return nil, ErrInput
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || (private && info.Mode().Perm()&0077 != 0) {
		return nil, ErrInput
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, ErrInput
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(info, opened) || !opened.Mode().IsRegular() || opened.Size() > limit || opened.Size() <= 0 ||
		(private && opened.Mode().Perm()&0077 != 0) {
		return nil, ErrInput
	}
	raw, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil || len(raw) == 0 || int64(len(raw)) > limit {
		return nil, ErrInput
	}
	return raw, nil
}

func decodeCanonical(raw []byte, out any) error {
	if boundJSON(raw) != nil {
		return ErrInput
	}
	if json.Unmarshal(raw, out) != nil {
		return ErrInput
	}
	encoded, err := json.Marshal(out)
	if err != nil || !bytes.Equal(encoded, raw) {
		return ErrInput
	}
	return nil
}

// Bound array cardinality, nesting and token count before typed decoding can
// allocate attacker-selected slices. Independently pinned local files can
// still be malformed; pinning is not a reason to remove hard input ceilings.
func boundJSON(raw []byte) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	tokens := 0
	var value func(int) error
	value = func(depth int) error {
		if depth > 64 || tokens >= 500000 {
			return ErrInput
		}
		token, err := d.Token()
		if err != nil {
			return ErrInput
		}
		tokens++
		open, composite := token.(json.Delim)
		if !composite {
			return nil
		}
		if open != '[' && open != '{' {
			return ErrInput
		}
		count := 0
		for d.More() {
			count++
			if count > 8192 {
				return ErrInput
			}
			if open == '{' {
				key, err := d.Token()
				if err != nil {
					return ErrInput
				}
				if _, ok := key.(string); !ok {
					return ErrInput
				}
				tokens++
			}
			if err := value(depth + 1); err != nil {
				return err
			}
		}
		close, err := d.Token()
		if err != nil || close != json.Delim(map[json.Delim]json.Delim{'[': ']', '{': '}'}[open]) {
			return ErrInput
		}
		return nil
	}
	if value(0) != nil {
		return ErrInput
	}
	if _, err := d.Token(); err != io.EOF {
		return ErrInput
	}
	return nil
}

func decodeArchives(raw []byte, count int, objectLimit int64) ([]json.RawMessage, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	token, err := d.Token()
	if err != nil || token != json.Delim('[') {
		return nil, ErrInput
	}
	archives := make([]json.RawMessage, 0, count)
	for d.More() {
		if len(archives) >= count {
			return nil, ErrInput
		}
		var body json.RawMessage
		if d.Decode(&body) != nil || len(body) == 0 || int64(len(body)) > objectLimit || boundJSON(body) != nil {
			return nil, ErrInput
		}
		archives = append(archives, body)
	}
	if token, err := d.Token(); err != nil || token != json.Delim(']') || len(archives) != count {
		return nil, ErrInput
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, ErrInput
	}
	canonical, err := json.Marshal(archives)
	if err != nil || !bytes.Equal(raw, canonical) {
		return nil, ErrInput
	}
	return archives, nil
}

func digest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}
func digestCanonical(value any) string { raw, _ := json.Marshal(value); return digest(raw) }
func validDigest(value string) bool {
	if len(value) != 71 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	decoded, err := hex.DecodeString(value[7:])
	return err == nil && len(decoded) == 32 && hex.EncodeToString(decoded) == value[7:]
}
func safeToken(value string) bool {
	return value != "" && len(value) <= 256 && !strings.ContainsAny(value, " \t\r\n\x00")
}
