package recoveryreceipt

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	grantReceiptPrefix  = "kova-grant-intent-"
	podReceiptPrefix    = "kova-pod-create-intent-"
	effectReceiptLabel  = "kova.cofy.dev/recovery-receipt"
	effectReceiptData   = "intent"
	effectVersion       = "1"
	maxEffectJSONBytes  = 16 * 1024
	maxEffectIDBytes    = 256
	maxWorkerPoolIDSize = 128
)

// PinnedBuild binds both effect receipts to one original, already selected
// namespace/Genesis/ledger epoch and one immutable KovaBuild. The worker-pool
// ID must be externally configured for that epoch; it is not a recovery grant.
type PinnedBuild struct {
	Namespace           string `json:"namespace"`
	NamespaceUID        string `json:"namespaceUid"`
	ReceiptNamespace    string `json:"receiptNamespace"`
	ReceiptNamespaceUID string `json:"receiptNamespaceUid"`
	GenesisName         string `json:"genesisName"`
	GenesisUID          string `json:"genesisUid"`
	Generation          string `json:"generation"`
	ActiveLedgerUID     string `json:"activeLedgerUid"`
	QueueLedgerUID      string `json:"queueLedgerUid"`
	BuildName           string `json:"buildName"`
	BuildUID            string `json:"buildUid"`
	RequesterName       string `json:"requesterName"`
	RequesterUID        string `json:"requesterUid"`
	RequesterHash       string `json:"requesterHash"`
	RequestDigest       string `json:"requestDigest"`
	SourceDigest        string `json:"sourceDigest"`
	WorkerPoolID        string `json:"workerPoolId"`
}

func (b PinnedBuild) validate() error {
	if len(validation.IsDNS1123Label(b.Namespace)) != 0 ||
		len(validation.IsDNS1123Label(b.ReceiptNamespace)) != 0 ||
		b.ReceiptNamespace == b.Namespace || !opaqueUID(b.ReceiptNamespaceUID) ||
		len(validation.IsDNS1123Subdomain(b.GenesisName)) != 0 ||
		len(validation.IsDNS1123Subdomain(b.BuildName)) != 0 ||
		!opaqueUID(b.NamespaceUID) || !opaqueUID(b.GenesisUID) ||
		!opaqueUID(b.ActiveLedgerUID) || !opaqueUID(b.QueueLedgerUID) || !opaqueUID(b.BuildUID) ||
		!lowerHex(b.Generation, 32) || !lowerHex(b.RequesterHash, 64) || !lowerHex(b.RequestDigest, 64) ||
		b.RequesterName == "" || !utf8.ValidString(b.RequesterName) || utf8.RuneCountInString(b.RequesterName) > 253 ||
		!utf8.ValidString(b.RequesterUID) || utf8.RuneCountInString(b.RequesterUID) > 253 ||
		!strings.HasPrefix(b.SourceDigest, "sha256:") || !lowerHex(strings.TrimPrefix(b.SourceDigest, "sha256:"), 64) ||
		!safeEffectID(b.WorkerPoolID, maxWorkerPoolIDSize) {
		return ErrInvalid
	}
	requesterHash := sha256.Sum256([]byte(b.RequesterName))
	if hex.EncodeToString(requesterHash[:]) != b.RequesterHash {
		return ErrInvalid
	}
	return nil
}

// ReceiptLink pins an earlier independently observed immutable receipt. A
// matching name without its original UID and data digest is not a match.
type ReceiptLink struct {
	Name       string `json:"name"`
	UID        string `json:"uid"`
	DataDigest string `json:"dataDigest"`
}

func (l ReceiptLink) validate(prefix string) error {
	if !strings.HasPrefix(l.Name, prefix) || !lowerHex(strings.TrimPrefix(l.Name, prefix), 32) ||
		!opaqueUID(l.UID) || !strings.HasPrefix(l.DataDigest, "sha256:") ||
		!lowerHex(strings.TrimPrefix(l.DataDigest, "sha256:"), 64) {
		return ErrInvalid
	}
	return nil
}

// GrantIntent is recorded after an exact active-ledger CAS and before any Pod
// Create. Future integration must pin GrantNonce in that CAS and demonstrate
// the original CR/status and allocated capacity are still live. This receipt
// alone never authorizes Pod creation or releases queue capacity.
type GrantIntent struct {
	Build                PinnedBuild  `json:"build"`
	AdmissionMode        string       `json:"admissionMode"` // queued or direct
	QueueReceipt         *ReceiptLink `json:"queueReceipt,omitempty"`
	GrantNonce           string       `json:"grantNonce"`
	ActiveLedgerFence    uint64       `json:"activeLedgerFence"`
	ActiveLedgerRV       string       `json:"activeLedgerResourceVersion"`
	AllocatedWorkerSlots int          `json:"allocatedWorkerSlots"`
}

func (g GrantIntent) validate() error {
	if err := g.Build.validate(); err != nil {
		return err
	}
	if !lowerHex(g.GrantNonce, 32) || g.ActiveLedgerFence == 0 ||
		!safeEffectID(g.ActiveLedgerRV, maxEffectIDBytes) ||
		g.AllocatedWorkerSlots < 1 || g.AllocatedWorkerSlots > 65535 {
		return ErrInvalid
	}
	switch g.AdmissionMode {
	case "queued":
		if g.QueueReceipt == nil || g.QueueReceipt.validate(queueReceiptPrefix) != nil {
			return ErrInvalid
		}
	case "direct":
		if g.QueueReceipt != nil {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	return boundedEffect(g)
}

// PodCreateIntent is recorded after the active ledger's one-use attempt nonce
// is committed, before the sole authorized Pod Create. It binds the exact
// deterministic Pod name and intended manifest/source/image digests. It is
// not evidence that a Pod exists or that its daemon executed a request.
type PodCreateIntent struct {
	Build             PinnedBuild `json:"build"`
	GrantReceipt      ReceiptLink `json:"grantReceipt"`
	PodName           string      `json:"podName"`
	PodAttemptNonce   string      `json:"podAttemptNonce"`
	PodTemplateDigest string      `json:"podTemplateDigest"`
	RunnerImageDigest string      `json:"runnerImageDigest"`
	BuildRequestID    string      `json:"buildRequestId"`
}

func (p PodCreateIntent) validate() error {
	if err := p.Build.validate(); err != nil {
		return err
	}
	if err := p.GrantReceipt.validate(grantReceiptPrefix); err != nil {
		return err
	}
	if len(validation.IsDNS1123Subdomain(p.PodName)) != 0 ||
		!lowerHex(p.PodAttemptNonce, 32) || !digestSHA256(p.PodTemplateDigest) ||
		!digestSHA256(p.RunnerImageDigest) || p.BuildRequestID != p.Build.BuildUID {
		return ErrInvalid
	}
	return boundedEffect(p)
}

func digestSHA256(value string) bool {
	return strings.HasPrefix(value, "sha256:") && lowerHex(strings.TrimPrefix(value, "sha256:"), 64)
}

func safeEffectID(value string, max int) bool {
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

func boundedEffect(value any) error {
	raw, err := json.Marshal(value)
	if err != nil || len(raw) > maxEffectJSONBytes {
		return ErrInvalid
	}
	return nil
}

// EffectWitness pins one direct readback of an immutable ConfigMap. It does
// not prove that a Grant/PodCreate effect was issued or settled.
type EffectWitness struct {
	Kind            string
	ReceiptName     string
	ReceiptUID      string
	ResourceVersion string
	DataDigest      string
}

func effectData(kind string, value any) (map[string]string, error) {
	raw, err := json.Marshal(value)
	if err != nil || len(raw) > maxEffectJSONBytes {
		return nil, ErrInvalid
	}
	return map[string]string{"version": effectVersion, "kind": kind, "payload": string(raw)}, nil
}

func newEffectConfigMap(namespace, name, kind string, value any) (*corev1.ConfigMap, error) {
	data, err := effectData(kind, value)
	if err != nil {
		return nil, err
	}
	immutable := true
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
			Labels:    map[string]string{effectReceiptLabel: kind},
		},
		Immutable: &immutable,
		Data:      data,
	}, nil
}

func NewGrantConfigMap(g GrantIntent) (*corev1.ConfigMap, error) {
	if err := g.validate(); err != nil {
		return nil, err
	}
	return newEffectConfigMap(g.Build.ReceiptNamespace, grantReceiptPrefix+g.GrantNonce, "grant", g)
}

func NewPodCreateConfigMap(p PodCreateIntent) (*corev1.ConfigMap, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return newEffectConfigMap(p.Build.ReceiptNamespace, podReceiptPrefix+p.PodAttemptNonce, "pod-create", p)
}

// DigestData is a deterministic digest of the complete ConfigMap Data map,
// including version and kind. It deliberately excludes mutable metadata.
func DigestData(data map[string]string) (string, error) {
	if len(data) == 0 {
		return "", ErrInvalid
	}
	raw, err := json.Marshal(data) // encoding/json sorts map keys.
	if err != nil || len(raw) > maxEffectJSONBytes*2 {
		return "", ErrInvalid
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func qualifyEffect(expected *corev1.ConfigMap, observed *corev1.ConfigMap, kind string) (EffectWitness, error) {
	if observed == nil || expected == nil || observed.Namespace != expected.Namespace || observed.Name != expected.Name ||
		!opaqueUID(string(observed.UID)) || observed.ResourceVersion == "" || observed.DeletionTimestamp != nil ||
		observed.Immutable == nil || !*observed.Immutable || len(observed.OwnerReferences) != 0 ||
		len(observed.BinaryData) != 0 || len(observed.Data) != len(expected.Data) ||
		len(observed.Labels) != 1 || observed.Labels[effectReceiptLabel] != kind {
		return EffectWitness{}, ErrChanged
	}
	for key, value := range expected.Data {
		if observed.Data[key] != value {
			return EffectWitness{}, ErrChanged
		}
	}
	digest, err := DigestData(observed.Data)
	if err != nil {
		return EffectWitness{}, err
	}
	return EffectWitness{Kind: kind, ReceiptName: observed.Name, ReceiptUID: string(observed.UID),
		ResourceVersion: observed.ResourceVersion, DataDigest: digest}, nil
}

func QualifyGrant(g GrantIntent, observed *corev1.ConfigMap) (EffectWitness, error) {
	expected, err := NewGrantConfigMap(g)
	if err != nil {
		return EffectWitness{}, err
	}
	return qualifyEffect(expected, observed, "grant")
}

func QualifyPodCreate(p PodCreateIntent, observed *corev1.ConfigMap) (EffectWitness, error) {
	expected, err := NewPodCreateConfigMap(p)
	if err != nil {
		return EffectWitness{}, err
	}
	return qualifyEffect(expected, observed, "pod-create")
}

func recordEffectOnce(ctx context.Context, api ConfigMaps, expected *corev1.ConfigMap, kind string) (EffectWitness, error) {
	if api == nil {
		return EffectWitness{}, fmt.Errorf("%w: direct ConfigMap API is missing", ErrInvalid)
	}
	// Keep the expected payload separate from any transport/client mutation.
	created, createErr := api.Create(ctx, expected.DeepCopy(), metav1.CreateOptions{})
	readback, readErr := api.Get(ctx, expected.Name, metav1.GetOptions{})
	if readErr != nil {
		return EffectWitness{}, fmt.Errorf("%w: Create result %v; direct read: %v", ErrUnconfirmed, createErr, readErr)
	}
	observed, err := qualifyEffect(expected, readback, kind)
	if err != nil {
		return EffectWitness{}, err
	}
	if created != nil {
		createdWitness, err := qualifyEffect(expected, created, kind)
		if err != nil || createdWitness.ReceiptUID != observed.ReceiptUID {
			return EffectWitness{}, ErrChanged
		}
	} else if createErr == nil {
		return EffectWitness{}, ErrUnconfirmed
	}
	return observed, nil
}

// RecordGrantOnce/RecordPodCreateOnce perform one Create and one exact named
// readback. Even a matching pre-existing receipt is evidence only, never a
// second Pod Create permission or substitute for the fresh ledger CAS.
func RecordGrantOnce(ctx context.Context, api ConfigMaps, g GrantIntent) (EffectWitness, error) {
	expected, err := NewGrantConfigMap(g)
	if err != nil {
		return EffectWitness{}, err
	}
	return recordEffectOnce(ctx, api, expected, "grant")
}

func RecordPodCreateOnce(ctx context.Context, api ConfigMaps, p PodCreateIntent) (EffectWitness, error) {
	expected, err := NewPodCreateConfigMap(p)
	if err != nil {
		return EffectWitness{}, err
	}
	return recordEffectOnce(ctx, api, expected, "pod-create")
}

func observeEffect(ctx context.Context, api ConfigMaps, expected *corev1.ConfigMap, kind string) (EffectWitness, error) {
	if api == nil {
		return EffectWitness{}, fmt.Errorf("%w: direct ConfigMap API is missing", ErrInvalid)
	}
	readback, err := api.Get(ctx, expected.Name, metav1.GetOptions{})
	if err != nil {
		return EffectWitness{}, fmt.Errorf("%w: direct read: %v", ErrUnconfirmed, err)
	}
	return qualifyEffect(expected, readback, kind)
}

// ObserveGrant/ObservePodCreate perform exact named readback after restart.
// NotFound or a read error is an unknown outcome, never no-effect evidence.
func ObserveGrant(ctx context.Context, api ConfigMaps, g GrantIntent) (EffectWitness, error) {
	expected, err := NewGrantConfigMap(g)
	if err != nil {
		return EffectWitness{}, err
	}
	return observeEffect(ctx, api, expected, "grant")
}

func ObservePodCreate(ctx context.Context, api ConfigMaps, p PodCreateIntent) (EffectWitness, error) {
	expected, err := NewPodCreateConfigMap(p)
	if err != nil {
		return EffectWitness{}, err
	}
	return observeEffect(ctx, api, expected, "pod-create")
}
