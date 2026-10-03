// Package admissioncontract models an externally provisioned admission epoch.
// It contains immutable identity and pure validation, not runtime writers.
package admissioncontract

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/cofy-x/kova/internal/admissionjson"
	"github.com/google/go-containerregistry/pkg/name"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	GenesisName          = "kova-service-admission-genesis"
	ActiveLedgerName     = "kova-service-admission"
	QueueLedgerName      = "kova-service-queue-admission"
	ActiveLedgerDataKey  = "reservations.json"
	QueueLedgerDataKey   = "queue.json"
	GenesisDataKey       = "genesis.json"
	PhaseInitializing    = "Initializing"
	PhaseCommitted       = "Committed"
	MaxContractJSONBytes = 8192
	maxOpaqueUIDBytes    = 256
	MaxLedgerDataBytes   = 768 * 1024
	annotationPrefix     = "kova.cofy.dev/"
	annotationNamespace  = annotationPrefix + "admission-namespace-uid"
	annotationGenesis    = annotationPrefix + "admission-genesis-uid"
	annotationGeneration = annotationPrefix + "admission-generation"
	annotationContract   = annotationPrefix + "admission-contract"
	annotationRole       = annotationPrefix + "admission-role"
	annotationSchema     = annotationPrefix + "admission-schema"
	annotationAttempt    = annotationPrefix + "admission-bootstrap"
)

type Limits struct {
	MaxActiveJobs             int `json:"maxActiveJobs"`
	MaxActiveJobsPerRequester int `json:"maxActiveJobsPerRequester"`
	WorkerSlots               int `json:"workerSlots"`
	MaxQueuedJobs             int `json:"maxQueuedJobs"`
	MaxQueuedJobsPerRequester int `json:"maxQueuedJobsPerRequester"`
}

type Contract struct {
	Version             int    `json:"version"`
	NamespaceUID        string `json:"namespaceUID"`
	ReceiptNamespace    string `json:"receiptNamespace"`
	ReceiptNamespaceUID string `json:"receiptNamespaceUID"`
	WorkerPoolID        string `json:"workerPoolID"`
	RunnerImage         string `json:"runnerImage"`
	Generation          string `json:"generation"`
	ActiveLedgerName    string `json:"activeLedgerName"`
	ActiveLedgerSchema  int    `json:"activeLedgerSchema"`
	QueueLedgerName     string `json:"queueLedgerName"`
	QueueLedgerSchema   int    `json:"queueLedgerSchema"`
	Limits              Limits `json:"limits"`
}

type Receipt struct {
	Namespace   string   `json:"namespace"`
	GenesisName string   `json:"genesisName"`
	GenesisUID  string   `json:"genesisUID"`
	Contract    Contract `json:"contract"`
}

// GenesisData lives in the externally created ConfigMap. Initializing role
// UIDs are monotonic provisional pins; only Committed grants runtime authority.
type GenesisData struct {
	Contract        Contract `json:"contract"`
	Phase           string   `json:"phase"`
	ActiveLedgerUID string   `json:"activeLedgerUID"`
	QueueLedgerUID  string   `json:"queueLedgerUID"`
}

type Role string

const (
	Active Role = "active"
	Queue  Role = "queue"
)

func ValidUID(uid string) bool {
	return uid != "" && len(uid) <= maxOpaqueUIDBytes && utf8.ValidString(uid)
}

func validNonce(value string) bool {
	if len(value) != 32 {
		return false
	}
	for _, c := range value {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func (l Limits) validate() error {
	// Match the bounded #58 two-ledger policy. Runtime configuration must also
	// pass the existing store validators; this is the receipt's early refusal.
	if l.MaxActiveJobs < 1 || l.MaxActiveJobs > 128 ||
		l.MaxActiveJobsPerRequester < 1 || l.MaxActiveJobsPerRequester > l.MaxActiveJobs ||
		l.WorkerSlots < 1 || l.WorkerSlots > 65535 ||
		l.MaxQueuedJobs < 1 || l.MaxQueuedJobs > 1000 ||
		l.MaxQueuedJobsPerRequester < 1 || l.MaxQueuedJobsPerRequester > l.MaxQueuedJobs ||
		8192+6000*l.MaxActiveJobs > MaxLedgerDataBytes ||
		4096+736*l.MaxQueuedJobs > MaxLedgerDataBytes {
		return fmt.Errorf("admission receipt has unsupported limits")
	}
	return nil
}

func (c Contract) validate() error {
	if c.Version != 3 || !ValidUID(c.NamespaceUID) ||
		len(validation.IsDNS1123Label(c.ReceiptNamespace)) != 0 || !ValidUID(c.ReceiptNamespaceUID) ||
		!ValidWorkerPoolID(c.WorkerPoolID) ||
		!validNonce(c.Generation) ||
		c.ActiveLedgerName != ActiveLedgerName || c.ActiveLedgerSchema != 2 ||
		c.QueueLedgerName != QueueLedgerName || c.QueueLedgerSchema != 2 {
		return fmt.Errorf("admission receipt has unsupported contract")
	}
	if _, err := RunnerManifestDigest(c.RunnerImage); err != nil {
		return err
	}
	return c.Limits.validate()
}

// ValidWorkerPoolID accepts an externally assigned identity for the entire
// worker capacity of this epoch. It is not derived from a mutable Service
// address and does not prove that any physical worker has been retired.
func ValidWorkerPoolID(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, r := range value {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || strings.ContainsRune("._:/@+-", r)) {
			return false
		}
	}
	return true
}

// RunnerManifestDigest accepts only a fully qualified digest reference. The
// digest identifies the OCI manifest or index selected for the Pod, never a
// Docker image configuration ID. Resolution/publishing belongs to the caller.
func RunnerManifestDigest(value string) (string, error) {
	ref, err := name.NewDigest(value, name.StrictValidation)
	if err != nil || len(value) > 512 || ref.Name() != value {
		return "", fmt.Errorf("runner image must be a canonical repository@sha256 manifest reference")
	}
	digest := ref.DigestStr()
	if len(digest) != 71 || !strings.HasPrefix(digest, "sha256:") {
		return "", fmt.Errorf("runner image must use a sha256 manifest digest")
	}
	for _, r := range strings.TrimPrefix(digest, "sha256:") {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return "", fmt.Errorf("runner manifest digest must use lowercase hexadecimal")
		}
	}
	return digest, nil
}

func (r Receipt) Validate() error {
	if len(validation.IsDNS1123Label(r.Namespace)) != 0 || r.GenesisName != GenesisName ||
		r.Namespace == r.Contract.ReceiptNamespace || !ValidUID(r.GenesisUID) {
		return fmt.Errorf("admission receipt has invalid original identity")
	}
	return r.Contract.validate()
}

func (r Receipt) ContractDigest() (string, error) {
	if err := r.Validate(); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(r.Contract)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func allowContract(path []string, key string) bool {
	switch len(path) {
	case 0:
		return key == "namespace" || key == "genesisName" || key == "genesisUID" || key == "contract" ||
			key == "phase" || key == "activeLedgerUID" || key == "queueLedgerUID"
	case 1:
		if path[0] == "contract" {
			switch key {
			case "version", "namespaceUID", "receiptNamespace", "receiptNamespaceUID", "workerPoolID", "runnerImage", "generation", "activeLedgerName", "activeLedgerSchema", "queueLedgerName", "queueLedgerSchema", "limits":
				return true
			}
		}
	case 2:
		if path[0] == "contract" && path[1] == "limits" {
			switch key {
			case "maxActiveJobs", "maxActiveJobsPerRequester", "workerSlots", "maxQueuedJobs", "maxQueuedJobsPerRequester":
				return true
			}
		}
	}
	return false
}

func RequireKeys(raw []byte, keys ...string) error {
	var values map[string]json.RawMessage
	if err := json.Unmarshal(raw, &values); err != nil {
		return err
	}
	if len(values) != len(keys) {
		return fmt.Errorf("admission contract has missing or extra fields")
	}
	for _, key := range keys {
		if _, ok := values[key]; !ok {
			return fmt.Errorf("admission contract has missing field")
		}
	}
	return nil
}

func requireContractKeys(raw []byte) error {
	if err := RequireKeys(raw, "version", "namespaceUID", "receiptNamespace", "receiptNamespaceUID", "workerPoolID", "runnerImage", "generation", "activeLedgerName", "activeLedgerSchema", "queueLedgerName", "queueLedgerSchema", "limits"); err != nil {
		return err
	}
	var values map[string]json.RawMessage
	if err := json.Unmarshal(raw, &values); err != nil {
		return err
	}
	return RequireKeys(values["limits"], "maxActiveJobs", "maxActiveJobsPerRequester", "workerSlots", "maxQueuedJobs", "maxQueuedJobsPerRequester")
}

func ParseReceipt(raw []byte) (Receipt, error) {
	if len(raw) == 0 || len(raw) > MaxContractJSONBytes {
		return Receipt{}, fmt.Errorf("admission receipt exceeds size limit")
	}
	var r Receipt
	if err := admissionjson.Decode(raw, &r, allowContract); err != nil {
		return Receipt{}, err
	}
	if err := RequireKeys(raw, "namespace", "genesisName", "genesisUID", "contract"); err != nil {
		return Receipt{}, err
	}
	var values map[string]json.RawMessage
	_ = json.Unmarshal(raw, &values)
	if err := requireContractKeys(values["contract"]); err != nil {
		return Receipt{}, err
	}
	if err := r.Validate(); err != nil {
		return Receipt{}, err
	}
	return r, nil
}

func decodeGenesis(raw []byte, r Receipt) (GenesisData, error) {
	if len(raw) == 0 || len(raw) > MaxContractJSONBytes {
		return GenesisData{}, fmt.Errorf("admission Genesis exceeds size limit")
	}
	var state GenesisData
	if err := admissionjson.Decode(raw, &state, allowContract); err != nil {
		return GenesisData{}, err
	}
	if err := RequireKeys(raw, "contract", "phase", "activeLedgerUID", "queueLedgerUID"); err != nil {
		return GenesisData{}, err
	}
	var values map[string]json.RawMessage
	_ = json.Unmarshal(raw, &values)
	if err := requireContractKeys(values["contract"]); err != nil {
		return GenesisData{}, err
	}
	if state.Contract != r.Contract || (state.Phase != PhaseInitializing && state.Phase != PhaseCommitted) ||
		(state.ActiveLedgerUID != "" && !ValidUID(state.ActiveLedgerUID)) ||
		(state.QueueLedgerUID != "" && !ValidUID(state.QueueLedgerUID)) ||
		(state.Phase == PhaseCommitted && (state.ActiveLedgerUID == "" || state.QueueLedgerUID == "")) {
		return GenesisData{}, fmt.Errorf("admission Genesis has invalid contract or phase")
	}
	return state, nil
}

func (r Receipt) QualifyNamespace(ns *corev1.Namespace) error {
	if err := r.Validate(); err != nil {
		return err
	}
	if ns == nil || ns.Name != r.Namespace || string(ns.UID) != r.Contract.NamespaceUID ||
		ns.DeletionTimestamp != nil || ns.Status.Phase != corev1.NamespaceActive {
		return fmt.Errorf("original admission Namespace is unavailable or changed")
	}
	return nil
}

// QualifyReceiptNamespace pins the independent, receipt-only failure domain.
// Same-name Namespace recreation cannot become authority for a new receipt,
// readback, or cleanup of the original admission epoch.
func (r Receipt) QualifyReceiptNamespace(ns *corev1.Namespace) error {
	if err := r.Validate(); err != nil {
		return err
	}
	if ns == nil || ns.Name != r.Contract.ReceiptNamespace ||
		string(ns.UID) != r.Contract.ReceiptNamespaceUID || ns.DeletionTimestamp != nil ||
		ns.Status.Phase != corev1.NamespaceActive {
		return fmt.Errorf("original recovery receipt Namespace is unavailable or changed")
	}
	return nil
}

func (r Receipt) QualifyGenesis(cm *corev1.ConfigMap) (GenesisData, error) {
	if err := r.Validate(); err != nil {
		return GenesisData{}, err
	}
	if cm == nil || cm.Namespace != r.Namespace || cm.Name != r.GenesisName || string(cm.UID) != r.GenesisUID ||
		cm.ResourceVersion == "" || cm.DeletionTimestamp != nil || len(cm.Data) != 1 || len(cm.BinaryData) != 0 {
		return GenesisData{}, fmt.Errorf("original admission Genesis is unavailable or changed")
	}
	state, err := decodeGenesis([]byte(cm.Data[GenesisDataKey]), r)
	if err != nil {
		return GenesisData{}, err
	}
	if (state.Phase == PhaseInitializing && cm.Immutable != nil && *cm.Immutable) ||
		(state.Phase == PhaseCommitted && (cm.Immutable == nil || !*cm.Immutable)) {
		return GenesisData{}, fmt.Errorf("admission Genesis immutable state disagrees with phase")
	}
	return state, nil
}

func (r Receipt) RoleFacts(role Role) (name string, schema int, err error) {
	switch role {
	case Active:
		return r.Contract.ActiveLedgerName, r.Contract.ActiveLedgerSchema, nil
	case Queue:
		return r.Contract.QueueLedgerName, r.Contract.QueueLedgerSchema, nil
	default:
		return "", 0, fmt.Errorf("invalid admission ledger role")
	}
}

func (r Receipt) ledgerAnnotations(role Role, attempt string) (map[string]string, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	_, schema, err := r.RoleFacts(role)
	if err != nil || !validNonce(attempt) {
		return nil, fmt.Errorf("invalid admission ledger creation attempt")
	}
	digest, err := r.ContractDigest()
	if err != nil {
		return nil, err
	}
	return map[string]string{
		annotationNamespace:  r.Contract.NamespaceUID,
		annotationGenesis:    r.GenesisUID,
		annotationGeneration: r.Contract.Generation,
		annotationContract:   digest,
		annotationRole:       string(role),
		annotationSchema:     fmt.Sprintf("%d", schema),
		annotationAttempt:    attempt,
	}, nil
}

// NewLedgerObject only constructs a request body; it never sends a Create.
func (r Receipt) NewLedgerObject(role Role, dataKey, emptyData, attempt string) (*corev1.ConfigMap, error) {
	name, _, err := r.RoleFacts(role)
	if err != nil || dataKey == "" || emptyData == "" || len(emptyData) > MaxLedgerDataBytes {
		return nil, fmt.Errorf("invalid admission ledger template")
	}
	annotations, err := r.ledgerAnnotations(role, attempt)
	if err != nil {
		return nil, err
	}
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: r.Namespace, Name: name, Annotations: annotations},
		Data:       map[string]string{dataKey: emptyData},
	}, nil
}

func (r Receipt) QualifyLedgerIdentity(cm *corev1.ConfigMap, role Role) error {
	name, schema, err := r.RoleFacts(role)
	if err != nil || r.Validate() != nil {
		return fmt.Errorf("invalid admission ledger receipt")
	}
	if cm == nil || cm.Namespace != r.Namespace || cm.Name != name || !ValidUID(string(cm.UID)) ||
		cm.ResourceVersion == "" || cm.DeletionTimestamp != nil || (cm.Immutable != nil && *cm.Immutable) ||
		len(cm.Annotations) != 7 {
		return fmt.Errorf("admission ledger original identity is unavailable or changed")
	}
	digest, _ := r.ContractDigest()
	if cm.Annotations[annotationNamespace] != r.Contract.NamespaceUID ||
		cm.Annotations[annotationGenesis] != r.GenesisUID ||
		cm.Annotations[annotationGeneration] != r.Contract.Generation ||
		cm.Annotations[annotationContract] != digest ||
		cm.Annotations[annotationRole] != string(role) ||
		cm.Annotations[annotationSchema] != fmt.Sprintf("%d", schema) ||
		!validNonce(cm.Annotations[annotationAttempt]) {
		return fmt.Errorf("admission ledger installation annotations differ from receipt")
	}
	return nil
}
