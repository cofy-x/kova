package admissiongenesis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/cofy-x/kova/internal/admissioncontract"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

var (
	ErrUncommitted    = errors.New("admission Genesis is not committed")
	ErrChanged        = errors.New("original admission installation changed")
	ErrUnknownCreate  = errors.New("admission ledger Create outcome is unknown")
	ErrUnknownPin     = errors.New("admission Genesis provisional pin outcome is unknown")
	ErrUnknownCommit  = errors.New("admission Genesis commit outcome is unknown")
	errPeerCommitted  = errors.New("admission Genesis was committed by a peer")
	errNonemptyLedger = errors.New("admission ledger is not canonical empty state")
)

// CoreAPI exposes only direct named API operations. A cache, List, or Watch is
// not an implementation of this interface and cannot provide bootstrap facts.
type CoreAPI interface {
	GetNamespace(context.Context, string) (*corev1.Namespace, error)
	GetConfigMap(context.Context, string, string) (*corev1.ConfigMap, error)
	CreateConfigMap(context.Context, string, *corev1.ConfigMap) (*corev1.ConfigMap, error)
	PatchConfigMap(context.Context, string, string, []byte) (*corev1.ConfigMap, error)
}

// DirectClient uses named typed client-go API reads/writes, not a cache or
// resourceVersion=0. Runtime wiring supplies a one-attempt mutation transport;
// an arbitrary clientset may replay POST/PATCH after an unknown response.
type DirectClient struct{ Client kubernetes.Interface }

func (c DirectClient) GetNamespace(ctx context.Context, name string) (*corev1.Namespace, error) {
	if c.Client == nil {
		return nil, fmt.Errorf("admission direct client is missing")
	}
	return c.Client.CoreV1().Namespaces().Get(ctx, name, metav1.GetOptions{})
}

func (c DirectClient) GetConfigMap(ctx context.Context, namespace, name string) (*corev1.ConfigMap, error) {
	if c.Client == nil {
		return nil, fmt.Errorf("admission direct client is missing")
	}
	return c.Client.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
}

func (c DirectClient) GetSecret(ctx context.Context, namespace, name string) (*corev1.Secret, error) {
	if c.Client == nil {
		return nil, fmt.Errorf("admission direct client is missing")
	}
	return c.Client.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
}

func (c DirectClient) CreateConfigMap(ctx context.Context, namespace string, cm *corev1.ConfigMap) (*corev1.ConfigMap, error) {
	if c.Client == nil {
		return nil, fmt.Errorf("admission direct client is missing")
	}
	return c.Client.CoreV1().ConfigMaps(namespace).Create(ctx, cm, metav1.CreateOptions{})
}

func (c DirectClient) PatchConfigMap(ctx context.Context, namespace, name string, body []byte) (*corev1.ConfigMap, error) {
	if c.Client == nil {
		return nil, fmt.Errorf("admission direct client is missing")
	}
	return c.Client.CoreV1().ConfigMaps(namespace).Patch(ctx, name, types.JSONPatchType, body, metav1.PatchOptions{})
}

type LedgerTemplate struct {
	Role      admissioncontract.Role
	DataKey   string
	EmptyData string
	Validate  func(*corev1.ConfigMap) error
}

type Binding struct {
	NamespaceUID    string
	GenesisUID      string
	ActiveLedgerUID string
	QueueLedgerUID  string
}

// Preflight is a required one-way veto for visible old CRs and Pods; an
// empty List is never the source of initialization authority.
type Bootstrapper struct {
	API       CoreAPI
	Receipt   admissioncontract.Receipt
	Active    LedgerTemplate
	Queue     LedgerTemplate
	Preflight func(context.Context) error
	// BeforeEffect rechecks the external receipt before each fresh-install
	// ConfigMap Create or Genesis pin/commit Patch. A runtime caller must set it;
	// deterministic core-protocol fixtures may use nil.
	BeforeEffect func(context.Context) error
}

func (b Bootstrapper) validate() error {
	if b.API == nil || b.Preflight == nil {
		return fmt.Errorf("admission bootstrap lacks direct API or old-work veto")
	}
	if err := b.Receipt.Validate(); err != nil {
		return err
	}
	for _, template := range []LedgerTemplate{b.Active, b.Queue} {
		if (template.Role != admissioncontract.Active && template.Role != admissioncontract.Queue) || template.Validate == nil ||
			template.DataKey == "" || template.EmptyData == "" || len(template.EmptyData) > admissioncontract.MaxLedgerDataBytes {
			return fmt.Errorf("admission bootstrap has invalid ledger template")
		}
	}
	if b.Active.Role != admissioncontract.Active || b.Queue.Role != admissioncontract.Queue ||
		b.Active.DataKey != admissioncontract.ActiveLedgerDataKey || b.Queue.DataKey != admissioncontract.QueueLedgerDataKey {
		return fmt.Errorf("admission bootstrap ledger roles or data keys are invalid")
	}
	// Validate both canonical request bodies before either role can Create or
	// pin. A broken second template must not strand the first ledger.
	for _, template := range []LedgerTemplate{b.Active, b.Queue} {
		request, err := b.Receipt.NewLedgerObject(template.Role, template.DataKey, template.EmptyData,
			"00000000000000000000000000000000")
		if err != nil {
			return fmt.Errorf("%s admission ledger template: %w", template.Role, err)
		}
		if err := validateCanonicalEmptyLimits(template, b.Receipt.Contract.Limits); err != nil {
			return err
		}
		if err := template.Validate(request); err != nil {
			return fmt.Errorf("%s admission ledger canonical template: %w", template.Role, err)
		}
	}
	return nil
}

func (b Bootstrapper) getOriginal(ctx context.Context) (*corev1.ConfigMap, admissioncontract.GenesisData, error) {
	ns, err := b.API.GetNamespace(ctx, b.Receipt.Namespace)
	if err != nil {
		return nil, admissioncontract.GenesisData{}, err
	}
	if err := b.Receipt.QualifyNamespace(ns); err != nil {
		return nil, admissioncontract.GenesisData{}, err
	}
	receipts, err := b.API.GetNamespace(ctx, b.Receipt.Contract.ReceiptNamespace)
	if err != nil {
		return nil, admissioncontract.GenesisData{}, err
	}
	if err := b.Receipt.QualifyReceiptNamespace(receipts); err != nil {
		return nil, admissioncontract.GenesisData{}, err
	}
	cm, err := b.API.GetConfigMap(ctx, b.Receipt.Namespace, b.Receipt.GenesisName)
	if err != nil {
		return nil, admissioncontract.GenesisData{}, err
	}
	state, err := b.Receipt.QualifyGenesis(cm)
	if err != nil {
		return nil, admissioncontract.GenesisData{}, err
	}
	return cm.DeepCopy(), state, nil
}

func (b Bootstrapper) readLedger(ctx context.Context, template LedgerTemplate, pinnedUID string, empty bool) (*corev1.ConfigMap, error) {
	name, _, _ := b.Receipt.RoleFacts(template.Role)
	cm, err := b.API.GetConfigMap(ctx, b.Receipt.Namespace, name)
	if apierrors.IsNotFound(err) && pinnedUID == "" {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := b.Receipt.QualifyLedgerIdentity(cm, template.Role); err != nil {
		return nil, err
	}
	if pinnedUID != "" && string(cm.UID) != pinnedUID {
		return nil, fmt.Errorf("%w: %s ledger UID changed", ErrChanged, template.Role)
	}
	if err := template.Validate(cm); err != nil {
		return nil, err
	}
	if empty && (len(cm.Data) != 1 || len(cm.BinaryData) != 0 || cm.Data[template.DataKey] != template.EmptyData) {
		return nil, fmt.Errorf("%w: %w: %s ledger", ErrChanged, errNonemptyLedger, template.Role)
	}
	return cm.DeepCopy(), nil
}

// Only a schema-valid ledger that changed from canonical empty to nonempty
// can indicate that a peer committed and admitted work in this read race.
// The full committed pair must still qualify. Never call this for Preflight.
func (b Bootstrapper) resolveNonemptyReadRace(ctx context.Context, readErr error) error {
	if !errors.Is(readErr, errNonemptyLedger) {
		return readErr
	}
	if _, err := b.ObserveCommitted(ctx); err == nil {
		return errPeerCommitted
	} else {
		return fmt.Errorf("%w: committed peer requalification failed: %v", readErr, err)
	}
}

func pinnedUID(state admissioncontract.GenesisData, role admissioncontract.Role) string {
	if role == admissioncontract.Active {
		return state.ActiveLedgerUID
	}
	return state.QueueLedgerUID
}

func setPinnedUID(state *admissioncontract.GenesisData, role admissioncontract.Role, uid string) {
	if role == admissioncontract.Active {
		state.ActiveLedgerUID = uid
	} else {
		state.QueueLedgerUID = uid
	}
}

func randomAttempt() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

type patchOp struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value"`
}

func genesisPatch(original *corev1.ConfigMap, next admissioncontract.GenesisData, commit bool) ([]byte, error) {
	if original == nil || original.UID == "" || original.ResourceVersion == "" {
		return nil, fmt.Errorf("Genesis CAS lacks original UID or resourceVersion")
	}
	data, err := json.Marshal(next)
	if err != nil || len(data) > admissioncontract.MaxContractJSONBytes {
		return nil, fmt.Errorf("Genesis CAS proposal exceeds contract")
	}
	ops := []patchOp{
		{Op: "test", Path: "/metadata/uid", Value: string(original.UID)},
		{Op: "test", Path: "/metadata/resourceVersion", Value: original.ResourceVersion},
		{Op: "test", Path: "/data/genesis.json", Value: original.Data[admissioncontract.GenesisDataKey]},
	}
	if original.Immutable != nil {
		if *original.Immutable {
			return nil, fmt.Errorf("Genesis CAS cannot update immutable object")
		}
		ops = append(ops, patchOp{Op: "test", Path: "/immutable", Value: false})
	}
	ops = append(ops, patchOp{Op: "replace", Path: "/data/genesis.json", Value: string(data)})
	if commit {
		// RFC6902 add works for both an absent member and an explicit false
		// member. The UID/RV/data tests protect either original representation.
		ops = append(ops, patchOp{Op: "add", Path: "/immutable", Value: true})
	}
	return json.Marshal(ops)
}

// ObserveCommitted is read-only. Its two Genesis reads detect some drift but
// do not form an atomic snapshot or authorize a later side effect.
func (b Bootstrapper) ObserveCommitted(ctx context.Context) (Binding, error) {
	if err := b.validate(); err != nil {
		return Binding{}, err
	}
	first, state, err := b.getOriginal(ctx)
	if err != nil {
		return Binding{}, err
	}
	if state.Phase != admissioncontract.PhaseCommitted {
		return Binding{}, ErrUncommitted
	}
	if _, err := b.readLedger(ctx, b.Active, state.ActiveLedgerUID, false); err != nil {
		return Binding{}, err
	}
	if _, err := b.readLedger(ctx, b.Queue, state.QueueLedgerUID, false); err != nil {
		return Binding{}, err
	}
	last, later, err := b.getOriginal(ctx)
	if err != nil {
		return Binding{}, err
	}
	if later != state || last.ResourceVersion != first.ResourceVersion {
		return Binding{}, fmt.Errorf("%w: Genesis changed during committed observation", ErrChanged)
	}
	return Binding{NamespaceUID: b.Receipt.Contract.NamespaceUID, GenesisUID: b.Receipt.GenesisUID,
		ActiveLedgerUID: state.ActiveLedgerUID, QueueLedgerUID: state.QueueLedgerUID}, nil
}

func (b Bootstrapper) ensureRole(ctx context.Context, template LedgerTemplate) error {
	_, state, err := b.getOriginal(ctx)
	if err != nil {
		return err
	}
	if state.Phase == admissioncontract.PhaseCommitted {
		return errPeerCommitted
	}
	currentPin := pinnedUID(state, template.Role)
	ledger, err := b.readLedger(ctx, template, currentPin, true)
	if err != nil {
		return b.resolveNonemptyReadRace(ctx, err)
	}
	if ledger == nil {
		if currentPin != "" {
			return fmt.Errorf("%w: pinned %s ledger disappeared", ErrChanged, template.Role)
		}
		if err := b.Preflight(ctx); err != nil {
			return err
		}
		// A new Genesis read narrows the missing-object/Create race. It is not
		// a lease: a later peer Create can still win this exact name.
		_, latest, err := b.getOriginal(ctx)
		if err != nil {
			return err
		}
		if latest.Phase == admissioncontract.PhaseCommitted {
			return errPeerCommitted
		}
		if pin := pinnedUID(latest, template.Role); pin != "" {
			// A peer advanced Genesis after our missing-object read. Honor its
			// one-way UID pin directly; recursive Create attempts are unnecessary.
			_, err := b.readLedger(ctx, template, pin, true)
			return b.resolveNonemptyReadRace(ctx, err)
		}
		attempt, err := randomAttempt()
		if err != nil {
			return err
		}
		request, err := b.Receipt.NewLedgerObject(template.Role, template.DataKey, template.EmptyData, attempt)
		if err != nil {
			return err
		}
		if b.BeforeEffect != nil {
			if err := b.BeforeEffect(ctx); err != nil {
				return err
			}
		}
		created, createErr := b.API.CreateConfigMap(ctx, b.Receipt.Namespace, request)
		observed, readErr := b.readLedger(ctx, template, "", true)
		if readErr != nil {
			resolved := b.resolveNonemptyReadRace(ctx, readErr)
			if errors.Is(resolved, errPeerCommitted) {
				return resolved
			}
			return fmt.Errorf("%w: %s ledger Create/read: %v", ErrUnknownCreate, template.Role, resolved)
		}
		if observed == nil {
			return fmt.Errorf("%w: %s ledger absent after Create; original attempt may arrive later: %v", ErrUnknownCreate, template.Role, createErr)
		}
		if createErr == nil && created != nil && created.UID != "" && created.UID != observed.UID {
			return fmt.Errorf("%w: %s ledger UID changed after Create", ErrChanged, template.Role)
		}
		ledger = observed
	}
	return b.pinRole(ctx, template, string(ledger.UID))
}

func (b Bootstrapper) pinRole(ctx context.Context, template LedgerTemplate, uid string) error {
	original, state, err := b.getOriginal(ctx)
	if err != nil {
		return err
	}
	if state.Phase == admissioncontract.PhaseCommitted {
		if pinnedUID(state, template.Role) != uid {
			return fmt.Errorf("%w: committed %s ledger UID differs", ErrChanged, template.Role)
		}
		return errPeerCommitted
	}
	if pin := pinnedUID(state, template.Role); pin != "" {
		if pin != uid {
			return fmt.Errorf("%w: provisional %s ledger UID differs", ErrChanged, template.Role)
		}
		return nil
	}
	ledger, err := b.readLedger(ctx, template, "", true)
	if err != nil {
		resolved := b.resolveNonemptyReadRace(ctx, err)
		if errors.Is(resolved, errPeerCommitted) {
			return resolved
		}
		err = resolved
	}
	if err != nil || ledger == nil || string(ledger.UID) != uid {
		return fmt.Errorf("%w: %s ledger changed before provisional pin: %v", ErrChanged, template.Role, err)
	}
	proposal := state
	setPinnedUID(&proposal, template.Role, uid)
	patch, err := genesisPatch(original, proposal, false)
	if err != nil {
		return err
	}
	if b.BeforeEffect != nil {
		if err := b.BeforeEffect(ctx); err != nil {
			return err
		}
	}
	_, patchErr := b.API.PatchConfigMap(ctx, b.Receipt.Namespace, b.Receipt.GenesisName, patch)
	_, observed, readErr := b.getOriginal(ctx)
	if readErr != nil {
		return fmt.Errorf("%w: %s pin readback failed: %v", ErrUnknownPin, template.Role, readErr)
	}
	if pin := pinnedUID(observed, template.Role); pin != "" && pin != uid {
		return fmt.Errorf("%w: peer pinned a different %s ledger UID", ErrChanged, template.Role)
	}
	if pinnedUID(observed, template.Role) != uid {
		return fmt.Errorf("%w: %s pin is not yet observed: %v", ErrUnknownPin, template.Role, patchErr)
	}
	verified, err := b.readLedger(ctx, template, uid, observed.Phase == admissioncontract.PhaseInitializing)
	if err != nil {
		resolved := b.resolveNonemptyReadRace(ctx, err)
		if errors.Is(resolved, errPeerCommitted) {
			return resolved
		}
		err = resolved
	}
	if err != nil || verified == nil {
		return fmt.Errorf("%w: %s pinned ledger changed: %v", ErrChanged, template.Role, err)
	}
	if observed.Phase == admissioncontract.PhaseCommitted {
		return errPeerCommitted
	}
	return nil
}

func (b Bootstrapper) commit(ctx context.Context) (Binding, error) {
	original, state, err := b.getOriginal(ctx)
	if err != nil {
		return Binding{}, err
	}
	if state.Phase == admissioncontract.PhaseCommitted {
		return b.ObserveCommitted(ctx)
	}
	if state.ActiveLedgerUID == "" || state.QueueLedgerUID == "" {
		return Binding{}, fmt.Errorf("%w: both provisional ledger UIDs are required", ErrChanged)
	}
	if err := b.Preflight(ctx); err != nil {
		return Binding{}, err
	}
	for _, template := range []LedgerTemplate{b.Active, b.Queue} {
		ledger, err := b.readLedger(ctx, template, pinnedUID(state, template.Role), true)
		if err != nil {
			resolved := b.resolveNonemptyReadRace(ctx, err)
			if errors.Is(resolved, errPeerCommitted) {
				return b.ObserveCommitted(ctx)
			}
			err = resolved
		}
		if err != nil || ledger == nil {
			return Binding{}, fmt.Errorf("%w: pinned %s ledger changed before commit: %v", ErrChanged, template.Role, err)
		}
	}
	proposal := state
	proposal.Phase = admissioncontract.PhaseCommitted
	patch, err := genesisPatch(original, proposal, true)
	if err != nil {
		return Binding{}, err
	}
	if b.BeforeEffect != nil {
		if err := b.BeforeEffect(ctx); err != nil {
			return Binding{}, err
		}
	}
	_, patchErr := b.API.PatchConfigMap(ctx, b.Receipt.Namespace, b.Receipt.GenesisName, patch)
	binding, observeErr := b.ObserveCommitted(ctx)
	if observeErr == nil {
		if binding.ActiveLedgerUID != state.ActiveLedgerUID || binding.QueueLedgerUID != state.QueueLedgerUID {
			return Binding{}, fmt.Errorf("%w: committed ledger pair differs from provisional pins", ErrChanged)
		}
		return binding, nil
	}
	if patchErr != nil {
		return Binding{}, fmt.Errorf("%w: response=%v, readback=%v", ErrUnknownCommit, patchErr, observeErr)
	}
	return Binding{}, fmt.Errorf("%w: successful response did not produce qualified commit: %v", ErrUnknownCommit, observeErr)
}

// EnsureFresh may finish only the receipt's original, externally authorized
// Initializing generation. No missing committed ledger is ever recreated.
func (b Bootstrapper) EnsureFresh(ctx context.Context) (Binding, error) {
	if err := b.validate(); err != nil {
		return Binding{}, err
	}
	_, state, err := b.getOriginal(ctx)
	if err != nil {
		return Binding{}, err
	}
	if state.Phase == admissioncontract.PhaseCommitted {
		return b.ObserveCommitted(ctx)
	}
	if err := b.Preflight(ctx); err != nil {
		return Binding{}, err
	}
	for _, template := range []LedgerTemplate{b.Active, b.Queue} {
		if err := b.ensureRole(ctx, template); err != nil {
			if errors.Is(err, errPeerCommitted) {
				return b.ObserveCommitted(ctx)
			}
			return Binding{}, err
		}
	}
	return b.commit(ctx)
}
