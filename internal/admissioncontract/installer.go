package admissioncontract

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

const ReceiptSecretDataKey = "receipt.json"

// InstallationSpec is supplied by the environment installer. The original
// Namespace UID and generation are inputs, never discovered and promoted to
// trusted identities by this helper.
type InstallationSpec struct {
	Namespace           string
	NamespaceUID        string
	ReceiptNamespace    string
	ReceiptNamespaceUID string
	Generation          string
	Limits              Limits
}

func (s InstallationSpec) Validate() error {
	_, err := s.contract()
	return err
}

func (s InstallationSpec) contract() (Contract, error) {
	if len(validation.IsDNS1123Label(s.Namespace)) != 0 ||
		len(validation.IsDNS1123Label(s.ReceiptNamespace)) != 0 ||
		s.Namespace == s.ReceiptNamespace {
		return Contract{}, fmt.Errorf("admission installation has invalid Namespace name")
	}
	contract := Contract{
		Version:             2,
		NamespaceUID:        s.NamespaceUID,
		ReceiptNamespace:    s.ReceiptNamespace,
		ReceiptNamespaceUID: s.ReceiptNamespaceUID,
		Generation:          s.Generation,
		ActiveLedgerName:    ActiveLedgerName,
		ActiveLedgerSchema:  1,
		QueueLedgerName:     QueueLedgerName,
		QueueLedgerSchema:   2,
		Limits:              s.Limits,
	}
	if err := contract.validate(); err != nil {
		return Contract{}, err
	}
	return contract, nil
}

func (s InstallationSpec) qualifyNamespace(ctx context.Context, reader Reader) error {
	if reader == nil {
		return fmt.Errorf("admission installation lacks a direct API reader")
	}
	ns, err := reader.GetNamespace(ctx, s.Namespace)
	if err != nil {
		return err
	}
	if ns == nil || ns.Name != s.Namespace || string(ns.UID) != s.NamespaceUID ||
		ns.DeletionTimestamp != nil || ns.Status.Phase != corev1.NamespaceActive {
		return fmt.Errorf("original admission Namespace is unavailable or changed")
	}
	receipts, err := reader.GetNamespace(ctx, s.ReceiptNamespace)
	if err != nil {
		return err
	}
	if receipts == nil || receipts.Name != s.ReceiptNamespace ||
		string(receipts.UID) != s.ReceiptNamespaceUID || receipts.DeletionTimestamp != nil ||
		receipts.Status.Phase != corev1.NamespaceActive {
		return fmt.Errorf("original recovery receipt Namespace is unavailable or changed")
	}
	return nil
}

// RenderInitializingGenesis produces a declarative ConfigMap for an external
// installer to apply. It checks the exact preexisting Namespace by a direct
// named read and never creates a Namespace, Genesis, ledger, or Secret.
func (s InstallationSpec) RenderInitializingGenesis(ctx context.Context, reader Reader) (*corev1.ConfigMap, error) {
	contract, err := s.contract()
	if err != nil {
		return nil, err
	}
	if err := s.qualifyNamespace(ctx, reader); err != nil {
		return nil, err
	}
	if _, err := reader.GetConfigMap(ctx, s.Namespace, GenesisName); err == nil {
		return nil, fmt.Errorf("admission Genesis already exists; render never replaces it")
	} else if !apierrors.IsNotFound(err) {
		return nil, err
	}
	encoded, err := json.Marshal(GenesisData{Contract: contract, Phase: PhaseInitializing})
	if err != nil {
		return nil, err
	}
	return &corev1.ConfigMap{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{Namespace: s.Namespace, Name: GenesisName},
		Data:       map[string]string{GenesisDataKey: string(encoded)},
	}, nil
}

// ReceiptFor creates a proposal only from a caller-supplied expected Genesis
// UID. The caller must then qualify that original object against the API.
func (s InstallationSpec) ReceiptFor(expectedGenesisUID string) (Receipt, error) {
	contract, err := s.contract()
	if err != nil {
		return Receipt{}, err
	}
	receipt := Receipt{Namespace: s.Namespace, GenesisName: GenesisName,
		GenesisUID: expectedGenesisUID, Contract: contract}
	if err := receipt.Validate(); err != nil {
		return Receipt{}, err
	}
	return receipt, nil
}

// ExportReceiptSecret verifies the explicitly pinned Namespace and Genesis
// before rendering an immutable Secret manifest. The Secret is created by the
// environment installer, never by the Service or this helper.
func (r Receipt) ExportReceiptSecret(ctx context.Context, reader Reader, serviceNamespace, secretName string) (*corev1.Secret, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	if len(validation.IsDNS1123Label(serviceNamespace)) != 0 || len(validation.IsDNS1123Subdomain(secretName)) != 0 ||
		serviceNamespace == r.Contract.ReceiptNamespace {
		return nil, fmt.Errorf("admission receipt Secret has invalid identity")
	}
	if reader == nil {
		return nil, fmt.Errorf("admission installation lacks a direct API reader")
	}
	ns, err := reader.GetNamespace(ctx, r.Namespace)
	if err != nil {
		return nil, err
	}
	if err := r.QualifyNamespace(ns); err != nil {
		return nil, err
	}
	receipts, err := reader.GetNamespace(ctx, r.Contract.ReceiptNamespace)
	if err != nil {
		return nil, err
	}
	if err := r.QualifyReceiptNamespace(receipts); err != nil {
		return nil, err
	}
	genesis, err := reader.GetConfigMap(ctx, r.Namespace, r.GenesisName)
	if err != nil {
		return nil, err
	}
	state, err := r.QualifyGenesis(genesis)
	if err != nil {
		return nil, err
	}
	if state.Phase != PhaseInitializing || state.ActiveLedgerUID != "" || state.QueueLedgerUID != "" {
		return nil, fmt.Errorf("admission receipt export requires untouched Initializing Genesis")
	}
	encoded, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	immutable := true
	return &corev1.Secret{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{Namespace: serviceNamespace, Name: secretName},
		Immutable:  &immutable,
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{ReceiptSecretDataKey: encoded},
	}, nil
}

// ValidateReceiptSecret binds the mounted receipt bytes to the exact
// externally installed immutable Secret UID. Recreating the same Secret name
// or changing the configured generation cannot silently rebind the Service.
func ValidateReceiptSecret(secret *corev1.Secret, serviceNamespace, secretName, expectedUID string, receiptRaw []byte) error {
	if !ValidUID(expectedUID) || len(validation.IsDNS1123Label(serviceNamespace)) != 0 ||
		len(validation.IsDNS1123Subdomain(secretName)) != 0 {
		return fmt.Errorf("admission receipt Secret has invalid expected identity")
	}
	receipt, err := ParseReceipt(receiptRaw)
	if err != nil {
		return err
	}
	if serviceNamespace == receipt.Contract.ReceiptNamespace {
		return fmt.Errorf("recovery receipt Namespace must be separate from Service namespace")
	}
	canonical, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	if secret == nil || secret.Namespace != serviceNamespace || secret.Name != secretName ||
		string(secret.UID) != expectedUID || secret.DeletionTimestamp != nil ||
		secret.Immutable == nil || !*secret.Immutable || secret.Type != corev1.SecretTypeOpaque ||
		len(secret.Data) != 1 || !bytes.Equal(secret.Data[ReceiptSecretDataKey], canonical) ||
		!bytes.Equal(receiptRaw, canonical) {
		return fmt.Errorf("original immutable admission receipt Secret is unavailable or changed")
	}
	return nil
}
