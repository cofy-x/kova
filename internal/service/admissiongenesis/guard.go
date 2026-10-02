package admissiongenesis

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
)

// Guard retains the original pair qualified at startup. A later read can
// refuse authority, but cannot silently adopt a different committed pair.
// Check is not a lease: callers must check again before each new side effect
// and retain their own UID/resourceVersion CAS and uncertain-outcome fences.
type Guard struct {
	Bootstrap Bootstrapper
	Original  Binding
}

// Checker is the read-only authority check used at runtime side-effect edges.
// A concrete Guard also exposes its original ledger bindings to CAS writers.
type Checker interface {
	Check(context.Context) error
}

func NewGuard(ctx context.Context, bootstrap Bootstrapper, original Binding) (*Guard, error) {
	if err := bootstrap.validate(); err != nil {
		return nil, err
	}
	if original.NamespaceUID != bootstrap.Receipt.Contract.NamespaceUID ||
		original.GenesisUID != bootstrap.Receipt.GenesisUID ||
		!validUID(original.ActiveLedgerUID) || !validUID(original.QueueLedgerUID) {
		return nil, fmt.Errorf("%w: startup binding differs from admission receipt", ErrChanged)
	}
	guard := &Guard{Bootstrap: bootstrap, Original: original}
	if err := guard.Check(ctx); err != nil {
		return nil, err
	}
	return guard, nil
}

func (g *Guard) Check(ctx context.Context) error {
	if g == nil {
		return fmt.Errorf("admission Genesis guard is missing")
	}
	current, err := g.Bootstrap.ObserveCommitted(ctx)
	if err != nil {
		return err
	}
	if current != g.Original {
		return fmt.Errorf("%w: committed admission pair differs from startup binding", ErrChanged)
	}
	return nil
}

func (g *Guard) CheckLedger(cm *corev1.ConfigMap, role Role) error {
	if g == nil {
		return fmt.Errorf("admission Genesis guard is missing")
	}
	if err := g.Bootstrap.Receipt.QualifyLedgerIdentity(cm, role); err != nil {
		return err
	}
	var expected string
	switch role {
	case Active:
		expected = g.Original.ActiveLedgerUID
	case Queue:
		expected = g.Original.QueueLedgerUID
	default:
		return fmt.Errorf("invalid admission ledger role")
	}
	if string(cm.UID) != expected {
		return fmt.Errorf("%w: %s ledger differs from startup UID", ErrChanged, role)
	}
	return nil
}

// PatchLedgerData is the only Genesis-mode ledger writer. JSON Patch tests
// the original UID, opaque resourceVersion, and exact old data in the same
// API operation that replaces the data. A stale writer therefore cannot
// mutate a same-name replacement, even if an ordinary Update were to accept
// its old UID. The direct CoreAPI transport must make one wire attempt.
func (g *Guard) PatchLedgerData(ctx context.Context, cm *corev1.ConfigMap, role Role, nextData string) error {
	if err := g.CheckLedger(cm, role); err != nil {
		return err
	}
	if cm.ResourceVersion == "" {
		return fmt.Errorf("%w: %s ledger lacks a resourceVersion", ErrChanged, role)
	}
	var template LedgerTemplate
	switch role {
	case Active:
		template = g.Bootstrap.Active
	case Queue:
		template = g.Bootstrap.Queue
	default:
		return fmt.Errorf("invalid admission ledger role")
	}
	if len(cm.Data) != 1 || len(cm.BinaryData) != 0 || cm.Data[template.DataKey] == "" {
		return fmt.Errorf("%w: %s ledger has unsupported data keys", ErrChanged, role)
	}
	proposed := cm.DeepCopy()
	proposed.Data = map[string]string{template.DataKey: nextData}
	if err := template.Validate(proposed); err != nil {
		return fmt.Errorf("%s ledger proposal: %w", role, err)
	}
	if err := g.Check(ctx); err != nil {
		return err
	}
	// JSON Pointer needs no escaping for the two fixed, validated data keys.
	body, err := json.Marshal([]patchOp{
		{Op: "test", Path: "/metadata/uid", Value: string(cm.UID)},
		{Op: "test", Path: "/metadata/resourceVersion", Value: cm.ResourceVersion},
		{Op: "test", Path: "/data/" + template.DataKey, Value: cm.Data[template.DataKey]},
		{Op: "replace", Path: "/data/" + template.DataKey, Value: nextData},
	})
	if err != nil {
		return err
	}
	updated, err := g.Bootstrap.API.PatchConfigMap(ctx, cm.Namespace, cm.Name, body)
	if err != nil {
		// A failed test, timeout, or lost response is not permission to
		// retry blindly. Callers use their existing exact-intent/nonce
		// readback rules; a later loop iteration requalifies the pair.
		return err
	}
	if err := g.CheckLedger(updated, role); err != nil {
		return err
	}
	if updated.Data[template.DataKey] != nextData {
		return fmt.Errorf("%w: %s ledger Patch response differs from proposal", ErrChanged, role)
	}
	// The Patch response proves this commit; the direct readback checks that
	// it still belongs to the original installation. Another valid CAS may
	// already have advanced the data, so equality is not required here.
	readback, err := g.Bootstrap.API.GetConfigMap(ctx, cm.Namespace, cm.Name)
	if err != nil {
		return err
	}
	if err := g.CheckLedger(readback, role); err != nil {
		return err
	}
	if err := template.Validate(readback); err != nil {
		return err
	}
	return g.Check(ctx)
}
