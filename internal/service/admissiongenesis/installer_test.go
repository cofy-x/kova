package admissiongenesis

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestExternalInstallerPinsCallerSuppliedIdentities(t *testing.T) {
	receipt := testReceipt()
	spec := InstallationSpec{Namespace: receipt.Namespace, NamespaceUID: receipt.Contract.NamespaceUID,
		Generation: receipt.Contract.Generation, Limits: receipt.Contract.Limits}
	api := newFakeCore(t, receipt)
	originalGenesis := api.objects[GenesisName]
	delete(api.objects, GenesisName)
	manifest, err := spec.RenderInitializingGenesis(context.Background(), api)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Name != GenesisName || manifest.Namespace != receipt.Namespace || manifest.UID != "" ||
		manifest.Immutable != nil || len(manifest.Data) != 1 || len(manifest.BinaryData) != 0 {
		t.Fatalf("unexpected declarative Genesis manifest: %+v", manifest)
	}
	var initial GenesisData
	if err := json.Unmarshal([]byte(manifest.Data[GenesisDataKey]), &initial); err != nil {
		t.Fatal(err)
	}
	if initial.Contract != receipt.Contract || initial.Phase != PhaseInitializing ||
		initial.ActiveLedgerUID != "" || initial.QueueLedgerUID != "" {
		t.Fatalf("Genesis manifest changed explicit installation facts: %+v", initial)
	}
	api.objects[GenesisName] = originalGenesis
	if _, err := spec.RenderInitializingGenesis(context.Background(), api); err == nil {
		t.Fatal("render helper accepted an existing Genesis")
	}
	proposed, err := spec.ReceiptFor(receipt.GenesisUID)
	if err != nil || proposed != receipt {
		t.Fatalf("expected exact externally pinned receipt, got %+v: %v", proposed, err)
	}
	secret, err := proposed.ExportReceiptSecret(context.Background(), api, "service-57", "kova-admission-receipt")
	if err != nil {
		t.Fatal(err)
	}
	if secret.UID != "" || secret.Immutable == nil || !*secret.Immutable ||
		secret.Type != corev1.SecretTypeOpaque || len(secret.Data) != 1 {
		t.Fatalf("receipt manifest was not an immutable external Secret: %+v", secret)
	}
	secret.UID = types.UID("receipt-secret-original")
	if err := ValidateReceiptSecret(secret, "service-57", "kova-admission-receipt", string(secret.UID),
		secret.Data[ReceiptSecretDataKey]); err != nil {
		t.Fatal(err)
	}

	wrongNS := spec
	wrongNS.NamespaceUID = "other-namespace-uid"
	if _, err := wrongNS.RenderInitializingGenesis(context.Background(), api); err == nil {
		t.Fatal("wrong caller-supplied Namespace UID was promoted from API discovery")
	}
	wrongGeneration := spec
	wrongGeneration.Generation = strings.Repeat("b", 32)
	wrongReceipt, err := wrongGeneration.ReceiptFor(receipt.GenesisUID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wrongReceipt.ExportReceiptSecret(context.Background(), api, "service-57", "kova-admission-receipt"); err == nil {
		t.Fatal("wrong caller-supplied generation was promoted from Genesis discovery")
	}
	if _, err := spec.ReceiptFor("replacement-genesis-uid"); err != nil {
		t.Fatal(err)
	}
	replaced, _ := spec.ReceiptFor("replacement-genesis-uid")
	if _, err := replaced.ExportReceiptSecret(context.Background(), api, "service-57", "kova-admission-receipt"); err == nil {
		t.Fatal("same-name replacement Genesis was accepted")
	}
}

func TestImmutableReceiptSecretRefusesRebinding(t *testing.T) {
	r := testReceipt()
	api := newFakeCore(t, r)
	secret, err := r.ExportReceiptSecret(context.Background(), api, "service-57", "receipt-57")
	if err != nil {
		t.Fatal(err)
	}
	secret.UID = types.UID("secret-original")
	raw := bytes.Clone(secret.Data[ReceiptSecretDataKey])
	check := func(changed *corev1.Secret, mounted []byte) {
		t.Helper()
		if err := ValidateReceiptSecret(changed, "service-57", "receipt-57", "secret-original", mounted); err == nil {
			t.Fatal("changed original receipt Secret was accepted")
		}
	}
	changed := secret.DeepCopy()
	changed.UID = types.UID("replacement")
	check(changed, raw)
	changed = secret.DeepCopy()
	changed.Immutable = nil
	check(changed, raw)
	changed = secret.DeepCopy()
	changed.Data[ReceiptSecretDataKey] = []byte(`{}`)
	check(changed, raw)
	changed = secret.DeepCopy()
	changed.Data["extra"] = []byte("x")
	check(changed, raw)
	changed = secret.DeepCopy()
	changed.Namespace = "other-service"
	check(changed, raw)
	check(secret, append(bytes.Clone(raw), ' '))
	if err := ValidateReceiptSecret(secret, "service-57", "receipt-57", "secret-original", raw); err != nil {
		t.Fatal(err)
	}
}
