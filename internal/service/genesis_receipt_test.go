package service

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/cofy-x/kova/internal/service/admissiongenesis"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type receiptSecretFake struct {
	secret *corev1.Secret
	reads  int
}

func (f *receiptSecretFake) GetSecret(_ context.Context, _, _ string) (*corev1.Secret, error) {
	f.reads++
	return f.secret.DeepCopy(), nil
}

func TestGenesisReceiptRequiresCompletePinnedImmutableSecret(t *testing.T) {
	if enabled, err := (genesisReceiptOptions{}).enabled(); err != nil || enabled {
		t.Fatalf("legacy mode = %v, %v", enabled, err)
	}
	for _, partial := range []genesisReceiptOptions{{File: "receipt"}, {SecretName: "receipt"},
		{File: "receipt", SecretNamespace: "service-57", SecretName: "receipt-57"}} {
		if enabled, err := partial.enabled(); err == nil || enabled {
			t.Fatalf("partial receipt configuration accepted: %+v", partial)
		}
	}
	receipt, _ := genesisTestReceiptAndConfig()
	raw, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "receipt.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	immutable := true
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Namespace: "service-57", Name: "receipt-57", UID: types.UID("secret-original"),
	}, Immutable: &immutable, Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{admissiongenesis.ReceiptSecretDataKey: raw}}
	reader := &receiptSecretFake{secret: secret}
	options := genesisReceiptOptions{File: path, SecretNamespace: "service-57", SecretName: "receipt-57", SecretUID: "secret-original"}
	loaded, check, err := options.loadAndCheck(context.Background(), reader)
	if err != nil || string(loaded) != string(raw) || reader.reads != 1 {
		t.Fatalf("original Secret was not pinned: %v, reads=%d", err, reader.reads)
	}
	reader.secret.UID = types.UID("same-name-replacement")
	if err := check(context.Background()); err == nil {
		t.Fatal("same-name receipt Secret replacement was accepted")
	}
}
