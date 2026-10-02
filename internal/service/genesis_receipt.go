package service

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/cofy-x/kova/internal/service/admissiongenesis"
	corev1 "k8s.io/api/core/v1"
)

type genesisReceiptOptions struct {
	File            string
	SecretNamespace string
	SecretName      string
	SecretUID       string
}

func (o genesisReceiptOptions) enabled() (bool, error) {
	if o.File == "" && o.SecretNamespace == "" && o.SecretName == "" && o.SecretUID == "" {
		return false, nil
	}
	if o.File == "" || o.SecretNamespace == "" || o.SecretName == "" || o.SecretUID == "" {
		return false, fmt.Errorf("admission Genesis requires receipt file and exact Secret namespace, name, and UID")
	}
	return true, nil
}

type genesisSecretReader interface {
	GetSecret(context.Context, string, string) (*corev1.Secret, error)
}

// The file is a read-only projected Secret volume. Its exact bytes must match
// a direct, named read of the original immutable Secret, including its UID.
func (o genesisReceiptOptions) loadAndCheck(ctx context.Context, reader genesisSecretReader) ([]byte, func(context.Context) error, error) {
	enabled, err := o.enabled()
	if err != nil {
		return nil, nil, err
	}
	if !enabled {
		return nil, nil, fmt.Errorf("admission Genesis receipt is not configured")
	}
	if reader == nil {
		return nil, nil, fmt.Errorf("admission Genesis receipt requires a direct Secret reader")
	}
	file, err := os.Open(o.File)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, 16*1024+1))
	if err != nil {
		return nil, nil, err
	}
	if len(raw) > 16*1024 {
		return nil, nil, fmt.Errorf("admission Genesis receipt exceeds 16 KiB")
	}
	check := o.checkRaw(raw, reader)
	if err := check(ctx); err != nil {
		return nil, nil, err
	}
	return raw, check, nil
}

func (o genesisReceiptOptions) checkRaw(raw []byte, reader genesisSecretReader) func(context.Context) error {
	return func(ctx context.Context) error {
		secret, err := reader.GetSecret(ctx, o.SecretNamespace, o.SecretName)
		if err != nil {
			return err
		}
		return admissiongenesis.ValidateReceiptSecret(secret, o.SecretNamespace, o.SecretName, o.SecretUID, raw)
	}
}
