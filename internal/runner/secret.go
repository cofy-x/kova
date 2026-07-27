package runner

import (
	"context"
	"fmt"
	"strings"
)

type secretDataReader interface {
	GetSecretData(ctx context.Context, namespace string, name string, key string) (string, error)
}

func ResolveRunnerImage(ctx context.Context, secrets secretDataReader, namespace string, secretName string, explicitImage string) (string, error) {
	if strings.TrimSpace(explicitImage) != "" {
		return strings.TrimSpace(explicitImage), nil
	}
	if strings.TrimSpace(secretName) == "" {
		return "", fmt.Errorf("--image is required when image pull secret is disabled")
	}
	if secrets == nil {
		return "", fmt.Errorf("Kubernetes client is required to resolve image pull secret")
	}
	if image, err := secrets.GetSecretData(ctx, namespace, secretName, "kova-image"); err == nil && image != "" {
		return image, nil
	}
	image, err := secrets.GetSecretData(ctx, "default", secretName, "kova-image")
	if err != nil {
		return "", fmt.Errorf("failed to read kova-image from %s/%s or default/%s", namespace, secretName, secretName)
	}
	if strings.TrimSpace(image) == "" {
		return "", fmt.Errorf("kova-image in %s/%s or default/%s is empty", namespace, secretName, secretName)
	}
	return image, nil
}
