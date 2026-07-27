package runner

import (
	"context"
	"fmt"
	"testing"
)

func TestResolveRunnerImageUsesExplicitImage(t *testing.T) {
	image, err := ResolveRunnerImage(context.Background(), nil, "default", "", " localhost:5001/kova:dev ")
	if err != nil {
		t.Fatal(err)
	}
	if image != "localhost:5001/kova:dev" {
		t.Fatalf("image = %q", image)
	}
}

func TestResolveRunnerImageRequiresImageWhenSecretDisabled(t *testing.T) {
	if _, err := ResolveRunnerImage(context.Background(), nil, "default", "", ""); err == nil {
		t.Fatal("expected explicit image error")
	}
}

func TestResolveRunnerImageReadsNamespaceSecretBeforeDefault(t *testing.T) {
	kube := fakeKubernetes{secretData: map[string]string{
		"jobs/kova-registry/kova-image":    "registry.local/jobs:dev",
		"default/kova-registry/kova-image": "registry.local/default:dev",
	}}

	image, err := ResolveRunnerImage(context.Background(), kube, "jobs", "kova-registry", "")
	if err != nil {
		t.Fatal(err)
	}
	if image != "registry.local/jobs:dev" {
		t.Fatalf("image = %q", image)
	}
}

func TestResolveRunnerImageFallsBackToDefaultSecret(t *testing.T) {
	kube := fakeKubernetes{secretData: map[string]string{
		"default/kova-registry/kova-image": "registry.local/default:dev",
	}}

	image, err := ResolveRunnerImage(context.Background(), kube, "jobs", "kova-registry", "")
	if err != nil {
		t.Fatal(err)
	}
	if image != "registry.local/default:dev" {
		t.Fatalf("image = %q", image)
	}
}

type fakeKubernetes struct {
	secretData map[string]string
}

func (f fakeKubernetes) GetSecretData(_ context.Context, namespace string, name string, key string) (string, error) {
	value, ok := f.secretData[namespace+"/"+name+"/"+key]
	if !ok {
		return "", fmt.Errorf("not found")
	}
	return value, nil
}
