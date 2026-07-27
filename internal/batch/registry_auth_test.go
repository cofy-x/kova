package batch

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadRegistryAuthResolvesNormalizedRegistry(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	auth := base64.StdEncoding.EncodeToString([]byte("user:pass"))
	config := []byte(`{"auths":{"https://registry.example.com/":{"auth":"` + auth + `"}}}`)
	if err := os.WriteFile(path, config, 0o600); err != nil {
		t.Fatal(err)
	}

	credentials, err := loadRegistryAuth(path)
	if err != nil {
		t.Fatal(err)
	}
	username, password := credentials.forURL("https://registry.example.com/v2/ns/image/manifests/dev")
	if username != "user" || password != "pass" {
		t.Fatalf("credentials = %q/%q", username, password)
	}
}

func TestLoadRegistryAuthAllowsMissingConfig(t *testing.T) {
	credentials, err := loadRegistryAuth(filepath.Join(t.TempDir(), "missing.json"))
	if err != nil {
		t.Fatal(err)
	}
	if username, password := credentials.forURL("https://registry.example.com/v2/image/manifests/dev"); username != "" || password != "" {
		t.Fatalf("unexpected credentials = %q/%q", username, password)
	}
}

func TestLoadRegistryAuthRejectsMalformedCredential(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	auth := base64.StdEncoding.EncodeToString([]byte("missing-separator"))
	config := []byte(`{"auths":{"registry.example.com":{"auth":"` + auth + `"}}}`)
	if err := os.WriteFile(path, config, 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := loadRegistryAuth(path); err == nil {
		t.Fatal("expected malformed credential to fail")
	}
}
