package ctxconfig

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadMissingReturnsEmptyConfig(t *testing.T) {
	cfg, err := Load(filepath.Join(t.TempDir(), "missing.json"))
	if err != nil {
		t.Fatalf("Load missing: %v", err)
	}
	if len(cfg.Contexts) != 0 {
		t.Fatalf("contexts = %#v, want empty", cfg.Contexts)
	}
}

func TestSaveLoadRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kova", "config.json")
	want := Config{
		Current: "kind",
		Contexts: map[string]Context{
			"kind": {
				Kubeconfig:            "/tmp/kubeconfig",
				Namespace:             "default",
				BuildkitAddr:          "tcp://kova.kova.svc:9094",
				RunnerImage:           "localhost:5001/kova:dev",
				RunnerImagePullPolicy: "IfNotPresent",
				ImagePullSecret:       "",
			},
		},
	}
	if err := Save(path, want); err != nil {
		t.Fatalf("Save: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat config: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("config permissions = %v, want 0600", got)
	}

	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.Current != want.Current {
		t.Fatalf("current = %q, want %q", got.Current, want.Current)
	}
	if got.Contexts["kind"].RunnerImage != "localhost:5001/kova:dev" {
		t.Fatalf("context = %#v", got.Contexts["kind"])
	}
}

func TestResolveUsesCurrentWhenNameEmpty(t *testing.T) {
	cfg := Config{
		Current:  "kind",
		Contexts: map[string]Context{"kind": {Kubeconfig: "kind.kubeconfig"}},
	}
	name, ctx, ok := cfg.Resolve("")
	if !ok {
		t.Fatal("expected current context")
	}
	if name != "kind" || ctx.Kubeconfig != "kind.kubeconfig" {
		t.Fatalf("resolved %q %#v", name, ctx)
	}
}

func TestValidateNameRejectsUnsupportedCharacters(t *testing.T) {
	if _, err := ValidateName("remote-stage"); err != nil {
		t.Fatalf("expected name to be valid: %v", err)
	}
	if _, err := ValidateName("../bad"); err == nil {
		t.Fatal("expected invalid name error")
	}
}
