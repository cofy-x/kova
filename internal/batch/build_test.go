package batch

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cofy-x/kova/internal/scheduler"
)

func TestRunBuildRejectsUnmatchedExplicitBatchTarget(t *testing.T) {
	root := t.TempDir()
	imageDir := filepath.Join(root, "image")
	if err := os.Mkdir(imageDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(imageDir, "metadata.json"), []byte(`{"target":"registry.example.com/ns/present:dev"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(imageDir, "Dockerfile"), []byte("FROM scratch\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	err := RunBuild(Options{
		ImageDirs:   root,
		Target:      "registry.example.com/ns/missing:dev",
		BuildFormat: "oci",
		Addrs:       []*scheduler.Addr{{Addr: "tcp://unused:9094"}},
		Concurrency: 1,
	})
	if err == nil || !strings.Contains(err.Error(), "did not match any image metadata") {
		t.Fatalf("expected unmatched target error, got %v", err)
	}
}
