package runner

import (
	"os"
	"path/filepath"
	"testing"
)

func TestClearMatchingState(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "state")
	if err := os.WriteFile(stateFile, []byte("pod=e2e\nnamespace=default\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := &Client{Config: Config{PodName: "e2e", Namespace: "default", StateFile: stateFile}}

	client.clearMatchingState()

	if _, err := os.Stat(stateFile); !os.IsNotExist(err) {
		t.Fatalf("state file still exists, err=%v", err)
	}
}

func TestClearMatchingStateLeavesOtherState(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "state")
	if err := os.WriteFile(stateFile, []byte("pod=other\nnamespace=default\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := &Client{Config: Config{PodName: "e2e", Namespace: "default", StateFile: stateFile}}

	client.clearMatchingState()

	if _, err := os.Stat(stateFile); err != nil {
		t.Fatalf("state file should remain: %v", err)
	}
}
