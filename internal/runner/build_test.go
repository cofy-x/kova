package runner

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStdinIsTerminalForNonFileReader(t *testing.T) {
	if stdinIsTerminal(strings.NewReader("zip")) {
		t.Fatal("strings.Reader must not be treated as terminal")
	}
}

func TestNonTerminalStdinKeepsPipeReaders(t *testing.T) {
	reader := strings.NewReader("input")
	if nonTerminalStdin(reader) != reader {
		t.Fatal("non-terminal stdin should be preserved")
	}
}

func TestPrepareBuildInputPackagesSingleDirectory(t *testing.T) {
	imageDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(imageDir, "Dockerfile"), []byte("FROM scratch\n"), 0o644); err != nil {
		t.Fatalf("write Dockerfile: %v", err)
	}

	args, input, cleanup, err := prepareBuildInput([]string{"--target", "localhost:5002/example/app:dev", imageDir})
	if cleanup != nil {
		defer cleanup()
	}
	if err != nil {
		t.Fatalf("prepare build input: %v", err)
	}
	if len(args) != 2 || args[0] != "--target" || args[1] != "localhost:5002/example/app:dev" {
		t.Fatalf("unexpected args: %#v", args)
	}
	if input == nil {
		t.Fatal("expected archive input")
	}
	raw, err := io.ReadAll(input)
	if err != nil {
		t.Fatalf("read input: %v", err)
	}
	if len(raw) == 0 {
		t.Fatal("expected non-empty archive")
	}
}
