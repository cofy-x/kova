package app

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	cli "github.com/urfave/cli/v2"
)

func TestResolvePprofServerAddrPrefersFlagThenEnv(t *testing.T) {
	t.Setenv(pprofServerEnv, "127.0.0.1:6061")

	if addr := ResolvePprofServerAddr(""); addr != "127.0.0.1:6061" {
		t.Fatalf("expected env fallback, got %q", addr)
	}
	if addr := ResolvePprofServerAddr(" 0.0.0.0:6060 "); addr != "0.0.0.0:6060" {
		t.Fatalf("expected trimmed flag value, got %q", addr)
	}
}

func TestNewCLIAppIncludesBuildkitAddrFlag(t *testing.T) {
	app := NewCLIApp()
	for _, flag := range app.Flags {
		if stringFlag, ok := flag.(*cli.StringFlag); ok && stringFlag.Name == "buildkit-addr" {
			if stringFlag.Value == "" {
				t.Fatal("buildkit-addr default must not be empty")
			}
			return
		}
	}
	t.Fatal("expected global buildkit-addr flag")
}

func TestRuntimeCommandsAreNotRegisteredInUserCLI(t *testing.T) {
	app := NewCLIApp()
	for _, command := range app.Commands {
		if command.Name == "daemon" || command.Name == "service" {
			t.Fatalf("runtime command %q should only be registered in kovad", command.Name)
		}
	}
}

func TestUsageErrorHintsMisplacedGlobalFlag(t *testing.T) {
	app := NewCLIApp()
	app.Writer = &bytes.Buffer{}
	app.ErrWriter = &bytes.Buffer{}

	err := app.Run([]string{"kova", "status", "-name", "e2e"})
	if err == nil {
		t.Fatal("expected usage error")
	}
	message := err.Error()
	for _, want := range []string{
		"flag provided but not defined: -name",
		"Hint: --name is a global flag",
		"kova --name <value> status",
	} {
		if !strings.Contains(message, want) {
			t.Fatalf("expected error to contain %q:\n%s", want, message)
		}
	}
}

func TestUsageErrorDoesNotHintUnknownCommandFlag(t *testing.T) {
	app := NewCLIApp()
	app.Writer = &bytes.Buffer{}
	app.ErrWriter = &bytes.Buffer{}

	err := app.Run([]string{"kova", "status", "--unknown"})
	if err == nil {
		t.Fatal("expected usage error")
	}
	if strings.Contains(err.Error(), "global flag") {
		t.Fatalf("unexpected global flag hint:\n%s", err)
	}
}

func TestUsageErrorHintsNestedCommandPath(t *testing.T) {
	app := NewCLIApp()
	app.Writer = &bytes.Buffer{}
	app.ErrWriter = &bytes.Buffer{}

	err := app.Run([]string{"kova", "ctx", "show", "--ctx", "kind"})
	if err == nil {
		t.Fatal("expected usage error")
	}
	if !strings.Contains(err.Error(), "kova --ctx <value> ctx show") {
		t.Fatalf("expected nested command path in hint:\n%s", err)
	}
}

func TestParseBuildTrailingArgsAcceptsDirectoryThenFlags(t *testing.T) {
	imageDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(imageDir, "Dockerfile"), []byte("FROM scratch\n"), 0o644); err != nil {
		t.Fatalf("write Dockerfile: %v", err)
	}

	opts := buildCLIOptions{
		Concurrency: 1,
		BuildFormat: "nydus",
		Timeout:     300,
	}
	err := parseBuildTrailingArgs([]string{
		imageDir,
		"--target", "localhost:5002/example/app:dev",
		"--format", "oci",
		"--concurrency=2",
		"--var", "KOVA_TAG=dev",
		"--fail-fast",
		"--verbose=false",
	}, &opts)
	if err != nil {
		t.Fatalf("parse trailing args: %v", err)
	}
	if opts.ImageDir != imageDir {
		t.Fatalf("image dir = %q", opts.ImageDir)
	}
	if opts.Target != "localhost:5002/example/app:dev" {
		t.Fatalf("target = %q", opts.Target)
	}
	if opts.BuildFormat != "oci" || opts.Concurrency != 2 {
		t.Fatalf("format/concurrency = %q/%d", opts.BuildFormat, opts.Concurrency)
	}
	if len(opts.Vars) != 1 || opts.Vars[0] != "KOVA_TAG=dev" {
		t.Fatalf("vars = %#v", opts.Vars)
	}
	if !opts.Failfast {
		t.Fatal("expected fail-fast")
	}
	if opts.Verbose {
		t.Fatal("expected verbose=false to override")
	}
}

func TestParseBuildTrailingArgsRejectsTargetConflict(t *testing.T) {
	opts := buildCLIOptions{Target: "localhost:5002/example/app:dev"}
	err := parseBuildTrailingArgs([]string{"localhost:5002/example/other:dev"}, &opts)
	if err == nil || !strings.Contains(err.Error(), "either --target or a positional target") {
		t.Fatalf("expected target conflict, got %v", err)
	}
}

func TestParseBuildTrailingArgsRejectsDirectoryAndPositionalTarget(t *testing.T) {
	imageDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(imageDir, "Dockerfile"), []byte("FROM scratch\n"), 0o644); err != nil {
		t.Fatalf("write Dockerfile: %v", err)
	}

	opts := buildCLIOptions{}
	err := parseBuildTrailingArgs([]string{imageDir, "localhost:5002/example/app:dev"}, &opts)
	if err == nil || !strings.Contains(err.Error(), "only with --target") {
		t.Fatalf("expected directory target error, got %v", err)
	}
}
