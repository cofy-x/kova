package app

import (
	"bytes"
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

func TestVersionCommand(t *testing.T) {
	app := NewCLIApp()
	var out bytes.Buffer
	app.Writer = &out
	app.ErrWriter = &out

	if err := app.Run([]string{"kova", "version"}); err != nil {
		t.Fatalf("run version: %v", err)
	}
	if !strings.HasPrefix(strings.TrimSpace(out.String()), "kova ") {
		t.Fatalf("version output = %q", out.String())
	}
}

func TestBuildRequiresKubernetesConfiguration(t *testing.T) {
	t.Setenv("KOVA_KUBECONFIG", "")
	app := NewCLIApp()
	app.Writer = &bytes.Buffer{}
	app.ErrWriter = &bytes.Buffer{}

	err := app.Run([]string{
		"kova",
		"--ctx-config", t.TempDir() + "/config.json",
		"--name", "runner",
		"build",
		"--target", "registry.example.com/team/app:test",
	})
	if err == nil || !strings.Contains(err.Error(), "--kubeconfig is required") {
		t.Fatalf("expected Kubernetes configuration error, got %v", err)
	}
}

func TestExportRejectsLegacyTrailingFlags(t *testing.T) {
	app := NewCLIApp()
	app.Writer = &bytes.Buffer{}
	app.ErrWriter = &bytes.Buffer{}

	err := app.Run([]string{"kova", "--kubeconfig", "unused", "--name", "runner", "export", "--", "--result", "result.jsonl"})
	if err == nil || !strings.Contains(err.Error(), "does not accept positional arguments") {
		t.Fatalf("expected legacy trailing flags to be rejected, got %v", err)
	}
}

func TestPreheatRejectsLegacyTrailingFlags(t *testing.T) {
	app := NewCLIApp()
	app.Writer = &bytes.Buffer{}
	app.ErrWriter = &bytes.Buffer{}

	err := app.Run([]string{"kova", "--kubeconfig", "unused", "--name", "runner", "preheat", "--", "--oci"})
	if err == nil || !strings.Contains(err.Error(), "does not accept positional arguments") {
		t.Fatalf("expected legacy trailing flags to be rejected, got %v", err)
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
