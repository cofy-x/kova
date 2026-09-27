package main

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/cofy-x/kova/internal/sourcecmd"
	"github.com/urfave/cli/v2"
)

func TestSourceFetchCommandPreservesInvalidSourceExitCode(t *testing.T) {
	err := newCLIApp().Run([]string{
		"kovad", "source", "fetch",
		"--uri", "https://sources.example.com/source.zip?token=forbidden",
		"--digest", "sha256:" + strings.Repeat("a", 64),
		"--output", filepath.Join(t.TempDir(), "source.zip"),
	})
	if err == nil || sourcecmd.ExitCode(err) != 20 {
		t.Fatalf("source-fetch error=%v exit=%d, want classified invalid source", err, sourcecmd.ExitCode(err))
	}
}

func TestPprofFlagBelongsOnlyToDaemonCommand(t *testing.T) {
	app := newCLIApp()
	if flagNamed(app.Flags, "pprof-server") {
		t.Fatal("pprof-server must not be a global kovad flag")
	}

	for _, command := range app.Commands {
		hasPprof := flagNamed(command.Flags, "pprof-server")
		if command.Name == "daemon" && !hasPprof {
			t.Fatal("daemon command must expose pprof-server")
		}
		if command.Name != "daemon" && hasPprof {
			t.Fatalf("command %q must not expose pprof-server", command.Name)
		}
	}
}

func flagNamed(flags []cli.Flag, name string) bool {
	for _, flag := range flags {
		for _, candidate := range flag.Names() {
			if candidate == name {
				return true
			}
		}
	}
	return false
}
