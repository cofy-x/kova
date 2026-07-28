package main

import (
	"testing"

	"github.com/urfave/cli/v2"
)

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
