package sourcecmd

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/cofy-x/kova/internal/daemonclient"
	"github.com/cofy-x/kova/internal/source"
	"github.com/cofy-x/kova/internal/sourcebundle"
	cli "github.com/urfave/cli/v2"
)

func TestSourceExitCodeUsesProvenFailureClass(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want int
	}{
		{name: "invalid content", err: errors.Join(sourcebundle.ErrInvalidSource, errors.New("source too large")), want: sourcebundle.FetchExitCodeInvalidSource},
		{name: "invalid archive", err: errors.Join(source.ErrInvalidBuildArchive, errors.New("duplicate target")), want: sourcebundle.FetchExitCodeInvalidSource},
		{name: "out of disk", err: syscall.ENOSPC, want: sourcebundle.FetchExitCodeResourceExhausted},
		{name: "quota", err: syscall.EDQUOT, want: sourcebundle.FetchExitCodeResourceExhausted},
		{name: "file limit", err: syscall.EFBIG, want: sourcebundle.FetchExitCodeResourceExhausted},
		{name: "network", err: errors.New("connection refused"), want: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ExitCode(classifySourceCommandError(tc.err)); got != tc.want {
				t.Fatalf("exit code = %d, want %d", got, tc.want)
			}
		})
	}
	if got := ExitCode(errors.New("ordinary command failure")); got != 1 {
		t.Fatalf("ordinary command exit code = %d", got)
	}
}

func TestSourceInspectRejectsWrongPodBeforeReadingArchive(t *testing.T) {
	app := &cli.App{Commands: []*cli.Command{CLICommand()}}
	missing := filepath.Join(t.TempDir(), "missing-source.zip")
	t.Setenv(daemonclient.RunnerPodUIDEnv, "pod-replacement")
	for _, args := range [][]string{
		{"kovad", "source", "inspect", "--input", missing, "--expected-pod-uid", "pod-original"},
		{"kovad", "source", "inspect", "--input", missing},
	} {
		if err := app.Run(args); err == nil || !strings.Contains(err.Error(), "Pod UID fence") {
			t.Fatalf("wrong/absent Pod UID did not fail before source read: args=%v err=%v", args, err)
		}
	}
	if err := app.Run([]string{"kovad", "source", "inspect", "--input", missing,
		"--expected-pod-uid", "pod-replacement"}); err == nil || strings.Contains(err.Error(), "Pod UID fence") {
		t.Fatalf("exact UID did not reach archive check: %v", err)
	}
	if err := os.Unsetenv(daemonclient.RunnerPodUIDEnv); err != nil {
		t.Fatal(err)
	}
	if err := app.Run([]string{"kovad", "source", "inspect", "--input", missing}); err == nil || strings.Contains(err.Error(), "Pod UID fence") {
		t.Fatalf("legacy source inspect behavior changed: %v", err)
	}
}
