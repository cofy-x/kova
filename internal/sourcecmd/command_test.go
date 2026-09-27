package sourcecmd

import (
	"errors"
	"syscall"
	"testing"

	"github.com/cofy-x/kova/internal/source"
	"github.com/cofy-x/kova/internal/sourcebundle"
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
