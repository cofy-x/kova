package version

import (
	"runtime"
	"strings"
	"testing"
)

func TestString(t *testing.T) {
	oldVersion, oldCommit, oldBuildDate := Version, Commit, BuildDate
	t.Cleanup(func() {
		Version, Commit, BuildDate = oldVersion, oldCommit, oldBuildDate
	})

	Version = "v1.2.3"
	Commit = "abc123"
	BuildDate = "2026-07-28T00:00:00Z"

	got := String("kova")
	for _, want := range []string{"kova v1.2.3", "commit abc123", "built 2026-07-28T00:00:00Z", runtime.GOOS + "/" + runtime.GOARCH} {
		if !strings.Contains(got, want) {
			t.Fatalf("String() = %q, want %q", got, want)
		}
	}
}
