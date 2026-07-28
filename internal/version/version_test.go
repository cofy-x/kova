package version

import (
	"runtime"
	"runtime/debug"
	"strings"
	"testing"
)

func preserveBuildVersion(t *testing.T) {
	t.Helper()
	oldVersion, oldCommit, oldBuildDate := Version, Commit, BuildDate
	t.Cleanup(func() {
		Version, Commit, BuildDate = oldVersion, oldCommit, oldBuildDate
	})
}

func TestString(t *testing.T) {
	preserveBuildVersion(t)

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

func TestApplyBuildInfoUsesModuleVersionForGoInstall(t *testing.T) {
	preserveBuildVersion(t)
	Version = "dev"
	Commit = "unknown"

	applyBuildInfo(&debug.BuildInfo{
		Main: debug.Module{Version: "v1.2.3"},
		Settings: []debug.BuildSetting{
			{Key: "vcs.revision", Value: "abc123"},
		},
	})

	if Version != "v1.2.3" || Commit != "abc123" {
		t.Fatalf("build info fallback = version %q, commit %q", Version, Commit)
	}
}

func TestApplyBuildInfoPreservesLinkerValues(t *testing.T) {
	preserveBuildVersion(t)
	Version = "v2.0.0"
	Commit = "release-commit"

	applyBuildInfo(&debug.BuildInfo{
		Main: debug.Module{Version: "v1.2.3"},
		Settings: []debug.BuildSetting{
			{Key: "vcs.revision", Value: "build-info-commit"},
		},
	})

	if Version != "v2.0.0" || Commit != "release-commit" {
		t.Fatalf("linker values changed to version %q, commit %q", Version, Commit)
	}
}

func TestApplyBuildInfoIgnoresDevelopmentVersion(t *testing.T) {
	preserveBuildVersion(t)
	Version = "dev"
	Commit = "unknown"

	applyBuildInfo(&debug.BuildInfo{Main: debug.Module{Version: "(devel)"}})

	if Version != "dev" || Commit != "unknown" {
		t.Fatalf("development values changed to version %q, commit %q", Version, Commit)
	}
}
