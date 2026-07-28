package version

import (
	"fmt"
	"runtime"
	"runtime/debug"
)

var (
	Version   = "dev"
	Commit    = "unknown"
	BuildDate = "unknown"
)

func init() {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}
	applyBuildInfo(info)
}

func applyBuildInfo(info *debug.BuildInfo) {
	if Version == "dev" && info.Main.Version != "" && info.Main.Version != "(devel)" {
		Version = info.Main.Version
	}
	if Commit != "unknown" {
		return
	}
	for _, setting := range info.Settings {
		if setting.Key == "vcs.revision" && setting.Value != "" {
			Commit = setting.Value
			return
		}
	}
}

func String(name string) string {
	return fmt.Sprintf("%s %s (commit %s, built %s, %s/%s)", name, Version, Commit, BuildDate, runtime.GOOS, runtime.GOARCH)
}
