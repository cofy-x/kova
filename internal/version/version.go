package version

import (
	"fmt"
	"runtime"
)

var (
	Version   = "dev"
	Commit    = "unknown"
	BuildDate = "unknown"
)

func String(name string) string {
	return fmt.Sprintf("%s %s (commit %s, built %s, %s/%s)", name, Version, Commit, BuildDate, runtime.GOOS, runtime.GOARCH)
}
