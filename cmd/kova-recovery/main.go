// kova-recovery is a separate explicit operator tool, never registered in the
// regular user client, controller manager, daemon or Helm startup commands.
package main

import (
	"fmt"
	"os"

	"github.com/cofy-x/kova/internal/recoveryoperator"
)

func main() {
	if err := recoveryoperator.NewCLIApp().Run(os.Args); err != nil {
		// Command errors are bounded, redacted classes, never archive bodies or
		// API/credential diagnostics. No automatic retry or recovery follows.
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
