package runner

import (
	"bytes"
	"fmt"
	"io"

	"github.com/cofy-x/kova/internal/kube"
)

func kubeExecOptions(stdin io.Reader, stdout io.Writer, stderr io.Writer, command ...string) kube.ExecOptions {
	return kube.ExecOptions{
		Command: command,
		Stdin:   stdin,
		Stdout:  stdout,
		Stderr:  stderr,
	}
}

func nonTerminalStdin(stdin io.Reader) io.Reader {
	if stdinIsTerminal(stdin) {
		return nil
	}
	return stdin
}

func execError(action string, stderr []byte, err error) error {
	if err == nil {
		return nil
	}
	if len(stderr) > 0 {
		return fmt.Errorf("%s: %w: %s", action, err, bytes.TrimSpace(stderr))
	}
	return fmt.Errorf("%s: %w", action, err)
}
