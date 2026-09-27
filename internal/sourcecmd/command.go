package sourcecmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/cofy-x/kova/internal/buildcontract"
	"github.com/cofy-x/kova/internal/source"
	"github.com/cofy-x/kova/internal/sourcebundle"

	cli "github.com/urfave/cli/v2"
)

func CLICommand() *cli.Command {
	return &cli.Command{
		Name: "source", Usage: "materialize immutable source bundles",
		Subcommands: []*cli.Command{{
			Name: "fetch", Usage: "fetch and verify an immutable source bundle",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "uri", Required: true},
				&cli.StringFlag{Name: "digest", Required: true},
				&cli.StringFlag{Name: "output", Required: true},
				&cli.StringSliceFlag{Name: "registry-plain-http"},
			},
			Action: func(c *cli.Context) error {
				output := filepath.Clean(c.String("output"))
				if err := os.MkdirAll(filepath.Dir(output), 0o755); err != nil {
					return classifyFetchCommandError(err)
				}
				if strings.TrimSpace(output) == "." {
					return fmt.Errorf("output path is required")
				}
				if err := sourcebundle.Fetch(c.Context, c.String("uri"), c.String("digest"), output, c.StringSlice("registry-plain-http")); err != nil {
					return classifyFetchCommandError(err)
				}
				return nil
			},
		}, {
			Name: "inspect", Usage: "validate a source archive and print its normalized target contract",
			Flags: []cli.Flag{&cli.StringFlag{Name: "input", Required: true}},
			Action: func(c *cli.Context) error {
				targets, err := source.BuildArchiveTargets(filepath.Clean(c.String("input")))
				if err != nil {
					return err
				}
				return json.NewEncoder(c.App.Writer).Encode(struct {
					Targets []buildcontract.TargetSpec `json:"targets"`
				}{Targets: targets})
			},
		}},
	}
}

// fetchCommandError intentionally does not implement cli.ExitCoder: the
// kovad entrypoint owns logging and process exit after shutting down telemetry.
type fetchCommandError struct {
	err  error
	code int
}

func (e *fetchCommandError) Error() string { return e.err.Error() }
func (e *fetchCommandError) Unwrap() error { return e.err }

func classifyFetchCommandError(err error) error {
	return &fetchCommandError{err: err, code: fetchExitCode(err)}
}

func fetchExitCode(err error) int {
	if errors.Is(err, sourcebundle.ErrInvalidSource) {
		return sourcebundle.FetchExitCodeInvalidSource
	}
	if errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.EDQUOT) || errors.Is(err, syscall.EFBIG) {
		return sourcebundle.FetchExitCodeResourceExhausted
	}
	return 1
}

// ExitCode returns the source-fetch protocol exit code, or the ordinary
// command failure code for other kovad commands.
func ExitCode(err error) int {
	var fetchErr *fetchCommandError
	if errors.As(err, &fetchErr) {
		return fetchErr.code
	}
	return 1
}
