package sourcecmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

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
					return err
				}
				if strings.TrimSpace(output) == "." {
					return fmt.Errorf("output path is required")
				}
				return sourcebundle.Fetch(c.Context, c.String("uri"), c.String("digest"), output, c.StringSlice("registry-plain-http"))
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
					Targets []string `json:"targets"`
				}{Targets: targets})
			},
		}},
	}
}
