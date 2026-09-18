package app

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/cofy-x/kova/internal/buildcontract"
	"github.com/cofy-x/kova/internal/source"
	"github.com/cofy-x/kova/internal/sourcebundle"

	cli "github.com/urfave/cli/v2"
)

func sourceCLICommand() *cli.Command {
	return &cli.Command{
		Name: "source", Usage: "create immutable source bundles",
		Subcommands: []*cli.Command{{
			Name: "pack", Usage: "create a deterministic source bundle from one context directory", ArgsUsage: "<context-directory>",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "target", Required: true, Usage: "tagged output image reference"},
				&cli.StringFlag{Name: "output", Required: true, Usage: "destination zip path"},
			},
			Action: func(c *cli.Context) error {
				if c.NArg() != 1 {
					return fmt.Errorf("source pack requires exactly one context directory")
				}
				target, err := buildcontract.NormalizeTarget(c.String("target"))
				if err != nil {
					return err
				}
				output := filepath.Clean(c.String("output"))
				if err := os.MkdirAll(filepath.Dir(output), 0o755); err != nil {
					return err
				}
				return source.CreateSingleImageArchive(filepath.Clean(c.Args().First()), target, output)
			},
		}, {
			Name: "push", Usage: "package and push a source bundle to an OCI registry", ArgsUsage: "<context-directory|source.zip>",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "repository", Required: true, Usage: "OCI repository tag used to publish the source bundle"},
				&cli.StringFlag{Name: "target", Usage: "output image reference; required for a context directory"},
				&cli.StringSliceFlag{Name: "registry-plain-http", Usage: "registry host using plain HTTP; repeatable and intended for development"},
			},
			Action: func(c *cli.Context) error {
				if c.NArg() != 1 {
					return fmt.Errorf("source push requires exactly one context directory or source zip")
				}
				target := c.String("target")
				if info, err := os.Stat(c.Args().First()); err == nil && info.IsDir() {
					target, err = buildcontract.NormalizeTarget(target)
					if err != nil {
						return err
					}
				}
				archive, cleanup, err := sourceArchive(c.Args().First(), target)
				if err != nil {
					return err
				}
				defer cleanup()
				ref, err := sourcebundle.Push(c.Context, archive, c.String("repository"), c.StringSlice("registry-plain-http"))
				if err != nil {
					return err
				}
				encoder := json.NewEncoder(c.App.Writer)
				encoder.SetIndent("", "  ")
				return encoder.Encode(ref)
			},
		}},
	}
}

func sourceArchive(input, target string) (string, func(), error) {
	info, err := os.Stat(input)
	if err != nil {
		return "", func() {}, err
	}
	if !info.IsDir() {
		if _, err := source.BuildArchiveTargets(input); err != nil {
			return "", func() {}, err
		}
		return filepath.Clean(input), func() {}, nil
	}
	if strings.TrimSpace(target) == "" {
		return "", func() {}, fmt.Errorf("--target is required for a context directory")
	}
	tmp, err := os.CreateTemp("", "kova-source-*.zip")
	if err != nil {
		return "", func() {}, err
	}
	path := tmp.Name()
	if err := tmp.Close(); err != nil {
		os.Remove(path)
		return "", func() {}, err
	}
	cleanup := func() { _ = os.Remove(path) }
	if err := source.CreateSingleImageArchive(filepath.Clean(input), target, path); err != nil {
		cleanup()
		return "", func() {}, err
	}
	return path, cleanup, nil
}
