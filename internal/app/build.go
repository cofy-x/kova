package app

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cofy-x/kova/internal/batch"
	"github.com/cofy-x/kova/internal/runner"
	"github.com/cofy-x/kova/internal/scheduler"
	"github.com/cofy-x/kova/internal/source"

	cli "github.com/urfave/cli/v2"
)

func buildCLICommand() *cli.Command {
	return &cli.Command{
		Name:  "build",
		Usage: "submit or run a batch image build",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "image-dirs", Usage: "path to a directory whose child directories each contain Dockerfile, metadata.json, and optional context"},
			&cli.StringFlag{Name: "target", Usage: "override a single-directory target or exactly filter a batch metadata target"},
			&cli.StringFlag{Name: "addrs", Usage: "comma-separated buildkitd addresses; hostnames are resolved to IPs for scheduling"},
			&cli.StringSliceFlag{Name: "var", Usage: "replace occurrences of $KEY or ${KEY} in Dockerfile and metadata.json before build; repeatable, format KEY=value"},
			&cli.IntFlag{Name: "concurrency", Value: 1, Usage: "total number of concurrent buildctl invocations across all buildkitd addresses"},
			&cli.BoolFlag{Name: "fail-fast", Usage: "stop immediately after the first buildctl failure"},
			&cli.StringFlag{Name: "format", Value: "nydus", Usage: "build output format: nydus, oci, or both"},
			&cli.DurationFlag{Name: "oom-cooldown", Value: batch.DefaultBuildkitOOMCooldown, Usage: "pause scheduling new tasks for the affected buildkit address after detecting an OOM-style connection refusal"},
			&cli.StringFlag{Name: "result", Value: "result.lmdb", Usage: "path to LMDB result database"},
			&cli.StringFlag{Name: "logs", Value: "logs.jsonl", Usage: "path to JSONL file storing failed build logs"},
			&cli.IntFlag{Name: "timeout", Value: 300, Usage: "kill a buildctl task if it runs longer than the given number of seconds; 0 disables timeout"},
			&cli.IntFlag{Name: "retry", Value: 0, Usage: "number of times to retry a failed build before giving up; 0 disables retry"},
			&cli.BoolFlag{Name: "skip-fail", Usage: "skip targets that previously failed (recorded in LMDB)"},
			&cli.BoolFlag{Name: "verbose", Usage: "stream buildctl subprocess output to the console"},
		},
		Action: func(c *cli.Context) error {
			cfg, err := runnerConfigFromContext(c)
			if err != nil {
				return err
			}
			if cfg.Enabled() {
				return runner.NewClient(cfg).Build(runnerBuildArgsFromContext(c))
			}
			buildOpts, err := buildCLIOptionsFromContext(c)
			if err != nil {
				return err
			}
			addrs, err := scheduler.ParseAddrs(buildOpts.AddrsRaw)
			if err != nil {
				return err
			}
			buildVars, err := source.ParseBuildVariables(buildOpts.Vars)
			if err != nil {
				return err
			}
			for _, addr := range addrs {
				addr.Cooldown = buildOpts.OOMCooldown
			}
			return batch.RunBuild(batch.Options{
				ImageDir:    buildOpts.ImageDir,
				ImageDirs:   buildOpts.ImageDirs,
				Addrs:       addrs,
				AddrsRaw:    buildOpts.AddrsRaw,
				Concurrency: buildOpts.Concurrency,
				Failfast:    buildOpts.Failfast,
				BuildFormat: source.NormalizeBuildFormatValue(buildOpts.BuildFormat),
				OOMCooldown: buildOpts.OOMCooldown,
				ResultPath:  buildOpts.ResultPath,
				LogsPath:    buildOpts.LogsPath,
				Vars:        buildVars,
				Timeout:     buildOpts.Timeout,
				Retry:       buildOpts.Retry,
				Verbose:     buildOpts.Verbose,
				Target:      buildOpts.Target,
				SkipFail:    buildOpts.SkipFail,
			})
		},
	}
}

type buildCLIOptions struct {
	ImageDir    string
	ImageDirs   string
	AddrsRaw    string
	Vars        []string
	Concurrency int
	Failfast    bool
	BuildFormat string
	OOMCooldown time.Duration
	ResultPath  string
	LogsPath    string
	Timeout     int
	Retry       int
	Verbose     bool
	Target      string
	SkipFail    bool
}

func buildCLIOptionsFromContext(c *cli.Context) (buildCLIOptions, error) {
	opts := buildCLIOptions{
		ImageDirs:   strings.TrimSpace(c.String("image-dirs")),
		AddrsRaw:    c.String("addrs"),
		Vars:        append([]string{}, c.StringSlice("var")...),
		Concurrency: c.Int("concurrency"),
		Failfast:    c.Bool("fail-fast"),
		BuildFormat: c.String("format"),
		OOMCooldown: c.Duration("oom-cooldown"),
		ResultPath:  c.String("result"),
		LogsPath:    c.String("logs"),
		Timeout:     c.Int("timeout"),
		Retry:       c.Int("retry"),
		Verbose:     c.Bool("verbose"),
		Target:      strings.TrimSpace(c.String("target")),
		SkipFail:    c.Bool("skip-fail"),
	}
	if err := parseBuildTrailingArgs(c.Args().Slice(), &opts); err != nil {
		return buildCLIOptions{}, err
	}
	return opts, nil
}

func parseBuildTrailingArgs(args []string, opts *buildCLIOptions) error {
	for i := 0; i < len(args); {
		arg := strings.TrimSpace(args[i])
		if arg == "" {
			i++
			continue
		}
		if strings.HasPrefix(arg, "--") {
			next, err := applyBuildTrailingFlag(args, i, opts)
			if err != nil {
				return err
			}
			i = next
			continue
		}
		if err := applyBuildPositional(arg, opts); err != nil {
			return err
		}
		i++
	}
	return nil
}

func applyBuildTrailingFlag(args []string, i int, opts *buildCLIOptions) (int, error) {
	raw := strings.TrimPrefix(args[i], "--")
	name, value, hasValue := strings.Cut(raw, "=")
	name = strings.TrimSpace(name)
	requireValue := func() (string, int, error) {
		if hasValue {
			return value, i + 1, nil
		}
		if i+1 >= len(args) {
			return "", i, fmt.Errorf("--%s requires a value", name)
		}
		return args[i+1], i + 2, nil
	}

	switch name {
	case "image-dirs":
		v, next, err := requireValue()
		if err != nil {
			return i, err
		}
		opts.ImageDirs = strings.TrimSpace(v)
		return next, nil
	case "target":
		v, next, err := requireValue()
		if err != nil {
			return i, err
		}
		if opts.Target != "" {
			return i, fmt.Errorf("build accepts --target at most once")
		}
		opts.Target = strings.TrimSpace(v)
		return next, nil
	case "addrs":
		v, next, err := requireValue()
		if err != nil {
			return i, err
		}
		opts.AddrsRaw = v
		return next, nil
	case "var":
		v, next, err := requireValue()
		if err != nil {
			return i, err
		}
		opts.Vars = append(opts.Vars, v)
		return next, nil
	case "format":
		v, next, err := requireValue()
		if err != nil {
			return i, err
		}
		opts.BuildFormat = v
		return next, nil
	case "concurrency", "timeout", "retry":
		v, next, err := requireValue()
		if err != nil {
			return i, err
		}
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			return i, fmt.Errorf("--%s must be an integer", name)
		}
		switch name {
		case "concurrency":
			opts.Concurrency = n
		case "timeout":
			opts.Timeout = n
		case "retry":
			opts.Retry = n
		}
		return next, nil
	case "oom-cooldown":
		v, next, err := requireValue()
		if err != nil {
			return i, err
		}
		d, err := time.ParseDuration(strings.TrimSpace(v))
		if err != nil {
			return i, fmt.Errorf("--oom-cooldown must be a duration")
		}
		opts.OOMCooldown = d
		return next, nil
	case "result":
		v, next, err := requireValue()
		if err != nil {
			return i, err
		}
		opts.ResultPath = v
		return next, nil
	case "logs":
		v, next, err := requireValue()
		if err != nil {
			return i, err
		}
		opts.LogsPath = v
		return next, nil
	case "fail-fast", "skip-fail", "verbose":
		v, err := trailingBoolValue(name, value, hasValue)
		if err != nil {
			return i, err
		}
		switch name {
		case "fail-fast":
			opts.Failfast = v
		case "skip-fail":
			opts.SkipFail = v
		case "verbose":
			opts.Verbose = v
		}
		return i + 1, nil
	default:
		return i, fmt.Errorf("unknown build flag: --%s", name)
	}
}

func trailingBoolValue(name, value string, hasValue bool) (bool, error) {
	if !hasValue {
		return true, nil
	}
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "1", "true", "yes":
		return true, nil
	case "0", "false", "no":
		return false, nil
	default:
		return false, fmt.Errorf("--%s must be a boolean", name)
	}
}

func applyBuildPositional(value string, opts *buildCLIOptions) error {
	if isExistingDir(value) {
		if opts.ImageDir != "" {
			return fmt.Errorf("build accepts at most one build directory")
		}
		if opts.ImageDirs != "" {
			return fmt.Errorf("build accepts either --image-dirs or a single build directory, not both")
		}
		opts.ImageDir = value
		return nil
	}
	if opts.Target != "" {
		return fmt.Errorf("build accepts either --target or a positional target filter, not both")
	}
	if opts.ImageDir != "" {
		return fmt.Errorf("build accepts a target for a single build directory only with --target")
	}
	opts.Target = strings.TrimSpace(value)
	return nil
}

func isExistingDir(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}
