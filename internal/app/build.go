package app

import (
	"time"

	"github.com/cofy-x/kova/internal/runner"

	cli "github.com/urfave/cli/v2"
)

const defaultBuildkitOOMCooldown = 2 * time.Minute

func buildCLICommand() *cli.Command {
	return &cli.Command{
		Name:  "build",
		Usage: "submit a batch image build to the selected runner",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "target", Usage: "override a single-directory target or exactly filter a batch metadata target"},
			&cli.StringSliceFlag{Name: "var", Usage: "replace occurrences of $KEY or ${KEY} in Dockerfile and metadata.json before build; repeatable, format KEY=value"},
			&cli.IntFlag{Name: "concurrency", Value: 1, Usage: "total number of concurrent buildctl invocations across all buildkitd addresses"},
			&cli.BoolFlag{Name: "fail-fast", Usage: "stop immediately after the first buildctl failure"},
			&cli.StringFlag{Name: "format", Value: "nydus", Usage: "build output format: nydus, oci, or both"},
			&cli.DurationFlag{Name: "oom-cooldown", Value: defaultBuildkitOOMCooldown, Usage: "pause scheduling new tasks for an affected BuildKit address after an OOM-style connection refusal"},
			&cli.IntFlag{Name: "timeout", Value: 300, Usage: "kill a buildctl task if it runs longer than the given number of seconds; 0 disables timeout"},
			&cli.IntFlag{Name: "retry", Value: 0, Usage: "number of times to retry a failed build before giving up; 0 disables retry"},
			&cli.BoolFlag{Name: "skip-fail", Usage: "skip targets previously recorded as failed by the runner"},
			&cli.BoolFlag{Name: "verbose", Usage: "stream buildctl subprocess output to the console"},
		},
		Action: func(c *cli.Context) error {
			cfg, err := runnerConfigFromContext(c)
			if err != nil {
				return err
			}
			return runner.NewClient(cfg).Build(runnerBuildArgsFromContext(c))
		},
	}
}
