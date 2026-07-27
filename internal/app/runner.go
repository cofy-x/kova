package app

import (
	"fmt"
	"os"
	"strings"

	"github.com/cofy-x/kova/internal/ctxconfig"
	"github.com/cofy-x/kova/internal/runner"
	"github.com/cofy-x/kova/internal/source"

	cli "github.com/urfave/cli/v2"
)

func prepareCLICommand() *cli.Command {
	return &cli.Command{
		Name:  "prepare",
		Usage: "create Pod running kovad daemon and wait until ready",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "image", Usage: "runner image"},
			&cli.StringFlag{Name: "image-pull-policy", Value: runner.DefaultConfig().RunnerImagePullPolicy, Usage: "runner image pull policy"},
			&cli.StringFlag{Name: "image-pull-secret", Value: runner.DefaultConfig().ImagePullSecret, Usage: "runner image pull secret name"},
		},
		Action: func(c *cli.Context) error {
			cfg, err := runnerConfigFromContext(c)
			if err != nil {
				return err
			}
			ctx, hasCtx, err := runnerCtxFromCLI(c)
			if err != nil {
				return err
			}
			image, imagePullPolicy, imagePullSecret := prepareImageOptionsFromContext(c, ctx, hasCtx)
			return runner.NewClient(cfg).Prepare(image, imagePullPolicy, imagePullSecret)
		},
	}
}

func listCLICommand() *cli.Command {
	return &cli.Command{
		Name:            "list",
		Usage:           "list runner namespace Pods; supports -o wide",
		SkipFlagParsing: true,
		Action: func(c *cli.Context) error {
			cfg, err := runnerConfigFromContext(c)
			if err != nil {
				return err
			}
			return runner.NewClient(cfg).List(stripDashDash(c.Args().Slice()))
		},
	}
}

func scaleCLICommand() *cli.Command {
	return &cli.Command{
		Name:  "scale",
		Usage: "scale deployment/kova or show kova pods",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "target", Usage: "desired kova replica count"},
		},
		Action: func(c *cli.Context) error {
			cfg, err := runnerConfigFromContext(c)
			if err != nil {
				return err
			}
			return runner.NewClient(cfg).Scale(c.String("target"))
		},
	}
}

func statusCLICommand() *cli.Command {
	return &cli.Command{
		Name:  "status",
		Usage: "print current build status JSON from the selected Pod",
		Action: func(c *cli.Context) error {
			cfg, err := runnerConfigFromContext(c)
			if err != nil {
				return err
			}
			return runner.NewClient(cfg).Status()
		},
	}
}

func waitCLICommand() *cli.Command {
	return &cli.Command{
		Name:  "wait",
		Usage: "wait until the current build leaves running state",
		Flags: []cli.Flag{
			&cli.IntFlag{Name: "timeout", Value: 0, Usage: "maximum seconds to wait; 0 disables timeout"},
			&cli.IntFlag{Name: "interval", Value: runner.DefaultConfig().WaitBuildIntervalSeconds, Usage: "poll interval in seconds"},
		},
		Action: func(c *cli.Context) error {
			cfg, err := runnerConfigFromContext(c)
			if err != nil {
				return err
			}
			return runner.NewClient(cfg).Wait(c.Int("timeout"), c.Int("interval"))
		},
	}
}

func logsCLICommand() *cli.Command {
	return &cli.Command{
		Name:            "logs",
		Usage:           "stream selected runner Pod logs; supports --tail",
		SkipFlagParsing: true,
		Action: func(c *cli.Context) error {
			cfg, err := runnerConfigFromContext(c)
			if err != nil {
				return err
			}
			return runner.NewClient(cfg).Logs(stripDashDash(c.Args().Slice()))
		},
	}
}

func execCLICommand() *cli.Command {
	return &cli.Command{
		Name:            "exec",
		Usage:           "execute a command in the selected runner Pod",
		SkipFlagParsing: true,
		Action: func(c *cli.Context) error {
			cfg, err := runnerConfigFromContext(c)
			if err != nil {
				return err
			}
			return runner.NewClient(cfg).Exec(stripDashDash(c.Args().Slice()))
		},
	}
}

func destroyCLICommand() *cli.Command {
	return &cli.Command{
		Name:  "destroy",
		Usage: "delete the prepared Pod and clear local state",
		Action: func(c *cli.Context) error {
			cfg, err := runnerConfigFromContext(c)
			if err != nil {
				return err
			}
			return runner.NewClient(cfg).Destroy()
		},
	}
}

func runnerConfigFromContext(c *cli.Context) (runner.Config, error) {
	cfg := runner.DefaultConfig()
	ctx, hasCtx, err := runnerCtxFromCLI(c)
	if err != nil {
		return runner.Config{}, err
	}
	cfg.Kubeconfig = valueFromFlagEnvCtx(c, "kubeconfig", "KOVA_KUBECONFIG", cfg.Kubeconfig, ctx.Kubeconfig, hasCtx)
	cfg.Namespace = valueFromFlagEnvCtx(c, "namespace", "KOVA_NAMESPACE", cfg.Namespace, ctx.Namespace, hasCtx)
	cfg.PodName = strings.TrimSpace(c.String("name"))
	cfg.WaitTimeout = strings.TrimSpace(c.String("wait"))
	cfg.BuildkitAddr = valueFromFlagEnvCtx(c, "buildkit-addr", "KOVA_BUILDKIT_ADDR", cfg.BuildkitAddr, ctx.BuildkitAddr, hasCtx)
	return cfg, nil
}

func runnerCtxFromCLI(c *cli.Context) (ctxconfig.Context, bool, error) {
	cfg, err := loadCtxConfig(c)
	if err != nil {
		return ctxconfig.Context{}, false, err
	}
	_, ctx, ok := cfg.Resolve(c.String("ctx"))
	if !ok {
		if strings.TrimSpace(c.String("ctx")) != "" {
			return ctxconfig.Context{}, false, fmt.Errorf("ctx %q does not exist", c.String("ctx"))
		}
		if strings.TrimSpace(cfg.Current) != "" {
			return ctxconfig.Context{}, false, fmt.Errorf("current ctx %q does not exist", cfg.Current)
		}
		return ctxconfig.Context{}, false, nil
	}
	return ctx, true, nil
}

func valueFromFlagEnvCtx(c *cli.Context, flagName string, envName string, current string, ctxValue string, hasCtx bool) string {
	if c.IsSet(flagName) {
		return strings.TrimSpace(c.String(flagName))
	}
	if strings.TrimSpace(envName) != "" && strings.TrimSpace(envValue(envName)) != "" {
		return strings.TrimSpace(current)
	}
	if hasCtx && strings.TrimSpace(ctxValue) != "" {
		return strings.TrimSpace(ctxValue)
	}
	return strings.TrimSpace(current)
}

func prepareImageOptionsFromContext(c *cli.Context, ctx ctxconfig.Context, hasCtx bool) (string, string, string) {
	image := valueFromFlagEnvCtx(c, "image", "KOVA_IMAGE", runner.DefaultConfig().RunnerImage, ctx.RunnerImage, hasCtx)
	imagePullPolicy := valueFromFlagEnvCtx(c, "image-pull-policy", "KOVA_IMAGE_PULL_POLICY", runner.DefaultConfig().RunnerImagePullPolicy, ctx.RunnerImagePullPolicy, hasCtx)
	imagePullSecret := valueFromPrepareSecret(c, ctx, hasCtx)
	return image, imagePullPolicy, imagePullSecret
}

func valueFromPrepareSecret(c *cli.Context, ctx ctxconfig.Context, hasCtx bool) string {
	if c.IsSet("image-pull-secret") {
		return strings.TrimSpace(c.String("image-pull-secret"))
	}
	if strings.TrimSpace(envValue("KOVA_IMAGE_PULL_SECRET")) != "" {
		return strings.TrimSpace(runner.DefaultConfig().ImagePullSecret)
	}
	if hasCtx {
		return strings.TrimSpace(ctx.ImagePullSecret)
	}
	return strings.TrimSpace(runner.DefaultConfig().ImagePullSecret)
}

func envValue(name string) string {
	return strings.TrimSpace(os.Getenv(name))
}

func runnerBuildArgsFromContext(c *cli.Context) []string {
	var args []string
	for _, value := range c.StringSlice("var") {
		args = append(args, "--var", value)
	}
	if target := strings.TrimSpace(c.String("target")); target != "" {
		args = append(args, "--target", target)
	}
	if c.Bool("fail-fast") {
		args = append(args, "--fail-fast")
	}
	if format := source.NormalizeBuildFormatValue(c.String("format")); format != "nydus" {
		args = append(args, "--format", format)
	}
	if c.Bool("skip-fail") {
		args = append(args, "--skip-fail")
	}
	if c.Bool("verbose") {
		args = append(args, "--verbose")
	}
	args = append(args,
		"--concurrency", fmt.Sprint(c.Int("concurrency")),
		"--oom-cooldown", c.Duration("oom-cooldown").String(),
		"--timeout", fmt.Sprint(c.Int("timeout")),
		"--retry", fmt.Sprint(c.Int("retry")),
	)
	args = append(args, c.Args().Slice()...)
	return args
}

func runnerExportArgsFromContext(c *cli.Context) []string {
	args := []string{"--result", c.String("result")}
	for _, target := range c.StringSlice("target") {
		args = append(args, "--target", target)
	}
	if c.Bool("oci") {
		args = append(args, "--oci")
	}
	if c.Bool("with-fail") {
		args = append(args, "--with-fail")
	}
	return args
}

func runnerPreheatArgsFromContext(c *cli.Context) []string {
	var args []string
	if value := strings.TrimSpace(c.String("target")); value != "" {
		args = append(args, "--target", value)
	}
	if value := strings.TrimSpace(c.String("dragonfly-scheduler-addr")); value != "" {
		args = append(args, "--dragonfly-scheduler-addr", value)
	}
	args = append(args,
		"--concurrency", fmt.Sprint(c.Int("concurrency")),
		"--interval", fmt.Sprint(c.Int("interval")),
		"--timeout", fmt.Sprint(c.Int("timeout")),
	)
	if c.Bool("fail-fast") {
		args = append(args, "--fail-fast")
	}
	if c.Bool("oci") {
		args = append(args, "--oci")
	}
	if c.Bool("verbose") {
		args = append(args, "--verbose")
	}
	if c.Bool("insecure-skip-verify") {
		args = append(args, "--insecure-skip-verify", "true")
	}
	return args
}

func stripDashDash(args []string) []string {
	if len(args) > 0 && args[0] == "--" {
		return args[1:]
	}
	return args
}
