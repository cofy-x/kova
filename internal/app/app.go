package app

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"regexp"
	"strings"

	"github.com/cofy-x/kova/internal/ctxconfig"
	"github.com/cofy-x/kova/internal/logging"
	"github.com/cofy-x/kova/internal/runner"
	"github.com/cofy-x/kova/internal/version"

	cli "github.com/urfave/cli/v2"
)

const pprofServerEnv = "KOVA_PPROF_SERVER"

var undefinedFlagPattern = regexp.MustCompile(`flag provided but not defined: -+([A-Za-z0-9][A-Za-z0-9-]*)`)

func NewCLIApp() *cli.App {
	commands := withUsageErrorHint([]*cli.Command{
		versionCLICommand(),
		doctorCLICommand(),
		jobCLICommand(),
		prepareCLICommand(),
		listCLICommand(),
		scaleCLICommand(),
		buildCLICommand(),
		statusCLICommand(),
		waitCLICommand(),
		logsCLICommand(),
		execCLICommand(),
		ctxCLICommand(),
		exportCLICommand(),
		preheatCLICommand(),
		destroyCLICommand(),
	})
	return &cli.App{
		Name:    "kova",
		Usage:   "distributed image build client for build, export, and preheat workflows",
		Version: version.Version,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "pprof-server",
				Usage: "listen address for net/http/pprof server, e.g. 0.0.0.0:5241",
			},
			&cli.StringFlag{Name: "ctx", EnvVars: []string{"KOVA_CTX"}, Usage: "Kova context name from local config"},
			&cli.StringFlag{Name: "ctx-config", EnvVars: []string{"KOVA_CTX_CONFIG"}, Usage: "path to local Kova context config"},
			&cli.StringFlag{Name: "kubeconfig", Usage: "path to kubeconfig for local runner operations"},
			&cli.StringFlag{Name: "namespace", Value: runner.DefaultConfig().Namespace, Usage: "Kubernetes namespace for the runner Pod"},
			&cli.StringFlag{Name: "name", Usage: "runner Pod name for local runner operations"},
			&cli.StringFlag{Name: "wait", Value: runner.DefaultConfig().WaitTimeout, Usage: "timeout used by prepare when waiting for Pod readiness"},
			&cli.StringFlag{Name: "buildkit-addr", Value: runner.DefaultConfig().BuildkitAddr, Usage: "BuildKit address passed to runner daemon and build requests"},
			&cli.StringFlag{Name: "service-url", EnvVars: []string{"KOVA_SERVICE_URL"}, Usage: "Kova service base URL"},
			&cli.StringFlag{Name: "service-ca-file", EnvVars: []string{"KOVA_SERVICE_CA_FILE"}, Usage: "CA bundle for the Kova service"},
			&cli.BoolFlag{Name: "service-insecure", EnvVars: []string{"KOVA_SERVICE_INSECURE"}, Usage: "skip Kova service TLS verification"},
		},
		OnUsageError: usageErrorWithGlobalFlagHint,
		Before: func(c *cli.Context) error {
			if addr := ResolvePprofServerAddr(c.String("pprof-server")); addr != "" {
				go func() {
					logging.Infof("Starting pprof server on %s", addr)
					if err := http.ListenAndServe(addr, nil); err != nil {
						logging.Errorf("pprof server: %v", err)
					}
				}()
			}
			return nil
		},
		Commands: commands,
	}
}

func withUsageErrorHint(commands []*cli.Command) []*cli.Command {
	for _, command := range commands {
		command.OnUsageError = usageErrorWithGlobalFlagHint
		command.Subcommands = withUsageErrorHint(command.Subcommands)
	}
	return commands
}

func usageErrorWithGlobalFlagHint(c *cli.Context, err error, isSubcommand bool) error {
	if err == nil {
		return nil
	}
	flagName, ok := misplacedGlobalFlagName(err.Error())
	if !ok || !isSubcommand || c == nil {
		return err
	}
	command := commandPath(c)
	if command == "" {
		return err
	}
	return fmt.Errorf("%w\n\nHint: --%s is a global flag. Put it before the command:\n  kova --%s <value> %s", err, flagName, flagName, command)
}

func commandPath(c *cli.Context) string {
	var names []string
	lineage := c.Lineage()
	for i := len(lineage) - 1; i >= 0; i-- {
		if lineage[i].Command == nil {
			continue
		}
		name := strings.TrimSpace(lineage[i].Command.Name)
		if name == "" || name == "kova" {
			continue
		}
		names = append(names, name)
	}
	return strings.Join(names, " ")
}

func misplacedGlobalFlagName(message string) (string, bool) {
	match := undefinedFlagPattern.FindStringSubmatch(message)
	if len(match) != 2 {
		return "", false
	}
	name := match[1]
	switch name {
	case "ctx", "ctx-config", "kubeconfig", "namespace", "name", "wait", "buildkit-addr", "service-url", "service-ca-file", "service-insecure":
		return name, true
	default:
		return "", false
	}
}

func loadCtxConfig(c *cli.Context) (ctxconfig.Config, error) {
	return ctxconfig.Load(c.String("ctx-config"))
}

func ResolvePprofServerAddr(flagValue string) string {
	if trimmed := strings.TrimSpace(flagValue); trimmed != "" {
		return trimmed
	}
	return strings.TrimSpace(os.Getenv(pprofServerEnv))
}

func exportCLICommand() *cli.Command {
	return &cli.Command{
		Name:  "export",
		Usage: "export stored build results as JSONL",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "result", Value: "result.jsonl", Usage: "path to write exported JSONL"},
			&cli.StringSliceFlag{Name: "target", Usage: "export only the exact image target; repeat for multiple targets"},
			&cli.BoolFlag{Name: "oci", Usage: "export only OCI (non-nydus) targets without suffix filtering changes"},
			&cli.BoolFlag{Name: "with-fail", Usage: "also export failed targets"},
		},
		Action: func(c *cli.Context) error {
			cfg, err := runnerConfigFromContext(c)
			if err != nil {
				return err
			}
			if c.NArg() != 0 {
				return fmt.Errorf("export does not accept positional arguments")
			}
			return runner.NewClient(cfg).Export(runnerExportArgsFromContext(c))
		},
	}
}

func preheatCLICommand() *cli.Command {
	return &cli.Command{
		Name:  "preheat",
		Usage: "preheat built images through Dragonfly",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "target", Usage: "preheat only the exact image target"},
			&cli.StringFlag{Name: "dragonfly-scheduler-addr", Usage: "Dragonfly Scheduler gRPC address (e.g. 10.1.70.58:8002)"},
			&cli.IntFlag{Name: "concurrency", Value: 1, Usage: "number of concurrent grpcurl invocations"},
			&cli.IntFlag{Name: "interval", Value: 5, Usage: "minimum interval in seconds between starting grpcurl preheat tasks; 0 disables throttling"},
			&cli.IntFlag{Name: "timeout", Value: 5, Usage: "kill a grpcurl task if it runs longer than the given number of seconds; 0 disables timeout"},
			&cli.BoolFlag{Name: "fail-fast", Usage: "stop immediately after the first preheat failure"},
			&cli.BoolFlag{Name: "oci", Usage: "only preheat OCI (non-nydus) targets"},
			&cli.BoolFlag{Name: "verbose", Usage: "stream grpcurl subprocess output to the console"},
			&cli.BoolFlag{Name: "insecure-skip-verify", Usage: "skip registry TLS verification in Dragonfly preheat requests"},
		},
		Action: func(c *cli.Context) error {
			cfg, err := runnerConfigFromContext(c)
			if err != nil {
				return err
			}
			if c.NArg() != 0 {
				return fmt.Errorf("preheat does not accept positional arguments")
			}
			return runner.NewClient(cfg).Preheat(runnerPreheatArgsFromContext(c))
		},
	}
}
