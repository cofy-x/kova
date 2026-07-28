package main

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"
	"time"

	"github.com/cofy-x/kova/internal/artifactcmd"
	"github.com/cofy-x/kova/internal/daemon"
	"github.com/cofy-x/kova/internal/logging"
	"github.com/cofy-x/kova/internal/observability"
	"github.com/cofy-x/kova/internal/version"

	cli "github.com/urfave/cli/v2"
)

const pprofServerEnv = "KOVA_PPROF_SERVER"

func main() {
	logging.ResetCommandStartTime(time.Now())
	exitCode := 0
	otelHandle, err := observability.Init(context.Background(), observability.ConfigFromEnv(
		observability.WithServiceName("kovad"),
		observability.WithComponent("runner"),
	))
	if err != nil {
		logging.Errorf("initialize observability: %v", err)
		os.Exit(1)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := otelHandle.Shutdown(ctx); err != nil {
			logging.Errorf("shutdown observability: %v", err)
		}
		if exitCode != 0 {
			os.Exit(exitCode)
		}
	}()
	app := newCLIApp()
	if err := app.Run(os.Args); err != nil {
		logging.Errorf("%v", err)
		exitCode = 1
		return
	}
}

func newCLIApp() *cli.App {
	daemonCommand := daemon.CLICommand()
	daemonCommand.Flags = append(daemonCommand.Flags, &cli.StringFlag{
		Name:  "pprof-server",
		Usage: "listen address for net/http/pprof server, e.g. 0.0.0.0:5241",
	})
	daemonCommand.Before = func(c *cli.Context) error {
		if addr := resolvePprofServerAddr(c.String("pprof-server")); addr != "" {
			go func() {
				logging.Infof("Starting pprof server on %s", addr)
				if err := http.ListenAndServe(addr, nil); err != nil {
					logging.Errorf("pprof server: %v", err)
				}
			}()
		}
		return nil
	}

	return &cli.App{
		Name:    "kovad",
		Usage:   "Kova runtime daemon entrypoint",
		Version: version.Version,
		Commands: []*cli.Command{
			{
				Name:  "version",
				Usage: "print version and build information",
				Action: func(c *cli.Context) error {
					_, err := fmt.Fprintln(c.App.Writer, version.String("kovad"))
					return err
				},
			},
			artifactcmd.CLICommand(),
			daemon.TransportCLICommand(),
			daemonCommand,
		},
	}
}

func resolvePprofServerAddr(flagValue string) string {
	if trimmed := strings.TrimSpace(flagValue); trimmed != "" {
		return trimmed
	}
	return strings.TrimSpace(os.Getenv(pprofServerEnv))
}
