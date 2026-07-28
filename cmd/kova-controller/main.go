package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/cofy-x/kova/internal/logging"
	"github.com/cofy-x/kova/internal/observability"
	"github.com/cofy-x/kova/internal/service"
	"github.com/cofy-x/kova/internal/version"

	"github.com/urfave/cli/v2"
)

func main() {
	logging.ResetCommandStartTime(time.Now())
	exitCode := 0
	otelHandle, err := observability.Init(context.Background(), observability.ConfigFromEnv(
		observability.WithServiceName("kova-controller"),
		observability.WithComponent("controller"),
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
	app := &cli.App{
		Name:    "kova-controller",
		Usage:   "Kova service API and build controller",
		Version: version.Version,
		Commands: []*cli.Command{
			{
				Name:  "version",
				Usage: "print version and build information",
				Action: func(c *cli.Context) error {
					_, err := fmt.Fprintln(c.App.Writer, version.String("kova-controller"))
					return err
				},
			},
			service.CLICommand(),
		},
	}
	if err := app.Run(os.Args); err != nil {
		logging.Errorf("%v", err)
		exitCode = 1
	}
}
