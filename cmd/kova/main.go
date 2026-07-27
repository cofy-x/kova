package main

import (
	"context"
	"os"
	"time"

	"github.com/cofy-x/kova/internal/app"
	"github.com/cofy-x/kova/internal/logging"
	"github.com/cofy-x/kova/internal/observability"
)

func main() {
	logging.ResetCommandStartTime(time.Now())
	exitCode := 0
	otelHandle, err := observability.Init(context.Background(), observability.ConfigFromEnv(
		observability.WithServiceName("kova-client"),
		observability.WithComponent("client"),
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
	cliApp := app.NewCLIApp()
	if err := cliApp.Run(os.Args); err != nil {
		logging.Errorf("%v", err)
		exitCode = 1
		return
	}
}
