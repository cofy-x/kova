package daemon

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cofy-x/kova/internal/batch"
	"github.com/cofy-x/kova/internal/logging"
	"github.com/cofy-x/kova/internal/observability"
	"github.com/cofy-x/kova/internal/source"

	"github.com/labstack/echo/v4"
	"go.opentelemetry.io/otel/attribute"
)

func runDaemon(socketPath, defaultAddrs string) error {
	srv := newDaemonServer(defaultAddrs, daemonResultDB, daemonLogsFile, serverBackend{})

	e := echo.New()
	e.HideBanner = true
	e.HidePort = true
	e.Use(observabilityMiddleware)
	e.GET("/api/v1/health", srv.handleHealth)
	e.POST("/api/v1/build", srv.handleBuildPost)
	e.POST("/api/v1/build/cancel", srv.handleBuildCancel)
	e.GET("/api/v1/build/status", srv.handleBuildStatus)
	e.POST("/api/v1/export", srv.handleExport)
	e.POST("/api/v1/preheat", srv.handlePreheat)

	_ = os.Remove(socketPath)
	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", socketPath, err)
	}
	defer ln.Close()

	httpSrv := &http.Server{Handler: e}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		logging.Infof("Received %s, shutting down daemon", sig)
		done, cancelled := srv.cancelActiveBuild("daemon shutdown")
		if cancelled {
			logging.Infof("Cancelled active build for daemon shutdown")
		}
		if done != nil {
			select {
			case <-done:
			case <-time.After(30 * time.Second):
				logging.Errorf("Build did not finish within 30s, forcing shutdown")
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		httpSrv.Shutdown(ctx)
	}()

	logging.Infof("Daemon listening on %s", socketPath)
	if err := httpSrv.Serve(ln); err != http.ErrServerClosed {
		return err
	}
	return nil
}

func newDaemonServer(defaultAddrs string, resultDB string, logsFile string, backend serverBackend) *daemonServer {
	if backend.runBuild == nil {
		backend.runBuild = batch.RunBuild
	}
	if backend.runExport == nil {
		backend.runExport = batch.RunExport
	}
	if backend.runPreheat == nil {
		backend.runPreheat = batch.RunPreheat
	}
	if backend.validateBuildArchive == nil {
		backend.validateBuildArchive = source.ValidateBuildArchive
	}
	if backend.extractZip == nil {
		backend.extractZip = source.ExtractZip
	}
	return &daemonServer{
		defaultAddrs: defaultAddrs,
		resultDB:     resultDB,
		logsFile:     logsFile,
		backend:      backend,
		build:        daemonState{Status: "idle"},
	}
}

func observabilityMiddleware(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		ctx, op := observability.StartOperation(c.Request().Context(), observability.OperationConfig{
			Name: "kova.daemon.http",
			SpanAttrs: []attribute.KeyValue{
				attribute.String(observability.AttrOperation, c.Path()),
				attribute.String("http.request.method", c.Request().Method),
			},
			MetricAttrs: []attribute.KeyValue{
				attribute.String(observability.AttrOperation, c.Path()),
				attribute.String("http.request.method", c.Request().Method),
			},
			Counter: observability.Instrument{Name: "kova_daemon_http_requests_total", Description: "Daemon HTTP requests"},
			Duration: observability.Instrument{
				Name:        "kova_daemon_http_request_duration_seconds",
				Description: "Daemon HTTP request duration",
			},
		})
		req := c.Request().WithContext(ctx)
		c.SetRequest(req)
		err := next(c)
		status := c.Response().Status
		if httpErr, ok := err.(*echo.HTTPError); ok {
			status = httpErr.Code
		}
		if status == 0 {
			status = http.StatusOK
		}
		op.SetHTTPStatusCode(status)
		if err != nil || status >= 500 {
			op.SetResult(observability.ResultError)
		}
		op.End(err)
		return err
	}
}
