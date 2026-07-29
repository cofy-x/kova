package httpapi

import (
	"context"
	"net/http"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/artifactstore"
	"github.com/cofy-x/kova/internal/kube"
	"github.com/cofy-x/kova/internal/logging"
	"github.com/cofy-x/kova/internal/observability"
	serviceauth "github.com/cofy-x/kova/internal/service/auth"
	"github.com/cofy-x/kova/internal/service/config"
	"github.com/cofy-x/kova/internal/serviceapi"
	"github.com/cofy-x/kova/internal/version"

	"github.com/labstack/echo/v4"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type kubeAPI interface {
	kube.API
}

type Server struct {
	cfg    config.Config
	kube   kubeAPI
	client client.Client
	reader client.Reader
	store  artifactstore.Store
	auth   serviceauth.Authenticator
	authz  serviceauth.Authorizer
}

var (
	authDenied      = observability.Int64Counter("kova.service.auth.denied", "Rejected service API authentication attempts")
	authzDenied     = observability.Int64Counter("kova.service.authorization.denied", "Rejected service API authorization attempts")
	artifactWrites  = observability.Int64Counter("kova.service.artifact.writes", "Artifact write attempts")
	artifactLatency = observability.DurationHistogram("kova.service.artifact.write.duration", "Artifact write latency")
	buildCancels    = observability.Int64Counter("kova.service.job.cancellations", "Accepted job cancellations")
)

func NewServer(cfg config.Config, kube kubeAPI, crClient client.Client, crReader client.Reader, store artifactstore.Store, authenticator serviceauth.Authenticator, authorizer serviceauth.Authorizer) *Server {
	if cfg.Listen == "" {
		cfg.Listen = ":8080"
	}
	if cfg.ArtifactRoot == "" {
		cfg.ArtifactRoot = artifactstore.DefaultRoot
	}
	if cfg.ArtifactDriver == "" {
		cfg.ArtifactDriver = artifactstore.DriverFilesystem
	}
	if cfg.JobTTL == 0 {
		cfg.JobTTL = 2 * time.Hour
	}
	if cfg.MaxUploadBytes == 0 {
		cfg.MaxUploadBytes = 1 << 30
	}
	if cfg.MaxLogBytes == 0 {
		cfg.MaxLogBytes = 16 << 20
	}
	if cfg.WaitTimeout == 0 {
		cfg.WaitTimeout = 3 * time.Minute
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 5 * time.Second
	}
	if crReader == nil {
		crReader = crClient
	}
	if store == nil {
		store, _ = artifactstore.NewFilesystem(cfg.ArtifactRoot)
	}
	return &Server{cfg: cfg, kube: kube, client: crClient, reader: crReader, store: store, auth: authenticator, authz: authorizer}
}

func (s *Server) Start(ctx context.Context) error {
	e := s.routes()
	httpSrv := &http.Server{Addr: s.cfg.Listen, Handler: e}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdownCtx)
	}()
	logging.Infof("Kova service listening on %s", s.cfg.Listen)
	if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *Server) routes() *echo.Echo {
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true
	e.GET("/healthz", func(c echo.Context) error {
		return c.JSON(http.StatusOK, map[string]string{"status": "ok"})
	})
	e.GET("/version", func(c echo.Context) error {
		return c.JSON(http.StatusOK, serviceapi.VersionInfo{
			APIVersion: serviceapi.APIVersion, Version: version.Version,
			Commit: version.Commit, BuildDate: version.BuildDate,
		})
	})
	e.GET("/readyz", func(c echo.Context) error {
		var builds kovav1.KovaBuildList
		if err := s.reader.List(c.Request().Context(), &builds, client.InNamespace(s.cfg.Namespace), client.Limit(1)); err != nil {
			return c.JSON(http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
		}
		return c.JSON(http.StatusOK, map[string]string{"status": "ready"})
	})
	v1 := e.Group("/v1", s.authMiddleware)
	v1.POST("/builds", s.handleCreateBuild)
	v1.GET("/builds", s.handleListBuilds)
	v1.GET("/builds/:id", s.handleGetBuild)
	v1.GET("/builds/:id/results", s.handleBuildResults)
	v1.GET("/builds/:id/logs", s.handleBuildLogs)
	v1.POST("/builds/:id/cancel", s.handleCancelBuild)
	v1.DELETE("/builds/:id", s.handleCancelBuild)
	v1.POST("/builds/:id/export", s.handleExportBuild)
	v1.POST("/builds/:id/preheat", s.handlePreheatBuild)
	return e
}
