package httpapi

import (
	"context"
	"fmt"
	"net/http"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/kube"
	"github.com/cofy-x/kova/internal/logging"
	"github.com/cofy-x/kova/internal/observability"
	serviceauth "github.com/cofy-x/kova/internal/service/auth"
	"github.com/cofy-x/kova/internal/service/buildcontroller"
	"github.com/cofy-x/kova/internal/service/config"
	"github.com/cofy-x/kova/internal/service/queueadmission"
	"github.com/cofy-x/kova/internal/version"
	apiv1 "github.com/cofy-x/kova/pkg/api/v1"

	"github.com/labstack/echo/v4"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type kubeAPI interface {
	kube.API
}

type Server struct {
	cfg             config.Config
	kube            kubeAPI
	client          client.Client
	reader          client.Reader
	readinessReader client.Reader
	auth            serviceauth.Authenticator
	authz           serviceauth.Authorizer
}

var (
	authDenied             = observability.Int64Counter("kova.service.auth.denied", "Rejected service API authentication attempts")
	authzDenied            = observability.Int64Counter("kova.service.authorization.denied", "Rejected service API authorization attempts")
	reviewUnavailableCount = observability.Int64Counter("kova.service.identity.review_unavailable", "Service API identity review outcomes that Kubernetes could not determine")
	buildCancels           = observability.Int64Counter("kova.service.job.cancellations", "Accepted job cancellations")
)

const (
	serviceReadHeaderTimeout = 5 * time.Second
	serviceReadTimeout       = 30 * time.Second
	serviceIdleTimeout       = time.Minute
)

func NewServer(cfg config.Config, kube kubeAPI, crClient client.Client, crReader client.Reader, readinessReader client.Reader, authenticator serviceauth.Authenticator, authorizer serviceauth.Authorizer) *Server {
	if cfg.Listen == "" {
		cfg.Listen = ":8080"
	}
	if cfg.JobTTL == 0 {
		cfg.JobTTL = 2 * time.Hour
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
	if readinessReader == nil {
		readinessReader = crReader
	}
	return &Server{cfg: cfg, kube: kube, client: crClient, reader: crReader, readinessReader: readinessReader, auth: authenticator, authz: authorizer}
}

func (s *Server) queueStore() queueadmission.Store {
	return s.queueStoreWithReader(s.reader)
}

func (s *Server) queueStoreWithReader(reader client.Reader) queueadmission.Store {
	return queueadmission.Store{
		Client: s.client, Reader: reader, Namespace: s.cfg.Namespace,
		GlobalLimit: s.cfg.MaxQueuedJobs, RequesterLimit: s.cfg.MaxQueuedJobsPerRequester,
	}
}

func (s *Server) Start(ctx context.Context) error {
	if err := s.initializeAdmission(ctx); err != nil {
		return err
	}
	httpSrv := s.httpServer()
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

func (s *Server) httpServer() *http.Server {
	return &http.Server{
		Addr:              s.cfg.Listen,
		Handler:           s.routes(),
		ReadHeaderTimeout: serviceReadHeaderTimeout,
		ReadTimeout:       serviceReadTimeout,
		IdleTimeout:       serviceIdleTimeout,
		// A write deadline would also cover potentially slow Kubernetes calls
		// after a mutation has begun. Until those operations have a separate
		// bounded response contract, do not turn an accepted build into an
		// indistinguishable transport timeout here.
	}
}

func (s *Server) initializeAdmission(ctx context.Context) error {
	queue := s.queueStore()
	activeErr := s.checkActiveAdmissionLedger(ctx)
	queueErr := queue.CheckReady(ctx)
	if activeErr == nil {
		if queueErr == nil {
			return nil
		}
		if !apierrors.IsNotFound(queueErr) {
			return queueErr
		}
		return s.waitAdmissionBootstrap(ctx, queue)
	}
	if !apierrors.IsNotFound(activeErr) {
		return activeErr
	}
	if queueErr == nil {
		// These are separate authoritative GETs, not an atomic snapshot.
		// A concurrent first start can finish between them.
		if err := s.checkActiveAdmissionLedger(ctx); err == nil {
			return nil
		} else if !apierrors.IsNotFound(err) {
			return err
		}
		return fmt.Errorf("active admission ledger is absent while queue admission ledger exists in %s; inspect recovery evidence before migration", s.cfg.Namespace)
	}
	if !apierrors.IsNotFound(queueErr) {
		return queueErr
	}
	// Validate BOTH first-start contracts before either write. In particular,
	// queue validation must reject pre-existing CRs before the active ledger
	// can be created. A fully initialized concurrent replica is safe to observe;
	// a partial pair is never repaired by this fallback.
	if err := queue.PreflightInitialization(ctx); err != nil {
		return s.bootstrapRaceResult(ctx, err)
	}
	if err := buildcontroller.PreflightAdmissionLedger(ctx, s.reader, s.cfg.Namespace, s.cfg); err != nil {
		return s.bootstrapRaceResult(ctx, err)
	}
	// Active-first creation makes either ledger a marker that prevents silent
	// replacement of the other after startup. A crash in this short bootstrap
	// window requires an explicit, audited empty-namespace recovery.
	created, err := buildcontroller.EnsureAdmissionLedger(ctx, s.client, s.reader, s.cfg.Namespace, s.cfg)
	if err != nil {
		return s.bootstrapRaceResult(ctx, err)
	}
	if !created {
		// Even a replica that earlier observed both ledgers missing must
		// not repair a peer's queue: that peer may have started and then
		// lost the ledger while holding an unknown CR Create intent.
		return s.waitAdmissionBootstrap(ctx, queue)
	}
	if err := queue.EnsureInitialized(ctx); err != nil {
		return s.bootstrapRaceResult(ctx, err)
	}
	return s.checkAdmissionLedgers(ctx)
}

func (s *Server) waitAdmissionBootstrap(ctx context.Context, queue queueadmission.Store) error {
	// Another replica may be between first-start active and queue creation.
	// Observe briefly, but never create the missing queue here.
	for retry := 0; retry < 10; retry++ {
		if err := queue.CheckReady(ctx); err == nil {
			return s.checkActiveAdmissionLedger(ctx)
		} else if !apierrors.IsNotFound(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return fmt.Errorf("queue admission ledger is absent while active admission ledger exists in %s; inspect recovery evidence before migration", s.cfg.Namespace)
}

func (s *Server) bootstrapRaceResult(ctx context.Context, bootstrapErr error) error {
	if err := s.checkAdmissionLedgers(ctx); err == nil {
		return nil
	}
	return bootstrapErr
}

func (s *Server) checkAdmissionLedgers(ctx context.Context) error {
	return s.checkAdmissionLedgersWithReader(ctx, s.reader)
}

func (s *Server) checkAdmissionLedgersWithReader(ctx context.Context, reader client.Reader) error {
	if err := s.queueStoreWithReader(reader).CheckReady(ctx); err != nil {
		return err
	}
	return buildcontroller.CheckAdmissionLedger(ctx, reader, s.cfg.Namespace, s.cfg)
}

func (s *Server) checkActiveAdmissionLedger(ctx context.Context) error {
	return buildcontroller.CheckAdmissionLedger(ctx, s.reader, s.cfg.Namespace, s.cfg)
}

func (s *Server) routes() *echo.Echo {
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true
	e.HTTPErrorHandler = s.httpErrorHandler
	e.GET("/healthz", func(c echo.Context) error {
		return c.JSON(http.StatusOK, map[string]string{"status": "ok"})
	})
	e.GET("/version", func(c echo.Context) error {
		return c.JSON(http.StatusOK, apiv1.VersionInfo{
			APIVersion: apiv1.APIVersion, Version: version.Version,
			Commit: version.Commit, BuildDate: version.BuildDate,
		})
	})
	e.GET("/readyz", func(c echo.Context) error {
		var builds kovav1.KovaBuildList
		if err := s.readinessReader.List(c.Request().Context(), &builds, client.InNamespace(s.cfg.Namespace), client.Limit(1)); err != nil {
			return serviceUnavailable(c, err)
		}
		if err := s.checkAdmissionLedgersWithReader(c.Request().Context(), s.readinessReader); err != nil {
			return serviceUnavailable(c, err)
		}
		return c.JSON(http.StatusOK, apiv1.ReadyStatus{Status: "ready"})
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
