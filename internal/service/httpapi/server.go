package httpapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/kube"
	"github.com/cofy-x/kova/internal/logging"
	"github.com/cofy-x/kova/internal/service/config"
	"github.com/cofy-x/kova/internal/service/runnerexec"
	"github.com/cofy-x/kova/internal/service/sourcestore"
	"github.com/cofy-x/kova/internal/source"

	"github.com/labstack/echo/v4"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
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
}

func NewServer(cfg config.Config, kube kubeAPI, crClient client.Client, crReader client.Reader) *Server {
	if cfg.Listen == "" {
		cfg.Listen = ":8080"
	}
	if cfg.SourceRoot == "" {
		cfg.SourceRoot = sourcestore.DefaultRoot
	}
	if cfg.SourceMountPath == "" {
		cfg.SourceMountPath = cfg.SourceRoot
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
	return &Server{cfg: cfg, kube: kube, client: crClient, reader: crReader}
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

func (s *Server) authMiddleware(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		if strings.TrimSpace(s.cfg.AuthToken) == "" {
			return next(c)
		}
		want := "Bearer " + s.cfg.AuthToken
		if c.Request().Header.Get("Authorization") != want {
			return c.JSON(http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		}
		return next(c)
	}
}

func (s *Server) handleCreateBuild(c echo.Context) error {
	if strings.TrimSpace(s.cfg.RunnerImage) == "" {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "runner image is required"})
	}
	if strings.TrimSpace(s.cfg.SourcePVCClaim) == "" {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "source PVC claim is required"})
	}
	file, err := c.FormFile("file")
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "multipart field file is required"})
	}
	request, err := buildRequestFromMultipart(c)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}
	id := idempotentJobID(request.IdempotencyKey)
	if id == "" {
		id, err = newJobID()
		if err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		}
	}
	build := kovav1.KovaBuild{
		TypeMeta: metav1.TypeMeta{APIVersion: kovav1.Group + "/" + kovav1.Version, Kind: "KovaBuild"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      id,
			Namespace: s.cfg.Namespace,
			Labels:    map[string]string{"app.kubernetes.io/name": "kova-build"},
		},
		Spec: kovav1.KovaBuildSpec{
			Source: kovav1.KovaBuildSourceSpec{Ready: false, PVC: kovav1.KovaBuildPVCSource{
				ClaimName: s.cfg.SourcePVCClaim,
				Path:      sourcestore.Path(id),
				MountPath: s.cfg.SourceMountPath,
			}},
			Build:          request.Options,
			SourceDigest:   request.SourceDigest,
			IdempotencyKey: request.IdempotencyKey,
		},
	}
	if err := s.client.Create(c.Request().Context(), &build); err != nil {
		if apierrors.IsAlreadyExists(err) && request.IdempotencyKey != "" {
			var existing kovav1.KovaBuild
			if getErr := s.reader.Get(c.Request().Context(), client.ObjectKey{Namespace: s.cfg.Namespace, Name: id}, &existing); getErr != nil {
				return c.JSON(http.StatusInternalServerError, map[string]string{"error": getErr.Error()})
			}
			if !sameBuildRequest(&existing, request) {
				return c.JSON(http.StatusConflict, map[string]string{"error": "idempotency key is already used with different build parameters"})
			}
			return c.JSON(http.StatusOK, buildJobFromCR(&existing, s.cfg))
		}
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	relPath, err := sourcestore.SaveUpload(s.cfg.SourceRoot, id, file)
	if err != nil {
		_ = s.client.Delete(c.Request().Context(), &build)
		_ = sourcestore.Remove(s.cfg.SourceRoot, id)
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	archivePath := filepath.Join(s.cfg.SourceRoot, filepath.FromSlash(relPath))
	if request.Options.Target != "" {
		err = source.ValidateSingleBuildArchiveTarget(archivePath, request.Options.Target)
	} else {
		_, err = source.ValidateBuildArchive(archivePath)
	}
	if err != nil {
		_ = s.client.Delete(c.Request().Context(), &build)
		_ = sourcestore.Remove(s.cfg.SourceRoot, id)
		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var current kovav1.KovaBuild
		if err := s.client.Get(c.Request().Context(), client.ObjectKeyFromObject(&build), &current); err != nil {
			return err
		}
		current.Spec.Source.Ready = true
		if err := s.client.Update(c.Request().Context(), &current); err != nil {
			return err
		}
		build = current
		return nil
	}); err != nil {
		_ = s.client.Delete(c.Request().Context(), &build)
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusAccepted, buildJobFromCR(&build, s.cfg))
}

func sameBuildRequest(build *kovav1.KovaBuild, request createBuildRequest) bool {
	return build.Spec.SourceDigest == request.SourceDigest &&
		reflect.DeepEqual(build.Spec.Build, request.Options)
}

func idempotentJobID(key string) string {
	if key == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(key))
	return "idem-" + hex.EncodeToString(sum[:10])
}

func (s *Server) handleListBuilds(c echo.Context) error {
	var builds kovav1.KovaBuildList
	if err := s.reader.List(c.Request().Context(), &builds, client.InNamespace(s.cfg.Namespace)); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	jobs := make([]BuildJob, 0, len(builds.Items))
	for i := range builds.Items {
		jobs = append(jobs, buildJobFromCR(&builds.Items[i], s.cfg))
	}
	return c.JSON(http.StatusOK, jobListResponse{Jobs: jobs})
}

func (s *Server) handleGetBuild(c echo.Context) error {
	build, err := s.getBuild(c.Request().Context(), c.Param("id"))
	if apierrors.IsNotFound(err) {
		return c.JSON(http.StatusNotFound, map[string]string{"error": "job not found"})
	}
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, buildJobFromCR(build, s.cfg))
}

func (s *Server) handleBuildLogs(c echo.Context) error {
	build, err := s.getBuild(c.Request().Context(), c.Param("id"))
	if apierrors.IsNotFound(err) {
		return c.JSON(http.StatusNotFound, map[string]string{"error": "job not found"})
	}
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	tail, err := strconv.ParseInt(strings.TrimSpace(defaultString(c.QueryParam("tail_lines"), "100")), 10, 64)
	if err != nil || tail < 0 {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "tail_lines must be a non-negative integer"})
	}
	var out bytes.Buffer
	if err := s.kube.WritePodLogsTail(c.Request().Context(), build.Namespace, build.Status.RunnerPodName, tail, &out); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.Blob(http.StatusOK, "text/plain; charset=utf-8", out.Bytes())
}

func (s *Server) handleCancelBuild(c echo.Context) error {
	build, err := s.getBuild(c.Request().Context(), c.Param("id"))
	if apierrors.IsNotFound(err) {
		return c.JSON(http.StatusNotFound, map[string]string{"error": "job not found"})
	}
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	if isTerminalPhase(build.Status.Phase) {
		return c.JSON(http.StatusOK, buildJobFromCR(build, s.cfg))
	}
	_ = s.runner().CancelBuild(c.Request().Context(), build)
	if build.Status.RunnerPodName != "" {
		if err := s.kube.DeletePod(c.Request().Context(), build.Namespace, build.Status.RunnerPodName); err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		}
	}
	now := metav1.Now()
	build.Status.Phase = kovav1.PhaseCancelled
	build.Status.SourceDigest = build.Spec.SourceDigest
	build.Status.IdempotencyKey = build.Spec.IdempotencyKey
	build.Status.FinishedAt = &now
	build.Status.Message = ""
	if err := s.client.Status().Update(c.Request().Context(), build); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusAccepted, buildJobFromCR(build, s.cfg))
}

func (s *Server) handleExportBuild(c echo.Context) error {
	build, err := s.getBuild(c.Request().Context(), c.Param("id"))
	if apierrors.IsNotFound(err) {
		return c.JSON(http.StatusNotFound, map[string]string{"error": "job not found"})
	}
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	out, err := s.runner().Post(c.Request().Context(), build, "export", c.QueryString())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.Blob(http.StatusOK, "application/x-ndjson", out)
}

func (s *Server) handlePreheatBuild(c echo.Context) error {
	build, err := s.getBuild(c.Request().Context(), c.Param("id"))
	if apierrors.IsNotFound(err) {
		return c.JSON(http.StatusNotFound, map[string]string{"error": "job not found"})
	}
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	out, err := s.runner().Post(c.Request().Context(), build, "preheat", c.QueryString())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.Blob(http.StatusOK, "application/json", out)
}

func (s *Server) runner() runnerexec.Client {
	return runnerexec.Client{Kube: s.kube, BuildkitAddr: s.cfg.BuildkitAddr}
}

func (s *Server) getBuild(ctx context.Context, id string) (*kovav1.KovaBuild, error) {
	var build kovav1.KovaBuild
	if err := s.reader.Get(ctx, client.ObjectKey{Namespace: s.cfg.Namespace, Name: id}, &build); err != nil {
		return nil, err
	}
	return &build, nil
}
