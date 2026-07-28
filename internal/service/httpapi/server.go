package httpapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/artifactstore"
	"github.com/cofy-x/kova/internal/kube"
	"github.com/cofy-x/kova/internal/logging"
	"github.com/cofy-x/kova/internal/observability"
	serviceauth "github.com/cofy-x/kova/internal/service/auth"
	"github.com/cofy-x/kova/internal/service/config"
	"github.com/cofy-x/kova/internal/service/runnerexec"
	"github.com/cofy-x/kova/internal/source"

	"github.com/labstack/echo/v4"
	"go.opentelemetry.io/otel/attribute"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

func (s *Server) authMiddleware(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		token, err := serviceauth.Bearer(c.Request().Header.Get("Authorization"))
		if err != nil && s.cfg.AuthMode != serviceauth.ModeUnsafeNone {
			authDenied.Add(c.Request().Context(), 1)
			return c.JSON(http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		}
		if s.auth == nil {
			authDenied.Add(c.Request().Context(), 1)
			return c.JSON(http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		}
		principal, err := s.auth.Authenticate(c.Request().Context(), token)
		if err != nil {
			authDenied.Add(c.Request().Context(), 1)
			return c.JSON(http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		}
		c.Set(principalContextKey, principal)
		return next(c)
	}
}

func (s *Server) handleCreateBuild(c echo.Context) error {
	principal := principalFromContext(c)
	if err := s.authorize(c.Request().Context(), principal, "create", ""); err != nil {
		return forbidden(c)
	}
	if strings.TrimSpace(s.cfg.RunnerImage) == "" {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "runner image is required"})
	}
	if s.cfg.MaxUploadBytes > 0 {
		c.Request().Body = http.MaxBytesReader(c.Response().Writer, c.Request().Body, s.cfg.MaxUploadBytes)
	}
	file, err := c.FormFile("file")
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return c.JSON(http.StatusRequestEntityTooLarge, map[string]string{"error": "build request exceeds the configured upload limit"})
		}
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "multipart field file is required"})
	}
	request, err := buildRequestFromMultipart(c)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}
	id := idempotentJobID(principal.Username, request.IdempotencyKey)
	if id == "" {
		id, err = newJobID()
		if err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		}
	}
	tmpPath, actualDigest, size, err := stageUpload(file)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	defer os.Remove(tmpPath)
	if request.SourceDigest != "" && !strings.EqualFold(request.SourceDigest, actualDigest) {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("source digest mismatch: expected %s, got %s", request.SourceDigest, actualDigest)})
	}
	request.SourceDigest = actualDigest
	if request.Options.Target != "" {
		err = source.ValidateSingleBuildArchiveTarget(tmpPath, request.Options.Target)
	} else {
		_, err = source.ValidateBuildArchive(tmpPath)
	}
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}
	staged, err := os.Open(tmpPath)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	key := "builds/" + id + "/" + strings.TrimPrefix(actualDigest, "sha256:") + ".zip"
	writeStarted := time.Now()
	artifactURI, putErr := s.store.Put(c.Request().Context(), key, staged, size, "application/zip")
	result := observability.ResultOK
	if putErr != nil {
		result = observability.ResultError
	}
	storeAttrs := []attribute.KeyValue{attribute.String(observability.AttrResult, result), attribute.String("kova.store.driver", s.cfg.ArtifactDriver)}
	artifactWrites.Add(c.Request().Context(), 1, storeAttrs...)
	artifactLatency.RecordDuration(c.Request().Context(), time.Since(writeStarted), storeAttrs...)
	closeErr := staged.Close()
	if putErr != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": putErr.Error()})
	}
	if closeErr != nil {
		_ = s.store.Delete(c.Request().Context(), artifactURI)
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": closeErr.Error()})
	}
	build := kovav1.KovaBuild{
		TypeMeta: metav1.TypeMeta{APIVersion: kovav1.Group + "/" + kovav1.Version, Kind: "KovaBuild"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      id,
			Namespace: s.cfg.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name": "kova-build",
				requesterLabel:           requesterID(principal.Username),
			},
		},
		Spec: kovav1.KovaBuildSpec{
			Requester:      kovav1.KovaBuildRequester{Username: principal.Username, UID: principal.UID},
			Source:         kovav1.KovaBuildSourceSpec{URI: artifactURI, Digest: actualDigest},
			Build:          request.Options,
			IdempotencyKey: request.IdempotencyKey,
		},
	}
	if err := s.client.Create(c.Request().Context(), &build); err != nil {
		if apierrors.IsAlreadyExists(err) && request.IdempotencyKey != "" {
			var existing kovav1.KovaBuild
			if getErr := s.reader.Get(c.Request().Context(), client.ObjectKey{Namespace: s.cfg.Namespace, Name: id}, &existing); getErr != nil {
				return c.JSON(http.StatusInternalServerError, map[string]string{"error": getErr.Error()})
			}
			if existing.Spec.Source.URI != artifactURI {
				_ = s.store.Delete(c.Request().Context(), artifactURI)
			}
			if !sameBuildRequest(&existing, request) {
				return c.JSON(http.StatusConflict, map[string]string{"error": "idempotency key is already used with different build parameters"})
			}
			return c.JSON(http.StatusOK, buildJobFromCR(&existing, s.cfg))
		}
		_ = s.store.Delete(c.Request().Context(), artifactURI)
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusAccepted, buildJobFromCR(&build, s.cfg))
}

func sameBuildRequest(build *kovav1.KovaBuild, request createBuildRequest) bool {
	return build.Spec.Source.Digest == request.SourceDigest &&
		reflect.DeepEqual(build.Spec.Build, request.Options)
}

func stageUpload(file *multipart.FileHeader) (string, string, int64, error) {
	src, err := file.Open()
	if err != nil {
		return "", "", 0, err
	}
	defer src.Close()
	tmp, err := os.CreateTemp("", "kova-source-*.zip")
	if err != nil {
		return "", "", 0, err
	}
	path := tmp.Name()
	hash := sha256.New()
	size, copyErr := io.Copy(io.MultiWriter(tmp, hash), src)
	closeErr := tmp.Close()
	if copyErr != nil {
		_ = os.Remove(path)
		return "", "", 0, copyErr
	}
	if closeErr != nil {
		_ = os.Remove(path)
		return "", "", 0, closeErr
	}
	return path, "sha256:" + hex.EncodeToString(hash.Sum(nil)), size, nil
}

func idempotentJobID(username, key string) string {
	if key == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(username + "\x00" + key))
	return "idem-" + hex.EncodeToString(sum[:10])
}

func (s *Server) handleListBuilds(c echo.Context) error {
	principal := principalFromContext(c)
	options := []client.ListOption{client.InNamespace(s.cfg.Namespace)}
	if err := s.authorize(c.Request().Context(), principal, "list", ""); err != nil {
		options = append(options, client.MatchingLabels{requesterLabel: requesterID(principal.Username)})
	}
	var builds kovav1.KovaBuildList
	if err := s.reader.List(c.Request().Context(), &builds, options...); err != nil {
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
	if err := s.authorizeBuild(c.Request().Context(), principalFromContext(c), "get", build); err != nil {
		return forbidden(c)
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
	if err := s.authorizeBuild(c.Request().Context(), principalFromContext(c), "get", build); err != nil {
		return forbidden(c)
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
	if err := s.authorizeBuild(c.Request().Context(), principalFromContext(c), "delete", build); err != nil {
		return forbidden(c)
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
	build.Status.ObservedGeneration = build.Generation
	build.Status.FinishedAt = &now
	build.Status.Message = ""
	apiMeta.SetStatusCondition(&build.Status.Conditions, metav1.Condition{
		Type: "Ready", Status: metav1.ConditionFalse, Reason: "Cancelled",
		Message: "build was cancelled", ObservedGeneration: build.Generation,
	})
	if err := s.client.Status().Update(c.Request().Context(), build); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	buildCancels.Add(c.Request().Context(), 1)
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
	if err := s.authorizeBuild(c.Request().Context(), principalFromContext(c), "update", build); err != nil {
		return forbidden(c)
	}
	out, err := s.runner().Post(c.Request().Context(), build, "export", c.QueryString())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.Blob(http.StatusOK, "application/x-ndjson", out)
}

const (
	principalContextKey = "kova.principal"
	requesterLabel      = "kova.cofy.dev/requester-id"
)

func principalFromContext(c echo.Context) serviceauth.Principal {
	principal, _ := c.Get(principalContextKey).(serviceauth.Principal)
	return principal
}

func requesterID(username string) string {
	sum := sha256.Sum256([]byte(username))
	return hex.EncodeToString(sum[:16])
}

func (s *Server) authorize(ctx context.Context, principal serviceauth.Principal, verb, name string) error {
	if s.authz == nil {
		return fmt.Errorf("authorization is not configured")
	}
	return s.authz.Authorize(ctx, principal, serviceauth.Attributes{
		Verb: verb, Namespace: s.cfg.Namespace, Resource: "kovabuilds", Name: name,
	})
}

func (s *Server) authorizeBuild(ctx context.Context, principal serviceauth.Principal, verb string, build *kovav1.KovaBuild) error {
	if err := s.authorize(ctx, principal, verb, build.Name); err == nil {
		return nil
	}
	if principal.Username != "" && build.Spec.Requester.Username == principal.Username {
		return nil
	}
	return fmt.Errorf("access denied")
}

func forbidden(c echo.Context) error {
	authzDenied.Add(c.Request().Context(), 1)
	return c.JSON(http.StatusForbidden, map[string]string{"error": "forbidden"})
}

func (s *Server) handlePreheatBuild(c echo.Context) error {
	build, err := s.getBuild(c.Request().Context(), c.Param("id"))
	if apierrors.IsNotFound(err) {
		return c.JSON(http.StatusNotFound, map[string]string{"error": "job not found"})
	}
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	if err := s.authorizeBuild(c.Request().Context(), principalFromContext(c), "update", build); err != nil {
		return forbidden(c)
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
