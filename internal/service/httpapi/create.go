package httpapi

import (
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
	"strings"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/observability"
	serviceauth "github.com/cofy-x/kova/internal/service/auth"
	"github.com/cofy-x/kova/internal/source"

	"github.com/labstack/echo/v4"
	"go.opentelemetry.io/otel/attribute"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

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
	if err := s.ensureQueueCapacity(c.Request().Context(), principal, id, request.IdempotencyKey != ""); err != nil {
		c.Response().Header().Set("Retry-After", "5")
		return c.JSON(http.StatusTooManyRequests, map[string]string{"error": err.Error()})
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
	targets, err := source.BuildArchiveTargets(tmpPath)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}
	if request.Options.Target != "" {
		if len(targets) != 1 {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("explicit target requires one image, got %d", len(targets))})
		}
		if targets[0] != strings.TrimSpace(request.Options.Target) {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("archive target %q does not match requested target %q", targets[0], request.Options.Target)})
		}
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
			Targets:        targets,
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

func (s *Server) ensureQueueCapacity(ctx context.Context, principal serviceauth.Principal, id string, idempotent bool) error {
	if s.cfg.MaxQueuedJobsPerRequester <= 0 {
		return nil
	}
	if idempotent {
		var existing kovav1.KovaBuild
		if err := s.reader.Get(ctx, client.ObjectKey{Namespace: s.cfg.Namespace, Name: id}, &existing); err == nil {
			return nil
		} else if !apierrors.IsNotFound(err) {
			return fmt.Errorf("check idempotent job: %w", err)
		}
	}
	var builds kovav1.KovaBuildList
	if err := s.reader.List(ctx, &builds, client.InNamespace(s.cfg.Namespace), client.MatchingLabels{requesterLabel: requesterID(principal.Username)}); err != nil {
		return fmt.Errorf("check requester queue capacity: %w", err)
	}
	queued := 0
	for i := range builds.Items {
		if builds.Items[i].Status.Phase == "" || builds.Items[i].Status.Phase == kovav1.PhaseQueued {
			queued++
		}
	}
	if queued >= s.cfg.MaxQueuedJobsPerRequester {
		return fmt.Errorf("requester queue limit of %d jobs is reached", s.cfg.MaxQueuedJobsPerRequester)
	}
	return nil
}
