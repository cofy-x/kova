package httpapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"reflect"
	"strings"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	serviceauth "github.com/cofy-x/kova/internal/service/auth"

	"github.com/labstack/echo/v4"
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
	request, err := buildRequestFromJSON(c)
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
	build := kovav1.KovaBuild{
		TypeMeta: metav1.TypeMeta{APIVersion: kovav1.Group + "/" + kovav1.Version, Kind: "KovaBuild"},
		ObjectMeta: metav1.ObjectMeta{
			Name: id, Namespace: s.cfg.Namespace,
			Labels: map[string]string{"app.kubernetes.io/name": "kova-build", requesterLabel: requesterID(principal.Username)},
		},
		Spec: kovav1.KovaBuildSpec{
			Requester: kovav1.KovaBuildRequester{Username: principal.Username, UID: principal.UID},
			Targets:   append([]string(nil), request.Targets...),
			Source:    kovav1.KovaBuildSourceSpec{URI: request.SourceURI, Digest: request.SourceDigest},
			Build:     request.Options, IdempotencyKey: request.IdempotencyKey,
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
	return c.JSON(http.StatusAccepted, buildJobFromCR(&build, s.cfg))
}

func sameBuildRequest(build *kovav1.KovaBuild, request createBuildRequest) bool {
	return build.Spec.Source.URI == request.SourceURI &&
		build.Spec.Source.Digest == request.SourceDigest &&
		reflect.DeepEqual(build.Spec.Targets, request.Targets) &&
		reflect.DeepEqual(build.Spec.Build, request.Options)
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
			return err
		}
	}
	var builds kovav1.KovaBuildList
	if err := s.reader.List(ctx, &builds, client.InNamespace(s.cfg.Namespace), client.MatchingLabels{requesterLabel: requesterID(principal.Username)}); err != nil {
		return err
	}
	queued := 0
	for i := range builds.Items {
		if builds.Items[i].Status.Phase == "" || builds.Items[i].Status.Phase == kovav1.PhaseQueued {
			queued++
		}
	}
	if queued >= s.cfg.MaxQueuedJobsPerRequester {
		return &queueCapacityError{limit: s.cfg.MaxQueuedJobsPerRequester}
	}
	return nil
}

type queueCapacityError struct{ limit int }

func (e *queueCapacityError) Error() string { return "requester queue limit is reached" }
