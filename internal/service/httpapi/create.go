package httpapi

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"unicode/utf8"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/logging"
	"github.com/cofy-x/kova/internal/service/queueadmission"

	"github.com/labstack/echo/v4"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (s *Server) handleCreateBuild(c echo.Context) error {
	principal := principalFromContext(c)
	if err := s.authorize(c.Request().Context(), principal, "create", ""); err != nil {
		return authorizationFailure(c, err)
	}
	// The CRD bounds these identities by Unicode characters. Reject an
	// unsupported authenticated identity before writing a durable queue intent.
	if principal.Username == "" || !utf8.ValidString(principal.Username) || utf8.RuneCountInString(principal.Username) > 253 ||
		!utf8.ValidString(principal.UID) || utf8.RuneCountInString(principal.UID) > 253 {
		return invalidRequest(c, fmt.Errorf("authenticated requester identity exceeds the KovaBuild contract"))
	}
	if strings.TrimSpace(s.cfg.RunnerImage) == "" {
		return internalContractError(c, &configurationError{message: "runner image is required"})
	}
	request, err := buildRequestFromJSON(c)
	if err != nil {
		return invalidRequest(c, err)
	}
	id := idempotentJobID(principal.Username, request.IdempotencyKey)
	if id == "" {
		id, err = newJobID()
		if err != nil {
			return internalError(c, err)
		}
	}
	c.Response().Header().Set("X-Kova-Build-ID", id)
	if request.IdempotencyKey != "" {
		var existing kovav1.KovaBuild
		err := s.reader.Get(c.Request().Context(), client.ObjectKey{Namespace: s.cfg.Namespace, Name: id}, &existing)
		if err == nil {
			if !sameBuildRequest(&existing, request) {
				return conflict(c, "idempotency key is already used with different build parameters")
			}
			return c.JSON(http.StatusOK, buildJobFromCR(&existing, s.cfg))
		}
		if !apierrors.IsNotFound(err) {
			return internalError(c, err)
		}
	}
	build := kovav1.KovaBuild{
		TypeMeta: metav1.TypeMeta{APIVersion: kovav1.Group + "/" + kovav1.Version, Kind: "KovaBuild"},
		ObjectMeta: metav1.ObjectMeta{
			Name: id, Namespace: s.cfg.Namespace,
			Labels: map[string]string{"app.kubernetes.io/name": "kova-build", requesterLabel: requesterID(principal.Username)},
			// Deletion before the controller's first reconcile must still pass
			// through verified queue-intent and runner cleanup.
			Finalizers: []string{kovav1.CleanupFinalizer},
		},
		Spec: kovav1.KovaBuildSpec{
			Requester: kovav1.KovaBuildRequester{Username: principal.Username, UID: principal.UID},
			Targets:   append([]kovav1.KovaBuildTargetSpec(nil), request.Targets...),
			Source:    kovav1.KovaBuildSourceSpec{URI: request.SourceURI, Digest: request.SourceDigest},
			Build:     request.Options, IdempotencyKey: request.IdempotencyKey,
		},
	}
	// Reserve itself validates the queue ledger. Check the independent active
	// ledger before committing a queue intent so a corrupt controller ledger
	// cannot cause a newly accepted build to stall without a runner.
	if err := s.checkActiveAdmissionLedger(c.Request().Context()); err != nil {
		return serviceUnavailable(c, err)
	}
	store := s.queueStore()
	intent, fresh, err := store.Reserve(c.Request().Context(), &build)
	if err != nil {
		switch {
		case errors.Is(err, queueadmission.ErrFull):
			return queueCapacityExceeded(c, "queue limit is reached")
		case errors.Is(err, queueadmission.ErrBusy):
			return queueCapacityExceeded(c, "queue admission is busy; retry")
		case errors.Is(err, queueadmission.ErrConflict):
			return conflict(c, "idempotency key is already used with different build parameters")
		default:
			return serviceUnavailable(c, err)
		}
	}
	if !fresh {
		var existing kovav1.KovaBuild
		err := s.reader.Get(c.Request().Context(), client.ObjectKey{Namespace: s.cfg.Namespace, Name: id}, &existing)
		if apierrors.IsNotFound(err) {
			return queueAdmissionPending(c, id)
		}
		if err != nil {
			return internalError(c, err)
		}
		if !sameBuildRequest(&existing, request) {
			return conflict(c, "idempotency key is already used with different build parameters")
		}
		return c.JSON(http.StatusOK, buildJobFromCR(&existing, s.cfg))
	}
	build.Annotations = map[string]string{queueadmission.IntentAnnotation: intent.Nonce}
	if err := s.client.Create(c.Request().Context(), &build); err != nil {
		if apierrors.IsAlreadyExists(err) {
			var existing kovav1.KovaBuild
			if getErr := s.reader.Get(c.Request().Context(), client.ObjectKey{Namespace: s.cfg.Namespace, Name: id}, &existing); getErr != nil {
				return internalError(c, getErr)
			}
			if existing.Annotations[queueadmission.IntentAnnotation] != intent.Nonce {
				if err := store.ReleaseRejected(c.Request().Context(), id, intent.Nonce); err != nil {
					return internalError(c, err)
				}
			}
			if !sameBuildRequest(&existing, request) {
				return conflict(c, "idempotency key is already used with different build parameters")
			}
			return c.JSON(http.StatusOK, buildJobFromCR(&existing, s.cfg))
		}
		if definitiveCreateRejection(err) {
			if releaseErr := store.ReleaseRejected(c.Request().Context(), id, intent.Nonce); releaseErr != nil {
				return internalError(c, releaseErr)
			}
			return internalError(c, err)
		}
		logging.Errorf("KovaBuild %s Create returned an uncertain outcome: %v", id, err)
		return queueAdmissionPending(c, id)
	}
	return c.JSON(http.StatusAccepted, buildJobFromCR(&build, s.cfg))
}

func definitiveCreateRejection(err error) bool {
	return apierrors.IsForbidden(err) || apierrors.IsInvalid(err) || apierrors.IsBadRequest(err) || apierrors.IsUnauthorized(err)
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

type configurationError struct{ message string }

func (e *configurationError) Error() string { return e.message }
