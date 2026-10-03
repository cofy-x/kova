package httpapi

import (
	"context"
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
	"github.com/cofy-x/kova/internal/service/recoveryreceipt"

	"github.com/labstack/echo/v4"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (s *Server) handleCreateBuild(c echo.Context) error {
	principal := principalFromContext(c)
	if err := s.authorize(c.Request().Context(), principal, "create", ""); err != nil {
		return authorizationFailure(c, err)
	}
	// Even an idempotent POST replay cannot use a replaced or lost original
	// installation as authority. Read-only GET routes remain available.
	if err := s.checkGenesis(c.Request().Context()); err != nil {
		return serviceUnavailable(c, err)
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
			if err := s.verifyGenesisHTTPReplay(c.Request().Context(), &existing, request, principal.Username); err != nil {
				return genesisHTTPReplayFailure(c, err)
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
	if err := s.checkGenesis(c.Request().Context()); err != nil {
		return serviceUnavailable(c, err)
	}
	if s.genesisLedger != nil && s.receipts == nil {
		return serviceUnavailable(c, fmt.Errorf("Genesis queue admission lacks a direct recovery receipt client"))
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
			if s.genesisLedger != nil && intent.CleanupKind == "rejected" {
				if cleanupErr := store.ResumeRejectedCleanup(c.Request().Context(), id, intent.Nonce); cleanupErr != nil {
					return serviceUnavailable(c, cleanupErr)
				}
			}
			return queueAdmissionPending(c, id)
		}
		if err != nil {
			return internalError(c, err)
		}
		if !sameBuildRequest(&existing, request) {
			return conflict(c, "idempotency key is already used with different build parameters")
		}
		if err := s.verifyGenesisHTTPReplay(c.Request().Context(), &existing, request, principal.Username); err != nil {
			return genesisHTTPReplayFailure(c, err)
		}
		return c.JSON(http.StatusOK, buildJobFromCR(&existing, s.cfg))
	}
	build.Annotations = map[string]string{queueadmission.IntentAnnotation: intent.Nonce}
	var queueWitness recoveryreceipt.ObservedQueueIntent
	if s.genesisLedger != nil {
		receiptIntent, err := store.ReceiptIntent(&build, intent)
		if err != nil {
			return queueAdmissionPending(c, id)
		}
		if err := store.CheckReceiptNamespace(c.Request().Context()); err != nil {
			return queueAdmissionPending(c, id)
		}
		// Only this fresh queue-CAS owner may make the receipt Create call.
		// A successor observing the same queue intent never enters this branch.
		queueWitness, err = recoveryreceipt.RecordQueueOnce(c.Request().Context(), s.receipts, receiptIntent)
		if err != nil {
			logging.Errorf("KovaBuild %s queue receipt outcome is unknown: %v", id, err)
			return queueAdmissionPending(c, id)
		}
		if err := store.CheckReceiptNamespace(c.Request().Context()); err != nil {
			return queueAdmissionPending(c, id)
		}
		if err := store.PinReceipt(c.Request().Context(), &build, intent, queueWitness); err != nil {
			logging.Errorf("KovaBuild %s queue receipt pin is unconfirmed: %v", id, err)
			return queueAdmissionPending(c, id)
		}
		build.Annotations[queueadmission.ReceiptUIDAnnotation] = queueWitness.ReceiptUID
		build.Annotations[queueadmission.ReceiptDigestAnnotation] = queueWitness.DataDigest
	}
	// A reserve is not a reusable permission for a later name-addressed CR
	// Create. On refusal the intent stays held; an absent CR is not proof that
	// an already-sent Create cannot arrive later.
	if err := s.checkGenesis(c.Request().Context()); err != nil {
		return serviceUnavailable(c, err)
	}
	if s.genesisLedger != nil {
		if err := store.CheckReceiptNamespace(c.Request().Context()); err != nil {
			return serviceUnavailable(c, err)
		}
	}
	if err := s.client.Create(c.Request().Context(), &build); err != nil {
		if apierrors.IsAlreadyExists(err) {
			var existing kovav1.KovaBuild
			if getErr := s.reader.Get(c.Request().Context(), client.ObjectKey{Namespace: s.cfg.Namespace, Name: id}, &existing); getErr != nil {
				return internalError(c, getErr)
			}
			if existing.Annotations[queueadmission.IntentAnnotation] != intent.Nonce {
				if err := s.checkGenesis(c.Request().Context()); err != nil {
					return serviceUnavailable(c, err)
				}
				var releaseErr error
				if s.genesisLedger != nil {
					releaseErr = store.ReleaseRejectedReceipt(c.Request().Context(), &build, queueWitness)
				} else {
					releaseErr = store.ReleaseRejected(c.Request().Context(), id, intent.Nonce)
				}
				if releaseErr != nil {
					return internalError(c, releaseErr)
				}
			} else if s.genesisLedger != nil &&
				(existing.Annotations[queueadmission.ReceiptUIDAnnotation] != queueWitness.ReceiptUID ||
					existing.Annotations[queueadmission.ReceiptDigestAnnotation] != queueWitness.DataDigest) {
				return conflict(c, "existing KovaBuild has a different queue recovery receipt")
			}
			if !sameBuildRequest(&existing, request) {
				return conflict(c, "idempotency key is already used with different build parameters")
			}
			if err := s.verifyGenesisHTTPReplay(c.Request().Context(), &existing, request, principal.Username); err != nil {
				return genesisHTTPReplayFailure(c, err)
			}
			return c.JSON(http.StatusOK, buildJobFromCR(&existing, s.cfg))
		}
		if definitiveCreateRejection(err) {
			if guardErr := s.checkGenesis(c.Request().Context()); guardErr != nil {
				return serviceUnavailable(c, guardErr)
			}
			var releaseErr error
			if s.genesisLedger != nil {
				releaseErr = store.ReleaseRejectedReceipt(c.Request().Context(), &build, queueWitness)
			} else {
				releaseErr = store.ReleaseRejected(c.Request().Context(), id, intent.Nonce)
			}
			if releaseErr != nil {
				return internalError(c, releaseErr)
			}
			return internalError(c, err)
		}
		logging.Errorf("KovaBuild %s Create returned an uncertain outcome: %v", id, err)
		return queueAdmissionPending(c, id)
	}
	return c.JSON(http.StatusAccepted, buildJobFromCR(&build, s.cfg))
}

var errUnlinkedGenesisHTTPBuild = errors.New("existing KovaBuild is not linked to this HTTP submission")

func genesisHTTPReplayFailure(c echo.Context, err error) error {
	if errors.Is(err, errUnlinkedGenesisHTTPBuild) {
		return conflict(c, "idempotency key belongs to a KovaBuild without this HTTP receipt link")
	}
	return serviceUnavailable(c, err)
}

// A same-name CR is not proof that this HTTP request reached the one authorized
// Create. Active replays need the pinned live queue receipt; after normal
// terminal cleanup, the exact CR settlement witness and absence of both the
// charged queue entry and old receipt allow a read-only idempotent replay.
func (s *Server) verifyGenesisHTTPReplay(ctx context.Context, build *kovav1.KovaBuild, request createBuildRequest, username string) error {
	if s.genesisLedger == nil {
		return nil
	}
	if build.Spec.Requester.Username != username || build.Spec.IdempotencyKey != request.IdempotencyKey ||
		build.Annotations[queueadmission.IntentAnnotation] == "" ||
		build.Annotations[queueadmission.ReceiptUIDAnnotation] == "" ||
		build.Annotations[queueadmission.ReceiptDigestAnnotation] == "" {
		return errUnlinkedGenesisHTTPBuild
	}
	settledUID := build.Annotations[queueadmission.ReceiptSettledAnnotation]
	if settledUID == "" {
		return s.queueStore().VerifyForBuild(ctx, build)
	}
	if settledUID != build.Annotations[queueadmission.ReceiptUIDAnnotation] ||
		(build.DeletionTimestamp == nil && build.Status.Phase != kovav1.PhaseSucceeded &&
			build.Status.Phase != kovav1.PhaseFailed && build.Status.Phase != kovav1.PhaseCancelled) {
		return errUnlinkedGenesisHTTPBuild
	}
	store := s.queueStore()
	if _, found, err := store.Lookup(ctx, build.Name); err != nil {
		return err
	} else if found {
		return queueadmission.ErrDrift
	}
	if err := store.CheckReceiptNamespace(ctx); err != nil {
		return err
	}
	var receipt corev1.ConfigMap
	err := s.reader.Get(ctx, client.ObjectKey{Namespace: s.genesisLedger.Bootstrap.Receipt.Contract.ReceiptNamespace,
		Name: "kova-admission-intent-" + build.Annotations[queueadmission.IntentAnnotation]}, &receipt)
	if !apierrors.IsNotFound(err) {
		if err != nil {
			return err
		}
		return queueadmission.ErrDrift
	}
	return s.checkGenesis(ctx)
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
