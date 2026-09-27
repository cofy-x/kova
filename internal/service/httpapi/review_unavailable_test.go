package httpapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	serviceauth "github.com/cofy-x/kova/internal/service/auth"
	apiv1 "github.com/cofy-x/kova/pkg/api/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type authenticatorFunc func(context.Context, string) (serviceauth.Principal, error)

func (fn authenticatorFunc) Authenticate(ctx context.Context, token string) (serviceauth.Principal, error) {
	return fn(ctx, token)
}

func TestUnavailableAuthenticationFailsClosedWithoutLeakingBearer(t *testing.T) {
	srv := newTestServer(t, &fakeKube{})
	srv.auth = authenticatorFunc(func(context.Context, string) (serviceauth.Principal, error) {
		return serviceauth.Principal{}, fmt.Errorf("review failed: %w", serviceauth.ErrReviewUnavailable)
	})
	request := httptest.NewRequest(http.MethodGet, "/v1/builds", nil)
	request.Header.Set("Authorization", "Bearer secret-bearer-must-not-leak")
	response := httptest.NewRecorder()
	srv.routes().ServeHTTP(response, request)
	assertReviewUnavailable(t, response)
	if strings.Contains(response.Body.String(), "secret-bearer-must-not-leak") {
		t.Fatal("public response leaked bearer")
	}

	srv.auth = authenticatorFunc(func(context.Context, string) (serviceauth.Principal, error) {
		return serviceauth.Principal{}, errors.New("definitively invalid")
	})
	denied := httptest.NewRecorder()
	srv.routes().ServeHTTP(denied, request)
	assertAPIError(t, denied, http.StatusUnauthorized, apiv1.ErrorCodeUnauthenticated, false)
}

func TestUnavailableAuthorizationFailsClosedAtEveryHTTPCallSite(t *testing.T) {
	srv := newTestServer(t, &fakeKube{})
	other := &kovav1.KovaBuild{
		ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: srv.cfg.Namespace},
		Spec:       kovav1.KovaBuildSpec{Requester: kovav1.KovaBuildRequester{Username: "other-user"}},
	}
	if err := srv.client.Create(context.Background(), other); err != nil {
		t.Fatal(err)
	}
	owned := &kovav1.KovaBuild{
		ObjectMeta: metav1.ObjectMeta{Name: "owned", Namespace: srv.cfg.Namespace},
		Spec:       kovav1.KovaBuildSpec{Requester: kovav1.KovaBuildRequester{Username: "test-user"}},
	}
	if err := srv.client.Create(context.Background(), owned); err != nil {
		t.Fatal(err)
	}
	queued := &kovav1.KovaBuild{
		ObjectMeta: metav1.ObjectMeta{Name: "queued", Namespace: srv.cfg.Namespace},
		Spec:       kovav1.KovaBuildSpec{Requester: kovav1.KovaBuildRequester{Username: "other-user"}},
	}
	if _, _, err := srv.queueStore().Reserve(context.Background(), queued); err != nil {
		t.Fatal(err)
	}
	ownedQueued := &kovav1.KovaBuild{
		ObjectMeta: metav1.ObjectMeta{Name: "owned-queued", Namespace: srv.cfg.Namespace},
		Spec:       kovav1.KovaBuildSpec{Requester: kovav1.KovaBuildRequester{Username: "test-user"}},
	}
	if _, _, err := srv.queueStore().Reserve(context.Background(), ownedQueued); err != nil {
		t.Fatal(err)
	}
	var reviews int
	srv.authz = authorizerFunc(func(context.Context, serviceauth.Principal, serviceauth.Attributes) error {
		reviews++
		return fmt.Errorf("review failed: %w", serviceauth.ErrReviewUnavailable)
	})
	for _, test := range []struct{ method, path string }{
		{http.MethodGet, "/v1/builds"},
		{http.MethodPost, "/v1/builds"},
		{http.MethodGet, "/v1/builds/queued"},
		{http.MethodGet, "/v1/builds/absent"},
		{http.MethodGet, "/v1/builds/other"},
		{http.MethodGet, "/v1/builds/other/results"},
		{http.MethodGet, "/v1/builds/other/logs"},
		{http.MethodPost, "/v1/builds/other/cancel"},
		{http.MethodDelete, "/v1/builds/other"},
		{http.MethodPost, "/v1/builds/other/export"},
		{http.MethodPost, "/v1/builds/other/preheat"},
	} {
		t.Run(test.method+" "+test.path, func(t *testing.T) {
			request := httptest.NewRequest(test.method, test.path, nil)
			request.Header.Set("Authorization", "Bearer token")
			response := httptest.NewRecorder()
			srv.routes().ServeHTTP(response, request)
			assertReviewUnavailable(t, response)
		})
	}
	for _, test := range []struct {
		path string
		code apiv1.ErrorCode
	}{
		{"/v1/builds/owned", ""},
		{"/v1/builds/owned-queued", apiv1.ErrorCodeQueueAdmissionPending},
	} {
		before := reviews
		request := httptest.NewRequest(http.MethodGet, test.path, nil)
		request.Header.Set("Authorization", "Bearer token")
		response := httptest.NewRecorder()
		srv.routes().ServeHTTP(response, request)
		if test.code == "" {
			if response.Code != http.StatusOK {
				t.Fatalf("owner get status=%d body=%s", response.Code, response.Body.String())
			}
		} else {
			assertAPIError(t, response, http.StatusServiceUnavailable, test.code, true)
		}
		if reviews != before {
			t.Fatalf("owner get made %d redundant authorization reviews", reviews-before)
		}
	}
	var stored kovav1.KovaBuild
	if err := srv.reader.Get(context.Background(), client.ObjectKey{Namespace: srv.cfg.Namespace, Name: other.Name}, &stored); err != nil {
		t.Fatal(err)
	}
	if stored.Annotations[kovav1.CancellationRequestedAnnotation] != "" {
		t.Fatal("authorization uncertainty mutated non-owner build")
	}
}

func TestDefinitiveAuthorizationDenialPreservesOwnerFallbacks(t *testing.T) {
	srv := newTestServer(t, &fakeKube{})
	owned := &kovav1.KovaBuild{
		ObjectMeta: metav1.ObjectMeta{Name: "owned", Namespace: srv.cfg.Namespace, Labels: map[string]string{requesterLabel: requesterID("test-user")}},
		Spec:       kovav1.KovaBuildSpec{Requester: kovav1.KovaBuildRequester{Username: "test-user"}},
	}
	if err := srv.client.Create(context.Background(), owned); err != nil {
		t.Fatal(err)
	}
	queued := &kovav1.KovaBuild{
		ObjectMeta: metav1.ObjectMeta{Name: "queued", Namespace: srv.cfg.Namespace},
		Spec:       kovav1.KovaBuildSpec{Requester: kovav1.KovaBuildRequester{Username: "other-user"}},
	}
	if _, _, err := srv.queueStore().Reserve(context.Background(), queued); err != nil {
		t.Fatal(err)
	}
	srv.authz = authorizerFunc(func(context.Context, serviceauth.Principal, serviceauth.Attributes) error {
		return errors.New("definitively denied")
	})
	for _, test := range []struct {
		path string
		want int
	}{
		{"/v1/builds", http.StatusOK},
		{"/v1/builds/owned", http.StatusOK},
		{"/v1/builds/queued", http.StatusNotFound},
		{"/v1/builds/absent", http.StatusNotFound},
	} {
		request := httptest.NewRequest(http.MethodGet, test.path, nil)
		request.Header.Set("Authorization", "Bearer token")
		response := httptest.NewRecorder()
		srv.routes().ServeHTTP(response, request)
		if response.Code != test.want {
			t.Fatalf("%s status=%d want=%d body=%s", test.path, response.Code, test.want, response.Body.String())
		}
	}
}

func assertReviewUnavailable(t *testing.T, response *httptest.ResponseRecorder) {
	t.Helper()
	assertAPIError(t, response, http.StatusServiceUnavailable, apiv1.ErrorCodeInternal, true)
	if response.Header().Get("Retry-After") != "5" {
		t.Fatalf("Retry-After=%q, want 5", response.Header().Get("Retry-After"))
	}
}
