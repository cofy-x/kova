package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	serviceauth "github.com/cofy-x/kova/internal/service/auth"
)

type fixedPrincipalAuthenticator struct{ principal serviceauth.Principal }

func (a fixedPrincipalAuthenticator) Authenticate(context.Context, string) (serviceauth.Principal, error) {
	return a.principal, nil
}

func TestUnsupportedAuthenticatedIdentityDoesNotReserveQueue(t *testing.T) {
	for _, principal := range []serviceauth.Principal{
		{Username: strings.Repeat("界", 254)},
		{Username: "alice", UID: strings.Repeat("界", 254)},
	} {
		srv := newTestServer(t, &fakeKube{})
		srv.auth = fixedPrincipalAuthenticator{principal: principal}
		response := httptest.NewRecorder()
		srv.routes().ServeHTTP(response, queueRequest(t, "identity-boundary"))
		if response.Code != http.StatusBadRequest {
			t.Fatalf("unsupported identity returned %d", response.Code)
		}
		id := idempotentJobID(principal.Username, "identity-boundary")
		if _, found, err := srv.queueStore().Lookup(context.Background(), id); err != nil || found {
			t.Fatalf("unsupported identity reserved queue intent: found=%t err=%v", found, err)
		}
	}
}
