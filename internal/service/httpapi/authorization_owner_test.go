package httpapi

import (
	"context"
	"errors"
	"testing"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	serviceauth "github.com/cofy-x/kova/internal/service/auth"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBuildOwnerSkipsRedundantAuthorizationReview(t *testing.T) {
	build := &kovav1.KovaBuild{
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "jobs"},
		Spec: kovav1.KovaBuildSpec{
			Requester: kovav1.KovaBuildRequester{Username: "alice"},
		},
	}
	var reviews int
	srv := &Server{cfg: testConfig(t.TempDir())}
	srv.authz = authorizerFunc(func(context.Context, serviceauth.Principal, serviceauth.Attributes) error {
		reviews++
		return errors.New("denied")
	})
	for _, verb := range []string{"get", "update", "delete"} {
		if err := srv.authorizeBuild(context.Background(), serviceauth.Principal{Username: "alice"}, verb, build); err != nil {
			t.Fatalf("owner %s: %v", verb, err)
		}
	}
	if reviews != 0 {
		t.Fatalf("owner operations made %d redundant authorization reviews", reviews)
	}
	if err := srv.authorizeBuild(context.Background(), serviceauth.Principal{Username: "bob"}, "get", build); err == nil {
		t.Fatal("non-owner passed a denied authorization review")
	}
	if reviews != 1 {
		t.Fatalf("non-owner made %d authorization reviews, want 1", reviews)
	}
	if err := srv.authorizeBuild(context.Background(), serviceauth.Principal{}, "get", build); err == nil {
		t.Fatal("empty principal passed a denied authorization review")
	}
	if reviews != 2 {
		t.Fatalf("empty principal made %d authorization reviews, want 2", reviews)
	}
}

func TestNonOwnerRetainsGrantedAuthorization(t *testing.T) {
	build := &kovav1.KovaBuild{
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "jobs"},
		Spec: kovav1.KovaBuildSpec{
			Requester: kovav1.KovaBuildRequester{Username: "alice"},
		},
	}
	var reviews int
	srv := &Server{cfg: testConfig(t.TempDir())}
	srv.authz = authorizerFunc(func(context.Context, serviceauth.Principal, serviceauth.Attributes) error {
		reviews++
		return nil
	})
	if err := srv.authorizeBuild(context.Background(), serviceauth.Principal{Username: "bob"}, "get", build); err != nil {
		t.Fatalf("authorized non-owner: %v", err)
	}
	if reviews != 1 {
		t.Fatalf("authorized non-owner made %d authorization reviews, want 1", reviews)
	}
}
