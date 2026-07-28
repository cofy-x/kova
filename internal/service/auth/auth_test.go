package auth

import (
	"context"
	"testing"

	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type reviewerFunc func(context.Context, *authenticationv1.TokenReview, metav1.CreateOptions) (*authenticationv1.TokenReview, error)

func (fn reviewerFunc) Create(ctx context.Context, review *authenticationv1.TokenReview, opts metav1.CreateOptions) (*authenticationv1.TokenReview, error) {
	return fn(ctx, review, opts)
}

func TestStaticAuthentication(t *testing.T) {
	authenticator, err := New(ModeStatic, "secret", "kova:test", nil)
	if err != nil {
		t.Fatal(err)
	}
	principal, err := authenticator.Authenticate(context.Background(), "secret")
	if err != nil {
		t.Fatal(err)
	}
	if principal.Username != "kova:test" {
		t.Fatalf("principal = %#v", principal)
	}
	if _, err := authenticator.Authenticate(context.Background(), "wrong"); err == nil {
		t.Fatal("expected invalid token to fail")
	}
}

func TestTokenReviewAuthentication(t *testing.T) {
	authenticator, err := New(ModeTokenReview, "", "", reviewerFunc(func(_ context.Context, review *authenticationv1.TokenReview, _ metav1.CreateOptions) (*authenticationv1.TokenReview, error) {
		return &authenticationv1.TokenReview{Status: authenticationv1.TokenReviewStatus{
			Authenticated: review.Spec.Token == "valid",
			User:          authenticationv1.UserInfo{Username: "alice", UID: "uid-1", Groups: []string{"builders"}},
		}}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	principal, err := authenticator.Authenticate(context.Background(), "valid")
	if err != nil {
		t.Fatal(err)
	}
	if principal.Username != "alice" || principal.UID != "uid-1" || len(principal.Groups) != 1 {
		t.Fatalf("principal = %#v", principal)
	}
	if _, err := authenticator.Authenticate(context.Background(), "invalid"); err == nil {
		t.Fatal("expected unauthenticated review to fail")
	}
}

func TestAuthenticationModesAreExplicit(t *testing.T) {
	if _, err := New(ModeStatic, "", "principal", nil); err == nil {
		t.Fatal("expected empty static token to fail")
	}
	if _, err := New(ModeStatic, "token", "", nil); err == nil {
		t.Fatal("expected empty static principal to fail")
	}
	if _, err := New(ModeTokenReview, "", "", nil); err == nil {
		t.Fatal("expected missing token reviewer to fail")
	}
	unsafe, err := New(ModeUnsafeNone, "", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	principal, err := unsafe.Authenticate(context.Background(), "")
	if err != nil || principal.Username != "system:anonymous" {
		t.Fatalf("unsafe mode: principal=%#v err=%v", principal, err)
	}
}

func TestBearer(t *testing.T) {
	if token, err := Bearer("bearer value"); err != nil || token != "value" {
		t.Fatalf("token=%q err=%v", token, err)
	}
	if _, err := Bearer("value"); err == nil {
		t.Fatal("expected malformed header to fail")
	}
}
