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
	authenticator, err := New(ModeStatic, "secret", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := authenticator.Authenticate(context.Background(), "secret"); err != nil {
		t.Fatal(err)
	}
	if err := authenticator.Authenticate(context.Background(), "wrong"); err == nil {
		t.Fatal("expected invalid token to fail")
	}
}

func TestTokenReviewAuthentication(t *testing.T) {
	authenticator, err := New(ModeTokenReview, "", reviewerFunc(func(_ context.Context, review *authenticationv1.TokenReview, _ metav1.CreateOptions) (*authenticationv1.TokenReview, error) {
		return &authenticationv1.TokenReview{Status: authenticationv1.TokenReviewStatus{Authenticated: review.Spec.Token == "valid"}}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := authenticator.Authenticate(context.Background(), "valid"); err != nil {
		t.Fatal(err)
	}
	if err := authenticator.Authenticate(context.Background(), "invalid"); err == nil {
		t.Fatal("expected unauthenticated review to fail")
	}
}

func TestAuthenticationModesAreExplicit(t *testing.T) {
	if _, err := New(ModeStatic, "", nil); err == nil {
		t.Fatal("expected empty static token to fail")
	}
	if _, err := New(ModeTokenReview, "", nil); err == nil {
		t.Fatal("expected missing token reviewer to fail")
	}
	unsafe, err := New(ModeUnsafeNone, "", nil)
	if err != nil || unsafe.Authenticate(context.Background(), "") != nil {
		t.Fatalf("unsafe mode: authenticator=%#v err=%v", unsafe, err)
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
