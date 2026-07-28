package auth

import (
	"context"
	"crypto/subtle"
	"fmt"
	"strings"

	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	ModeTokenReview = "tokenreview"
	ModeStatic      = "static"
	ModeUnsafeNone  = "unsafe-none"
)

type Authenticator interface {
	Authenticate(context.Context, string) error
}

type TokenReviewer interface {
	Create(context.Context, *authenticationv1.TokenReview, metav1.CreateOptions) (*authenticationv1.TokenReview, error)
}

func New(mode, staticToken string, reviewer TokenReviewer) (Authenticator, error) {
	switch strings.TrimSpace(mode) {
	case "", ModeTokenReview:
		if reviewer == nil {
			return nil, fmt.Errorf("tokenreview authentication requires a Kubernetes client")
		}
		return tokenReview{reviewer: reviewer}, nil
	case ModeStatic:
		if strings.TrimSpace(staticToken) == "" {
			return nil, fmt.Errorf("static authentication requires a non-empty token")
		}
		return static{token: staticToken}, nil
	case ModeUnsafeNone:
		return unsafeNone{}, nil
	default:
		return nil, fmt.Errorf("unsupported authentication mode %q", mode)
	}
}

type tokenReview struct {
	reviewer TokenReviewer
}

func (a tokenReview) Authenticate(ctx context.Context, token string) error {
	if strings.TrimSpace(token) == "" {
		return fmt.Errorf("bearer token is required")
	}
	review, err := a.reviewer.Create(ctx, &authenticationv1.TokenReview{
		Spec: authenticationv1.TokenReviewSpec{Token: token},
	}, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("review bearer token: %w", err)
	}
	if !review.Status.Authenticated || review.Status.Error != "" {
		return fmt.Errorf("bearer token was not authenticated")
	}
	return nil
}

type static struct {
	token string
}

func (a static) Authenticate(_ context.Context, token string) error {
	if subtle.ConstantTimeCompare([]byte(token), []byte(a.token)) != 1 {
		return fmt.Errorf("bearer token was not authenticated")
	}
	return nil
}

type unsafeNone struct{}

func (unsafeNone) Authenticate(context.Context, string) error { return nil }

func Bearer(header string) (string, error) {
	scheme, token, ok := strings.Cut(strings.TrimSpace(header), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || strings.TrimSpace(token) == "" {
		return "", fmt.Errorf("a Bearer authorization header is required")
	}
	return strings.TrimSpace(token), nil
}
