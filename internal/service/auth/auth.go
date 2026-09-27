package auth

import (
	"context"
	"crypto/subtle"
	"errors"
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

// ErrReviewUnavailable means Kubernetes could not determine an authentication
// or authorization decision. Callers must fail closed without treating it as
// a definitive credential or policy denial.
var ErrReviewUnavailable = errors.New("identity review unavailable")

type Authenticator interface {
	Authenticate(context.Context, string) (Principal, error)
}

type Principal struct {
	Username string              `json:"username"`
	UID      string              `json:"uid,omitempty"`
	Groups   []string            `json:"groups,omitempty"`
	Extra    map[string][]string `json:"extra,omitempty"`
}

type TokenReviewer interface {
	Create(context.Context, *authenticationv1.TokenReview, metav1.CreateOptions) (*authenticationv1.TokenReview, error)
}

func New(mode, staticToken, staticPrincipal string, reviewer TokenReviewer) (Authenticator, error) {
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
		if strings.TrimSpace(staticPrincipal) == "" {
			return nil, fmt.Errorf("static authentication requires a non-empty principal")
		}
		return static{token: staticToken, principal: strings.TrimSpace(staticPrincipal)}, nil
	case ModeUnsafeNone:
		return unsafeNone{}, nil
	default:
		return nil, fmt.Errorf("unsupported authentication mode %q", mode)
	}
}

type tokenReview struct {
	reviewer TokenReviewer
}

func (a tokenReview) Authenticate(ctx context.Context, token string) (Principal, error) {
	if strings.TrimSpace(token) == "" {
		return Principal{}, fmt.Errorf("bearer token is required")
	}
	review, err := a.reviewer.Create(ctx, &authenticationv1.TokenReview{
		Spec: authenticationv1.TokenReviewSpec{Token: token},
	}, metav1.CreateOptions{})
	if err != nil {
		// Do not forward a reviewer error: it may contain credential material.
		return Principal{}, fmt.Errorf("TokenReview request failed: %w", ErrReviewUnavailable)
	}
	if review == nil || strings.TrimSpace(review.Status.Error) != "" {
		return Principal{}, fmt.Errorf("TokenReview could not check token: %w", ErrReviewUnavailable)
	}
	if !review.Status.Authenticated {
		return Principal{}, fmt.Errorf("bearer token was not authenticated")
	}
	if strings.TrimSpace(review.Status.User.Username) == "" {
		return Principal{}, fmt.Errorf("authenticated TokenReview has no username: %w", ErrReviewUnavailable)
	}
	extra := make(map[string][]string, len(review.Status.User.Extra))
	for key, values := range review.Status.User.Extra {
		extra[key] = append([]string(nil), values...)
	}
	return Principal{
		Username: review.Status.User.Username,
		UID:      review.Status.User.UID,
		Groups:   append([]string(nil), review.Status.User.Groups...),
		Extra:    extra,
	}, nil
}

type static struct {
	token     string
	principal string
}

func (a static) Authenticate(_ context.Context, token string) (Principal, error) {
	if subtle.ConstantTimeCompare([]byte(token), []byte(a.token)) != 1 {
		return Principal{}, fmt.Errorf("bearer token was not authenticated")
	}
	return Principal{Username: a.principal}, nil
}

type unsafeNone struct{}

func (unsafeNone) Authenticate(context.Context, string) (Principal, error) {
	return Principal{Username: "system:anonymous", Groups: []string{"system:unauthenticated"}}, nil
}

func Bearer(header string) (string, error) {
	scheme, token, ok := strings.Cut(strings.TrimSpace(header), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || strings.TrimSpace(token) == "" {
		return "", fmt.Errorf("a Bearer authorization header is required")
	}
	return strings.TrimSpace(token), nil
}
