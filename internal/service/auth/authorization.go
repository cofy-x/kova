package auth

import (
	"context"
	"fmt"
	"strings"

	authorizationv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ServiceBuildResource is the virtual Kubernetes authorization resource used
// by the HTTP service. It intentionally does not name the KovaBuild CRD: users
// may be authorized to call the service without receiving direct write access
// to controller-owned build state.
const ServiceBuildResource = "servicebuilds"

type Attributes struct {
	Verb      string
	Namespace string
	Resource  string
	Name      string
}

type Authorizer interface {
	Authorize(context.Context, Principal, Attributes) error
}

type SubjectAccessReviewer interface {
	Create(context.Context, *authorizationv1.SubjectAccessReview, metav1.CreateOptions) (*authorizationv1.SubjectAccessReview, error)
}

type subjectAccessReview struct {
	reviewer SubjectAccessReviewer
}

func NewSubjectAccessReviewAuthorizer(reviewer SubjectAccessReviewer) (Authorizer, error) {
	if reviewer == nil {
		return nil, fmt.Errorf("subjectaccessreview authorization requires a Kubernetes client")
	}
	return subjectAccessReview{reviewer: reviewer}, nil
}

func (a subjectAccessReview) Authorize(ctx context.Context, principal Principal, attrs Attributes) error {
	review, err := a.reviewer.Create(ctx, &authorizationv1.SubjectAccessReview{
		Spec: authorizationv1.SubjectAccessReviewSpec{
			User:   principal.Username,
			UID:    principal.UID,
			Groups: append([]string(nil), principal.Groups...),
			Extra:  principalExtra(principal.Extra),
			ResourceAttributes: &authorizationv1.ResourceAttributes{
				Group:     "kova.cofy.dev",
				Version:   "v1alpha1",
				Resource:  attrs.Resource,
				Namespace: attrs.Namespace,
				Verb:      attrs.Verb,
				Name:      attrs.Name,
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		// A transport failure is not an RBAC denial. Keep the raw reviewer error
		// out of the public path because it may contain principal details.
		return fmt.Errorf("SubjectAccessReview request failed: %w", ErrReviewUnavailable)
	}
	if review == nil {
		return fmt.Errorf("SubjectAccessReview returned no decision: %w", ErrReviewUnavailable)
	}
	// Kubernetes can report an EvaluationError together with a conclusive
	// allow or deny. Only an unresolved evaluation must become unavailable.
	if review.Status.Allowed {
		if review.Status.Denied {
			return fmt.Errorf("SubjectAccessReview returned contradictory decision: %w", ErrReviewUnavailable)
		}
		return nil
	}
	if !review.Status.Denied && strings.TrimSpace(review.Status.EvaluationError) != "" {
		return fmt.Errorf("SubjectAccessReview could not determine access: %w", ErrReviewUnavailable)
	}
	reason := strings.TrimSpace(review.Status.Reason)
	if reason == "" {
		reason = "access denied"
	}
	return fmt.Errorf("%s", reason)
}

func principalExtra(extra map[string][]string) map[string]authorizationv1.ExtraValue {
	out := make(map[string]authorizationv1.ExtraValue, len(extra))
	for key, values := range extra {
		out[key] = authorizationv1.ExtraValue(append([]string(nil), values...))
	}
	return out
}

type AllowAllAuthorizer struct{}

func (AllowAllAuthorizer) Authorize(context.Context, Principal, Attributes) error { return nil }
