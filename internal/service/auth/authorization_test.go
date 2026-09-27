package auth

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	authorizationv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type subjectAccessReviewerFunc func(context.Context, *authorizationv1.SubjectAccessReview, metav1.CreateOptions) (*authorizationv1.SubjectAccessReview, error)

func (fn subjectAccessReviewerFunc) Create(ctx context.Context, review *authorizationv1.SubjectAccessReview, opts metav1.CreateOptions) (*authorizationv1.SubjectAccessReview, error) {
	return fn(ctx, review, opts)
}

func TestSubjectAccessReviewAuthorization(t *testing.T) {
	authorizer, err := NewSubjectAccessReviewAuthorizer(subjectAccessReviewerFunc(func(_ context.Context, review *authorizationv1.SubjectAccessReview, _ metav1.CreateOptions) (*authorizationv1.SubjectAccessReview, error) {
		if review.Spec.User != "alice" || review.Spec.UID != "uid-1" || len(review.Spec.Groups) != 1 || review.Spec.Groups[0] != "builders" ||
			len(review.Spec.Extra["scope"]) != 1 || review.Spec.Extra["scope"][0] != "team-a" ||
			review.Spec.ResourceAttributes.Verb != "get" || review.Spec.ResourceAttributes.Name != "build-1" {
			t.Fatalf("review = %#v", review.Spec)
		}
		return &authorizationv1.SubjectAccessReview{Status: authorizationv1.SubjectAccessReviewStatus{Allowed: true}}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	err = authorizer.Authorize(context.Background(), Principal{Username: "alice", UID: "uid-1", Groups: []string{"builders"}, Extra: map[string][]string{"scope": {"team-a"}}}, Attributes{
		Verb: "get", Namespace: "kova", Resource: "kovabuilds", Name: "build-1",
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSubjectAccessReviewDenial(t *testing.T) {
	authorizer, err := NewSubjectAccessReviewAuthorizer(subjectAccessReviewerFunc(func(context.Context, *authorizationv1.SubjectAccessReview, metav1.CreateOptions) (*authorizationv1.SubjectAccessReview, error) {
		return &authorizationv1.SubjectAccessReview{Status: authorizationv1.SubjectAccessReviewStatus{Allowed: false, Reason: "denied by policy"}}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := authorizer.Authorize(context.Background(), Principal{Username: "alice"}, Attributes{Verb: "create"}); err == nil {
		t.Fatal("expected authorization denial")
	}
}

func TestSubjectAccessReviewDecisionCompleteness(t *testing.T) {
	tests := []struct {
		name        string
		review      *authorizationv1.SubjectAccessReview
		err         error
		allowed     bool
		unavailable bool
	}{
		{name: "transport error", err: fmt.Errorf("secret-principal-must-not-leak"), unavailable: true},
		{name: "nil review", unavailable: true},
		{name: "undecided evaluation error", review: &authorizationv1.SubjectAccessReview{Status: authorizationv1.SubjectAccessReviewStatus{EvaluationError: "secret-principal-must-not-leak"}}, unavailable: true},
		{name: "explicit denial with evaluation error", review: &authorizationv1.SubjectAccessReview{Status: authorizationv1.SubjectAccessReviewStatus{Denied: true, EvaluationError: "partial evaluation"}}},
		{name: "ordinary denial", review: &authorizationv1.SubjectAccessReview{}},
		{name: "allow with evaluation error", review: &authorizationv1.SubjectAccessReview{Status: authorizationv1.SubjectAccessReviewStatus{Allowed: true, EvaluationError: "partial evaluation"}}, allowed: true},
		{name: "contradictory allow and deny", review: &authorizationv1.SubjectAccessReview{Status: authorizationv1.SubjectAccessReviewStatus{Allowed: true, Denied: true}}, unavailable: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authorizer, err := NewSubjectAccessReviewAuthorizer(subjectAccessReviewerFunc(func(context.Context, *authorizationv1.SubjectAccessReview, metav1.CreateOptions) (*authorizationv1.SubjectAccessReview, error) {
				return test.review, test.err
			}))
			if err != nil {
				t.Fatal(err)
			}
			err = authorizer.Authorize(context.Background(), Principal{Username: "alice"}, Attributes{Verb: "get"})
			if (err == nil) != test.allowed || errors.Is(err, ErrReviewUnavailable) != test.unavailable {
				t.Fatalf("allowed=%t unavailable=%t err=%v", test.allowed, test.unavailable, err)
			}
			if err != nil && strings.Contains(err.Error(), "secret-principal-must-not-leak") {
				t.Fatal("reviewer error leaked")
			}
		})
	}
}
