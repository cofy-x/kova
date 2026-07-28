package auth

import (
	"context"
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
		if review.Spec.User != "alice" || review.Spec.ResourceAttributes.Verb != "get" || review.Spec.ResourceAttributes.Name != "build-1" {
			t.Fatalf("review = %#v", review.Spec)
		}
		return &authorizationv1.SubjectAccessReview{Status: authorizationv1.SubjectAccessReviewStatus{Allowed: true}}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	err = authorizer.Authorize(context.Background(), Principal{Username: "alice"}, Attributes{
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
