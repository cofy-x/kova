package buildresult

import (
	"context"
	"testing"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"

	"github.com/google/go-containerregistry/pkg/name"
)

func TestReferenceOptionsUsePlainHTTPOnlyForLocalRegistries(t *testing.T) {
	local, err := name.ParseReference("host.docker.internal:5002/demo:dev", referenceOptions("host.docker.internal:5002/demo:dev", nil)...)
	if err != nil {
		t.Fatal(err)
	}
	remote, err := name.ParseReference("registry.example/demo:dev", referenceOptions("registry.example/demo:dev", nil)...)
	if err != nil {
		t.Fatal(err)
	}
	if local.Context().Registry.Scheme() != "http" {
		t.Fatalf("local registry scheme = %q", local.Context().Registry.Scheme())
	}
	if remote.Context().Registry.Scheme() != "https" {
		t.Fatalf("remote registry scheme = %q", remote.Context().Registry.Scheme())
	}
}

func TestReferenceOptionsUseConfiguredPlainHTTPRegistry(t *testing.T) {
	ref, err := name.ParseReference(
		"kind-registry:5000/demo:dev",
		referenceOptions("kind-registry:5000/demo:dev", []string{"kind-registry:5000"})...,
	)
	if err != nil {
		t.Fatal(err)
	}
	if ref.Context().Registry.Scheme() != "http" {
		t.Fatalf("configured registry scheme = %q", ref.Context().Registry.Scheme())
	}
}

type exporterFunc func(context.Context, *kovav1.KovaBuild, string, string) ([]byte, error)

func (fn exporterFunc) Post(ctx context.Context, build *kovav1.KovaBuild, path, query string) ([]byte, error) {
	return fn(ctx, build, path, query)
}

func TestResolvePreservesTypedPartialFailures(t *testing.T) {
	build := &kovav1.KovaBuild{Spec: kovav1.KovaBuildSpec{Build: kovav1.KovaBuildOptions{Format: "both", Target: "registry.example/demo:payload"}}}
	var queries []string
	results := Resolve(context.Background(), exporterFunc(func(_ context.Context, _ *kovav1.KovaBuild, path, query string) ([]byte, error) {
		if path != "export" {
			t.Fatalf("path = %q", path)
		}
		queries = append(queries, query)
		return []byte(`{"target":"registry.example/demo:payload","success":false,"reason":"oci failed"}` + "\n" +
			`{"target":"registry.example/demo:payload_nydus_v3","success":false,"reason":"nydus failed"}` + "\n"), nil
	}), build, nil)
	if len(results) != 2 || results[0].Status != "failed" || results[1].Status != "failed" || AllSucceeded(results) {
		t.Fatalf("results = %#v", results)
	}
	if len(queries) != 2 || queries[0] != "with-fail=true" || queries[1] != "with-fail=true&oci=true" {
		t.Fatalf("queries = %#v", queries)
	}
}

func TestResolvePreservesSuccessfulVariantWhenOtherExportFails(t *testing.T) {
	build := &kovav1.KovaBuild{Spec: kovav1.KovaBuildSpec{Build: kovav1.KovaBuildOptions{Format: "both", Target: "registry.example/demo:payload"}}}
	results := Resolve(context.Background(), exporterFunc(func(_ context.Context, _ *kovav1.KovaBuild, _, query string) ([]byte, error) {
		if query == "with-fail=true&oci=true" {
			return nil, context.DeadlineExceeded
		}
		return []byte(`{"target":"registry.example/demo:payload_nydus_v3","success":false,"reason":"nydus failed"}` + "\n"), nil
	}), build, nil)
	if len(results) != 2 || results[0].Error != "nydus failed" || results[1].Error != context.DeadlineExceeded.Error() {
		t.Fatalf("results = %#v", results)
	}
}

func TestCancelledResultsDoNotCallExporter(t *testing.T) {
	called := false
	build := &kovav1.KovaBuild{Spec: kovav1.KovaBuildSpec{Build: kovav1.KovaBuildOptions{Format: "oci", Target: "registry.example/demo:payload"}}, Status: kovav1.KovaBuildStatus{Phase: kovav1.PhaseCancelled}}
	results := Resolve(context.Background(), exporterFunc(func(context.Context, *kovav1.KovaBuild, string, string) ([]byte, error) {
		called = true
		return nil, nil
	}), build, nil)
	if called || len(results) != 1 || results[0].Status != "cancelled" {
		t.Fatalf("called=%v results=%#v", called, results)
	}
}
