package buildresult

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"

	"github.com/google/go-containerregistry/pkg/name"
)

type registryResolverFunc func(context.Context, string, []string) (string, error)

func (fn registryResolverFunc) Resolve(ctx context.Context, target string, plainHTTP []string) (string, error) {
	return fn(ctx, target, plainHTTP)
}

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
	build := &kovav1.KovaBuild{Spec: kovav1.KovaBuildSpec{Targets: []string{"registry.example/demo:payload"}, Build: kovav1.KovaBuildOptions{Format: "both"}}}
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
	build := &kovav1.KovaBuild{Spec: kovav1.KovaBuildSpec{Targets: []string{"registry.example/demo:payload"}, Build: kovav1.KovaBuildOptions{Format: "both"}}}
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
	build := &kovav1.KovaBuild{Spec: kovav1.KovaBuildSpec{Targets: []string{"registry.example/demo:payload"}, Build: kovav1.KovaBuildOptions{Format: "oci"}}, Status: kovav1.KovaBuildStatus{Phase: kovav1.PhaseCancelled}}
	results := Resolve(context.Background(), exporterFunc(func(context.Context, *kovav1.KovaBuild, string, string) ([]byte, error) {
		called = true
		return nil, nil
	}), build, nil)
	if called || len(results) != 1 || results[0].Status != "cancelled" {
		t.Fatalf("called=%v results=%#v", called, results)
	}
}

func TestPendingExpandsBatchTargetsAndFormats(t *testing.T) {
	build := &kovav1.KovaBuild{Spec: kovav1.KovaBuildSpec{
		Targets: []string{"registry.example/a:dev", "registry.example/b:dev"},
		Build:   kovav1.KovaBuildOptions{Format: "both"},
	}}
	results := Pending(build)
	if len(results) != 4 || results[0].Repository != "registry.example/a:dev_nydus_v3" ||
		results[1].Repository != "registry.example/a:dev" ||
		results[2].Repository != "registry.example/b:dev_nydus_v3" ||
		results[3].Repository != "registry.example/b:dev" {
		t.Fatalf("results = %#v", results)
	}
}

func TestHundredLogicalTargetsExpandToTwoHundredConcreteOutputs(t *testing.T) {
	targets := make([]string, kovav1.MaxLogicalTargets)
	for index := range targets {
		targets[index] = fmt.Sprintf("registry.example/image-%03d:dev", index)
	}
	pending := Pending(&kovav1.KovaBuild{Spec: kovav1.KovaBuildSpec{
		Targets: targets,
		Build:   kovav1.KovaBuildOptions{Format: "both", Concurrency: kovav1.MaxBuildConcurrency},
	}})
	if len(pending) != kovav1.MaxConcreteOutputs {
		t.Fatalf("concrete outputs = %d, want %d", len(pending), kovav1.MaxConcreteOutputs)
	}
	for index := range pending {
		pending[index].Status = "succeeded"
		pending[index].ManifestDigest = "sha256:" + strings.Repeat("a", 64)
	}
	if outputs := Outputs(pending); len(outputs) != kovav1.MaxConcreteOutputs {
		t.Fatalf("stored outputs = %d, want %d", len(outputs), kovav1.MaxConcreteOutputs)
	}
}

func TestResolvePreservesSuccessfulDigestsWhenAnotherRegistryFails(t *testing.T) {
	build := &kovav1.KovaBuild{Spec: kovav1.KovaBuildSpec{
		Targets: []string{"registry.example/a:dev", "registry.example/b:dev"},
		Build:   kovav1.KovaBuildOptions{Format: "oci", Concurrency: 2},
	}}
	exporter := exporterFunc(func(context.Context, *kovav1.KovaBuild, string, string) ([]byte, error) {
		return []byte("{\"target\":\"registry.example/a:dev\",\"success\":true}\n" +
			"{\"target\":\"registry.example/b:dev\",\"success\":true}\n"), nil
	})
	results := resolveWithRegistry(context.Background(), exporter, registryResolverFunc(func(_ context.Context, target string, _ []string) (string, error) {
		if strings.Contains(target, "/b:") {
			return "", fmt.Errorf("registry unavailable")
		}
		return "sha256:" + strings.Repeat("a", 64), nil
	}), build, nil)
	if AllSucceeded(results) {
		t.Fatal("expected overall failure")
	}
	outputs := Outputs(results)
	if len(outputs) != 1 || outputs[0].Image != "registry.example/a:dev" || outputs[0].ManifestDigest == "" {
		t.Fatalf("outputs = %#v", outputs)
	}
}

func TestResolveBoundsManifestVerificationConcurrency(t *testing.T) {
	const count = 20
	targets := make([]string, count)
	var exported strings.Builder
	for index := range targets {
		targets[index] = fmt.Sprintf("registry.example/image-%02d:dev", index)
		fmt.Fprintf(&exported, "{\"target\":%q,\"success\":true}\n", targets[index])
	}
	build := &kovav1.KovaBuild{
		Spec:   kovav1.KovaBuildSpec{Targets: targets, Build: kovav1.KovaBuildOptions{Format: "oci", Concurrency: count}},
		Status: kovav1.KovaBuildStatus{AllocatedConcurrency: 4},
	}
	var active, maximum atomic.Int32
	resolver := registryResolverFunc(func(context.Context, string, []string) (string, error) {
		current := active.Add(1)
		defer active.Add(-1)
		for {
			observed := maximum.Load()
			if current <= observed || maximum.CompareAndSwap(observed, current) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		return "sha256:" + strings.Repeat("b", 64), nil
	})
	results := resolveWithRegistry(context.Background(), exporterFunc(func(context.Context, *kovav1.KovaBuild, string, string) ([]byte, error) {
		return []byte(exported.String()), nil
	}), resolver, build, nil)
	if !AllSucceeded(results) {
		t.Fatalf("results = %#v", results)
	}
	if got := maximum.Load(); got < 2 || got > 4 {
		t.Fatalf("maximum registry concurrency = %d, want 2..4", got)
	}
}
