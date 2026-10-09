package buildresult

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/runnerexec"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

func TestCollectReceiptsPersistsExactPushAndFailsClosedOnMissingDigest(t *testing.T) {
	const digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	build := &kovav1.KovaBuild{}
	results := []kovav1.BuildVerificationResult{
		{Format: "oci", Image: "registry.example/one:dev", Platform: "linux/amd64", State: "pending"},
		{Format: "oci", Image: "registry.example/two:dev", Platform: "linux/amd64", State: "pending"},
	}
	transient, hard := CollectReceipts(context.Background(), exporterFunc(func(_ context.Context, _ *kovav1.KovaBuild, path, query string) ([]byte, error) {
		if path != "export" || query != "with-fail=true&summary=true&oci=true" {
			t.Fatalf("path=%q query=%q", path, query)
		}
		return []byte(fmt.Sprintf("{\"target\":%q,\"success\":true,\"manifest_digest\":%q}\n{\"target\":%q,\"success\":true}\n", results[0].Image, digest, results[1].Image)), nil
	}), build, results)
	if transient != "" || !hard || results[0].PushedDigest != digest || results[0].State != "pending" || results[1].State != "failed" {
		t.Fatalf("transient=%q hard=%v results=%#v", transient, hard, results)
	}
}

func TestCollectReceiptsTransportErrorRemainsPending(t *testing.T) {
	results := []kovav1.BuildVerificationResult{{Format: "oci", Image: "registry.example/one:dev", Platform: "linux/amd64", State: "pending"}}
	transient, hard := CollectReceipts(context.Background(), exporterFunc(func(context.Context, *kovav1.KovaBuild, string, string) ([]byte, error) {
		return nil, context.DeadlineExceeded
	}), &kovav1.KovaBuild{}, results)
	if transient == "" || hard || results[0].State != "pending" || results[0].PushedDigest != "" {
		t.Fatalf("transient=%q hard=%v results=%#v", transient, hard, results)
	}
}

func TestCollectReceiptsRetainsOptionalObservationOnSuccessAndFailure(t *testing.T) {
	for _, success := range []bool{true, false} {
		results := []kovav1.BuildVerificationResult{{Format: "oci", Image: "registry.example/app:dev", Platform: "linux/amd64", State: "pending"}}
		transient, hard := CollectReceipts(context.Background(), exporterFunc(func(context.Context, *kovav1.KovaBuild, string, string) ([]byte, error) {
			return []byte(fmt.Sprintf(`{"target":"registry.example/app:dev","success":%t,"manifest_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","build_observation":{"schemaVersion":1,"availability":"unavailable","reason":"malformed_stream","exportAvailability":"unavailable","pushAvailability":"unavailable","nydusAvailability":"unavailable","workerSessionsAvailability":"unavailable"}}`+"\n", success)), nil
		}), &kovav1.KovaBuild{}, results)
		if transient != "" || hard == success || results[0].BuildObservation.Availability != "unavailable" || results[0].BuildObservation.Reason != "malformed_stream" {
			t.Fatalf("success=%t hard=%t results=%#v", success, hard, results)
		}
		copy := (&kovav1.KovaBuild{Status: kovav1.KovaBuildStatus{VerificationResults: results}}).DeepCopy()
		copy.Status.VerificationResults[0].BuildObservation.Reason = "invalid_observation"
		if results[0].BuildObservation.Reason != "malformed_stream" {
			t.Fatal("Kubernetes DeepCopy aliased telemetry")
		}
	}
}

func TestCollectReceiptsOversizedExportFailsClosed(t *testing.T) {
	results := []kovav1.BuildVerificationResult{{Format: "oci", Image: "registry.example/one:dev", Platform: "linux/amd64", State: "pending"}}
	transient, hard := CollectReceipts(context.Background(), exporterFunc(func(context.Context, *kovav1.KovaBuild, string, string) ([]byte, error) {
		return nil, fmt.Errorf("exec: %w", runnerexec.ErrExportTooLarge)
	}), &kovav1.KovaBuild{}, results)
	if transient != "" || !hard || results[0].State != "failed" {
		t.Fatalf("transient=%q hard=%v results=%#v", transient, hard, results)
	}
}

func TestCollectNydusReceiptRequiresConverterPushDigest(t *testing.T) {
	const digest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	results := []kovav1.BuildVerificationResult{{Format: "nydus", Image: "registry.example/one:dev_nydus_v3", Platform: "linux/amd64", State: "pending"}}
	transient, hard := CollectReceipts(context.Background(), exporterFunc(func(_ context.Context, _ *kovav1.KovaBuild, _, query string) ([]byte, error) {
		if query != "with-fail=true&summary=true" {
			t.Fatalf("query=%q", query)
		}
		return []byte(fmt.Sprintf("{\"target\":%q,\"success\":true,\"manifest_digest\":%q}\n", results[0].Image, digest)), nil
	}), &kovav1.KovaBuild{}, results)
	if transient != "" || hard || results[0].PushedDigest != digest {
		t.Fatalf("transient=%q hard=%v results=%#v", transient, hard, results)
	}
	results[0].PushedDigest = ""
	transient, hard = CollectReceipts(context.Background(), exporterFunc(func(context.Context, *kovav1.KovaBuild, string, string) ([]byte, error) {
		return []byte(fmt.Sprintf("{\"target\":%q,\"success\":true}\n", results[0].Image)), nil
	}), &kovav1.KovaBuild{}, results)
	if transient != "" || !hard || results[0].State != "failed" {
		t.Fatalf("missing digest transient=%q hard=%v results=%#v", transient, hard, results)
	}
}

func TestVerifyReceiptsPinsDigestDespiteSameTagOverwrite(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	server := httptest.NewServer(registry.New())
	defer server.Close()
	host := strings.TrimPrefix(server.URL, "http://")
	ref, err := name.NewTag(host+"/team/image:shared", name.Insecure)
	if err != nil {
		t.Fatal(err)
	}
	imageA, digestA := imageForPlatform(t, "amd64", "build-a")
	imageB, digestB := imageForPlatform(t, "amd64", "build-b")
	if err := remote.Write(ref, imageA); err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(ref, imageB); err != nil {
		t.Fatal(err)
	}
	results := []kovav1.BuildVerificationResult{{Format: "oci", Image: ref.Name(), Platform: "linux/amd64", PushedDigest: digestA, State: "pending"}}
	transient, hard := VerifyRemoteReceipts(context.Background(), results, []string{host}, 16)
	if digestA == digestB || transient != "" || hard || results[0].State != "succeeded" || results[0].PushedDigest != digestA {
		t.Fatalf("digestA=%q digestB=%q transient=%q hard=%v results=%#v", digestA, digestB, transient, hard, results)
	}
}

func TestVerifyReceiptsNeverFallsBackToOverwrittenTagWhenPushedDigestUnavailable(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	registryHandler := registry.New()
	var digestA string
	var blockOldDigest atomic.Bool
	var digestReads, tagReads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if blockOldDigest.Load() {
			switch r.URL.Path {
			case "/v2/team/image/manifests/" + digestA:
				digestReads.Add(1)
				http.NotFound(w, r)
				return
			case "/v2/team/image/manifests/shared":
				tagReads.Add(1)
			}
		}
		registryHandler.ServeHTTP(w, r)
	}))
	defer server.Close()
	host := strings.TrimPrefix(server.URL, "http://")
	ref, err := name.NewTag(host+"/team/image:shared", name.Insecure)
	if err != nil {
		t.Fatal(err)
	}
	imageA, pushedDigest := imageForPlatform(t, "amd64", "build-a")
	imageB, digestB := imageForPlatform(t, "amd64", "build-b")
	digestA = pushedDigest
	if digestA == digestB {
		t.Fatal("test images must have different manifest digests")
	}
	if err := remote.Write(ref, imageA); err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(ref, imageB); err != nil {
		t.Fatal(err)
	}
	current, err := remote.Get(ref)
	if err != nil {
		t.Fatal(err)
	}
	if current.Descriptor.Digest.String() != digestB {
		t.Fatalf("tag digest = %s, want competing build's %s", current.Descriptor.Digest, digestB)
	}

	// A registry may stop serving A by digest after B overwrites the tag.
	// Retrying A is safe; resolving the mutable tag would silently report B.
	blockOldDigest.Store(true)
	results := []kovav1.BuildVerificationResult{{Format: "oci", Image: ref.Name(), Platform: "linux/amd64", PushedDigest: digestA, State: "pending"}}
	transient, hard := VerifyRemoteReceipts(context.Background(), results, []string{host}, 16)
	if transient == "" || hard || results[0].State != "pending" || results[0].PushedDigest != digestA {
		t.Fatalf("transient=%q hard=%v results=%#v, want A pending", transient, hard, results)
	}
	if digestReads.Load() == 0 || tagReads.Load() != 0 {
		t.Fatalf("digest reads=%d tag reads=%d, want only digest-pinned reads", digestReads.Load(), tagReads.Load())
	}
	blockOldDigest.Store(false)
	transient, hard = VerifyRemoteReceipts(context.Background(), results, []string{host}, 16)
	outputs := VerificationOutputs(results)
	if transient != "" || hard || results[0].State != "succeeded" ||
		len(outputs) != 1 || outputs[0].ManifestDigest != digestA {
		t.Fatalf("recovery transient=%q hard=%v results=%#v outputs=%#v, want A verified", transient, hard, results, outputs)
	}
}

func TestVerifyReceiptsDistinguishesTransientAndDefinitiveMismatch(t *testing.T) {
	const digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	results := []kovav1.BuildVerificationResult{
		{Format: "oci", Image: "registry.example/temporary:dev", Platform: "linux/amd64", PushedDigest: digest, State: "pending"},
		{Format: "oci", Image: "registry.example/wrong:dev", Platform: "linux/amd64", PushedDigest: digest, State: "pending"},
	}
	transient, hard := VerifyReceipts(context.Background(), registryResolverFunc(func(_ context.Context, target, pushedDigest string, _ []string) (string, string, error) {
		if pushedDigest != digest {
			t.Fatalf("digest = %q", pushedDigest)
		}
		if strings.Contains(target, "temporary") {
			return "", "", errors.New("registry unavailable")
		}
		return digest, "linux/arm64", nil
	}), results, nil, 16)
	if transient != "registry manifest verification unavailable" || !hard || results[0].State != "pending" || results[1].State != "failed" {
		t.Fatalf("transient=%q hard=%v results=%#v", transient, hard, results)
	}
}

func TestVerifyReceiptsBoundsConcurrentRegistryCalls(t *testing.T) {
	const digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	results := make([]kovav1.BuildVerificationResult, 20)
	for i := range results {
		results[i] = kovav1.BuildVerificationResult{Format: "oci", Image: fmt.Sprintf("registry.example/%d:dev", i), Platform: "linux/amd64", PushedDigest: digest, State: "pending"}
	}
	var active, peak atomic.Int32
	transient, hard := VerifyReceipts(context.Background(), registryResolverFunc(func(context.Context, string, string, []string) (string, string, error) {
		count := active.Add(1)
		for {
			old := peak.Load()
			if count <= old || peak.CompareAndSwap(old, count) {
				break
			}
		}
		active.Add(-1)
		return digest, "linux/amd64", nil
	}), results, nil, 16)
	if transient != "" || hard || peak.Load() > 4 {
		t.Fatalf("transient=%q hard=%v peak=%d", transient, hard, peak.Load())
	}
	var succeeded int
	for _, result := range results {
		if result.State == "succeeded" {
			succeeded++
		}
	}
	if succeeded != 16 {
		t.Fatalf("succeeded=%d, want 16", succeeded)
	}
}
