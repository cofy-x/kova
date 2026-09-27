package sourcebundle

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cofy-x/kova/internal/source"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/static"
	"github.com/google/go-containerregistry/pkg/v1/types"
)

func TestValidateRequiresImmutableVerifiableSource(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	for _, uri := range []string{
		"oci://registry.example.com/team/source@sha256:" + strings.Repeat("b", 64),
		"https://sources.example.com/source.zip",
	} {
		if err := Validate(uri, digest); err != nil {
			t.Fatalf("Validate(%q) = %v", uri, err)
		}
	}
	for _, uri := range []string{
		"oci://registry.example.com/team/source:latest",
		"http://sources.example.com/source.zip",
		"https://user:password@sources.example.com/source.zip",
		"https://sources.example.com/source.zip?token=secret",
	} {
		if err := Validate(uri, digest); err == nil {
			t.Fatalf("expected %q to be rejected", uri)
		}
	}
	if err := Validate("https://sources.example.com/source.zip", "sha256:short"); err == nil {
		t.Fatal("expected invalid content digest rejection")
	}
}

func TestFetchHTTPSCountsStreamedBytesAndCleansTemp(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("1234"))
		w.(http.Flusher).Flush() // No trusted Content-Length preflight.
		_, _ = w.Write([]byte("56789"))
	}))
	defer server.Close()
	dir := t.TempDir()
	output := filepath.Join(dir, "source.zip")
	if err := os.WriteFile(output, []byte("existing"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := fetchWithLimit(context.Background(), server.URL+"/source.zip", "sha256:"+strings.Repeat("a", 64), output, nil, 8, server.Client())
	if !errors.Is(err, source.ErrArchiveTooLarge) {
		t.Fatalf("fetch error = %v, want compressed-byte limit", err)
	}
	raw, err := os.ReadFile(output)
	if err != nil || string(raw) != "existing" {
		t.Fatalf("existing output changed: %q, %v", raw, err)
	}
	assertOnlyOutput(t, dir, "source.zip")
}

func TestFetchHTTPSCancellationCleansTemp(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("partial"))
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done()
	}))
	defer server.Close()
	dir := t.TempDir()
	output := filepath.Join(dir, "source.zip")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- fetchWithLimit(ctx, server.URL+"/source.zip", "sha256:"+strings.Repeat("a", 64), output, nil, 64, server.Client())
	}()
	<-started
	cancel()
	if err := <-done; err == nil {
		t.Fatal("expected fetch cancellation")
	}
	assertOnlyOutput(t, dir, "")
}

func TestFetchOCICountsStreamedLayerBytesAndCleansTemp(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	server := httptest.NewServer(registry.New())
	defer server.Close()
	ref, err := name.NewTag(strings.TrimPrefix(server.URL, "http://")+"/team/source:test", name.Insecure)
	if err != nil {
		t.Fatal(err)
	}
	image, err := mutate.AppendLayers(empty.Image, static.NewLayer([]byte("123456789"), LayerMediaType))
	if err != nil {
		t.Fatal(err)
	}
	image = mutate.MediaType(image, types.OCIManifestSchema1)
	image = mutate.ConfigMediaType(image, types.OCIConfigJSON)
	if err := remote.Write(ref, image); err != nil {
		t.Fatal(err)
	}
	manifestDigest, err := image.Digest()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	output := filepath.Join(dir, "source.zip")
	uri := "oci://" + ref.Context().Digest(manifestDigest.String()).Name()
	err = fetchWithLimit(context.Background(), uri, "sha256:"+strings.Repeat("a", 64), output, nil, 8, http.DefaultClient)
	if !errors.Is(err, source.ErrArchiveTooLarge) {
		t.Fatalf("OCI fetch error = %v, want compressed-byte limit", err)
	}
	assertOnlyOutput(t, dir, "")
}

func TestFetchHTTPSAcceptsExactLimit(t *testing.T) {
	body := []byte("12345678")
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(body) }))
	defer server.Close()
	dir := t.TempDir()
	output := filepath.Join(dir, "source.zip")
	digest := sha256.Sum256(body)
	if err := fetchWithLimit(context.Background(), server.URL+"/source.zip", fmt.Sprintf("sha256:%x", digest), output, nil, int64(len(body)), server.Client()); err != nil {
		t.Fatal(err)
	}
	if raw, err := os.ReadFile(output); err != nil || string(raw) != string(body) {
		t.Fatalf("fetched output = %q, %v", raw, err)
	}
	assertOnlyOutput(t, dir, "source.zip")
}

func assertOnlyOutput(t *testing.T, dir, want string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if want == "" && len(entries) == 0 {
		return
	}
	if len(entries) != 1 || entries[0].Name() != want {
		t.Fatalf("output directory contains unexpected files: %v", entries)
	}
}
