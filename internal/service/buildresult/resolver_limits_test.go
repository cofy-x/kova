package buildresult

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type registryRoundTripFunc func(*http.Request) (*http.Response, error)

func (fn registryRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func TestBoundedRegistryTransportRejectsDishonestContentLength(t *testing.T) {
	actual := bytes.Repeat([]byte("a"), int(maxRegistryVerificationResponseBytes)+1)
	transport := boundedRegistryTransport{base: registryRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			// The server claims one byte, but sends more than the limit.
			ContentLength: 1,
			Body:          io.NopCloser(bytes.NewReader(actual)),
		}, nil
	})}
	response, err := transport.RoundTrip(httptest.NewRequest(http.MethodGet, "http://registry.example/v2/image", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	n, err := io.Copy(io.Discard, response.Body)
	if !errors.Is(err, errRegistryVerificationResponseTooLarge) || n != maxRegistryVerificationResponseBytes {
		t.Fatalf("read %d bytes, err=%v; want bounded response error", n, err)
	}
}

func TestBoundedRegistryTransportRejectsOversizedDeclaredLengthBeforeRead(t *testing.T) {
	read := false
	transport := boundedRegistryTransport{base: registryRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode:    http.StatusOK,
			ContentLength: maxRegistryVerificationResponseBytes + 1,
			Body:          readFlagBody{read: &read},
		}, nil
	})}
	_, err := transport.RoundTrip(httptest.NewRequest(http.MethodGet, "http://registry.example/v2/image", nil))
	if !errors.Is(err, errRegistryVerificationResponseTooLarge) || read {
		t.Fatalf("err=%v read=%v; want reject before body read", err, read)
	}
}

func TestBoundedRegistryBodyRejectsNoProgressAtLimit(t *testing.T) {
	body := boundedRegistryBody{ReadCloser: noProgressBody{}, remaining: 0}
	if n, err := body.Read(make([]byte, 1)); n != 0 || !errors.Is(err, io.ErrNoProgress) {
		t.Fatalf("read %d bytes, err=%v; want no-progress failure", n, err)
	}
}

type noProgressBody struct{}

func (noProgressBody) Read([]byte) (int, error) { return 0, nil }
func (noProgressBody) Close() error             { return nil }

type readFlagBody struct{ read *bool }

func (b readFlagBody) Read([]byte) (int, error) { *b.read = true; return 0, io.EOF }
func (b readFlagBody) Close() error             { return nil }

func TestRegistryResolverRejectsOversizedValidDigestConfigWithDishonestSize(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	config := []byte(`{"architecture":"amd64","os":"linux","config":{"Labels":{"padding":"` + strings.Repeat("a", int(maxRegistryVerificationResponseBytes)) + `"}}}`)
	configDigest := sha256Digest(config)
	// The config digest is valid, but the manifest's config.size is not. A
	// registry that streams this blob must not induce an unbounded ConfigFile read.
	manifest := []byte(fmt.Sprintf(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","size":%d,"digest":%q},"layers":[]}`,
		len(config)+1000, configDigest))
	server, host := testOversizedRegistry(t, manifest, config)
	defer server.Close()
	_, _, err := (remoteRegistryResolver{}).Resolve(context.Background(), host+"/team/image:dev", sha256Digest(manifest), []string{host})
	if !errors.Is(err, ErrDefinitive) || !errors.Is(err, errRegistryVerificationResponseTooLarge) {
		t.Fatalf("err=%v, want definitive bounded config failure", err)
	}
}

func TestRegistryResolverRejectsOversizedValidDigestManifest(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	manifest := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","annotations":{"padding":"` + strings.Repeat("a", int(maxRegistryVerificationResponseBytes)) + `"},"config":{"mediaType":"application/vnd.oci.image.config.v1+json","size":2,"digest":"sha256:` + strings.Repeat("0", 64) + `"},"layers":[]}`)
	server, host := testOversizedRegistry(t, manifest, nil)
	defer server.Close()
	_, _, err := (remoteRegistryResolver{}).Resolve(context.Background(), host+"/team/image:dev", sha256Digest(manifest), []string{host})
	if !errors.Is(err, ErrDefinitive) || !errors.Is(err, errRegistryVerificationResponseTooLarge) {
		t.Fatalf("err=%v, want definitive bounded manifest failure", err)
	}
}

func testOversizedRegistry(t *testing.T, manifest, config []byte) (*httptest.Server, string) {
	t.Helper()
	manifestDigest := sha256Digest(manifest)
	configDigest := sha256Digest(config)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/":
			w.WriteHeader(http.StatusOK)
		case "/v2/team/image/manifests/" + manifestDigest:
			w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
			w.(http.Flusher).Flush() // Unknown Content-Length exercises the body cap.
			_, _ = w.Write(manifest)
		case "/v2/team/image/blobs/" + configDigest:
			w.Header().Set("Content-Type", "application/octet-stream")
			w.(http.Flusher).Flush()
			_, _ = w.Write(config)
		default:
			http.NotFound(w, r)
		}
	}))
	return server, strings.TrimPrefix(server.URL, "http://")
}

func sha256Digest(body []byte) string {
	return fmt.Sprintf("sha256:%x", sha256.Sum256(body))
}
