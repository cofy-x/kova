package sourcebundle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/cofy-x/kova/internal/source"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/types"
)

const LayerMediaType types.MediaType = "application/vnd.cofy.kova.source.v1+zip"

const (
	// These are the source-fetch init container's explicit termination codes.
	// Other nonzero exits are not evidence of invalid source content.
	FetchExitCodeInvalidSource     = 20
	FetchExitCodeResourceExhausted = 21
)

// ErrInvalidSource marks a source contract or content rejection. Fetch errors
// without this marker may instead be caused by the registry, network, or host.
var ErrInvalidSource = errors.New("invalid immutable source")

func invalidSource(err error) error {
	return fmt.Errorf("%w: %w", ErrInvalidSource, err)
}

type Reference struct {
	URI    string `json:"uri"`
	Digest string `json:"digest"`
}

func Validate(uri, digest string) error {
	if !validDigest(digest) {
		return fmt.Errorf("source digest must be a lowercase sha256 digest")
	}
	parsed, err := url.Parse(strings.TrimSpace(uri))
	if err != nil || parsed.Host == "" {
		return fmt.Errorf("source URI must be an absolute oci:// or https:// URI")
	}
	switch parsed.Scheme {
	case "https":
		if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
			return fmt.Errorf("HTTPS source URI must not contain credentials, a query, or a fragment")
		}
	case "oci":
		if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
			return fmt.Errorf("OCI source URI must not contain credentials, a query, or a fragment")
		}
		ref, err := name.NewDigest(strings.TrimPrefix(uri, "oci://"), name.WeakValidation)
		if err != nil || !strings.Contains(ref.Name(), "@sha256:") {
			return fmt.Errorf("OCI source URI must be pinned by manifest digest")
		}
	default:
		return fmt.Errorf("source URI scheme must be oci or https")
	}
	return nil
}

func Push(ctx context.Context, archivePath, destination string, plainHTTP []string) (Reference, error) {
	file, err := os.Open(filepath.Clean(archivePath))
	if err != nil {
		return Reference{}, err
	}
	defer file.Close()
	snapshot, err := os.CreateTemp("", ".kova-source-push-*.zip")
	if err != nil {
		return Reference{}, err
	}
	snapshotPath := snapshot.Name()
	defer os.Remove(snapshotPath)
	payloadHash := sha256.New()
	size, copyErr := source.CopyArchive(io.MultiWriter(snapshot, payloadHash), file, source.MaxArchiveBytes)
	closeErr := snapshot.Close()
	if copyErr != nil {
		return Reference{}, copyErr
	}
	if closeErr != nil {
		return Reference{}, closeErr
	}
	if _, err := source.ValidateBuildArchive(snapshotPath); err != nil {
		return Reference{}, err
	}
	digest := v1.Hash{Algorithm: "sha256", Hex: hex.EncodeToString(payloadHash.Sum(nil))}
	ref, err := name.ParseReference(strings.TrimPrefix(strings.TrimSpace(destination), "oci://"), referenceOptions(destination, plainHTTP)...)
	if err != nil {
		return Reference{}, fmt.Errorf("parse source destination: %w", err)
	}
	layer := sourceFileLayer{path: snapshotPath, digest: digest, size: size}
	image, err := mutate.AppendLayers(empty.Image, layer)
	if err != nil {
		return Reference{}, err
	}
	image = mutate.MediaType(image, types.OCIManifestSchema1)
	image = mutate.ConfigMediaType(image, types.OCIConfigJSON)
	image = mutate.Annotations(image, map[string]string{
		"org.opencontainers.image.title": "kova-source.zip",
		"dev.cofy.kova.source.digest":    digest.String(),
	}).(v1.Image)
	if err := remote.Write(ref, image, remote.WithContext(ctx), remote.WithAuthFromKeychain(authn.DefaultKeychain)); err != nil {
		return Reference{}, fmt.Errorf("push OCI source bundle: %w", err)
	}
	manifestDigest, err := image.Digest()
	if err != nil {
		return Reference{}, err
	}
	return Reference{
		URI:    "oci://" + ref.Context().Digest(manifestDigest.String()).Name(),
		Digest: digest.String(),
	}, nil
}

// sourceFileLayer serves the validated immutable snapshot without buffering the
// entire source archive in the client heap. This custom layer is an already
// compressed ZIP payload; OCI must not gzip it again.
type sourceFileLayer struct {
	path   string
	digest v1.Hash
	size   int64
}

func (l sourceFileLayer) Digest() (v1.Hash, error)             { return l.digest, nil }
func (l sourceFileLayer) DiffID() (v1.Hash, error)             { return l.digest, nil }
func (l sourceFileLayer) Compressed() (io.ReadCloser, error)   { return os.Open(l.path) }
func (l sourceFileLayer) Uncompressed() (io.ReadCloser, error) { return os.Open(l.path) }
func (l sourceFileLayer) Size() (int64, error)                 { return l.size, nil }
func (l sourceFileLayer) MediaType() (types.MediaType, error)  { return LayerMediaType, nil }

func Fetch(ctx context.Context, uri, digest, output string, plainHTTP []string) error {
	return fetchWithLimit(ctx, uri, digest, output, plainHTTP, source.MaxArchiveBytes, http.DefaultClient)
}

func fetchWithLimit(ctx context.Context, uri, digest, output string, plainHTTP []string, maxBytes int64, client *http.Client) error {
	if err := Validate(uri, digest); err != nil {
		return invalidSource(err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(output), ".kova-source-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	hash := sha256.New()
	writer := io.MultiWriter(tmp, hash)
	parsed, _ := url.Parse(uri)
	switch parsed.Scheme {
	case "https":
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, uri, nil)
		if err != nil {
			tmp.Close()
			return err
		}
		resp, err := client.Do(req)
		if err != nil {
			tmp.Close()
			return fmt.Errorf("fetch HTTPS source: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			tmp.Close()
			return fmt.Errorf("fetch HTTPS source: %s", resp.Status)
		}
		if resp.ContentLength > maxBytes {
			tmp.Close()
			return invalidSource(source.ErrArchiveTooLarge)
		}
		_, err = source.CopyArchive(writer, resp.Body, maxBytes)
		if err != nil {
			tmp.Close()
			if errors.Is(err, source.ErrArchiveTooLarge) {
				return invalidSource(err)
			}
			return err
		}
	case "oci":
		ref, err := name.NewDigest(strings.TrimPrefix(uri, "oci://"), referenceOptions(uri, plainHTTP)...)
		if err != nil {
			tmp.Close()
			return invalidSource(err)
		}
		image, err := remote.Image(ref, remote.WithContext(ctx), remote.WithAuthFromKeychain(authn.DefaultKeychain))
		if err != nil {
			tmp.Close()
			return fmt.Errorf("fetch OCI source manifest: %w", err)
		}
		layers, err := image.Layers()
		if err != nil {
			tmp.Close()
			return fmt.Errorf("inspect OCI source layers: %w", err)
		}
		if len(layers) != 1 {
			tmp.Close()
			return invalidSource(errors.New("OCI source bundle must contain exactly one layer"))
		}
		mediaType, err := layers[0].MediaType()
		if err != nil {
			tmp.Close()
			return fmt.Errorf("inspect OCI source layer media type: %w", err)
		}
		if mediaType != LayerMediaType {
			tmp.Close()
			return invalidSource(fmt.Errorf("OCI source layer has unsupported media type %q", mediaType))
		}
		reader, err := layers[0].Compressed()
		if err != nil {
			tmp.Close()
			return err
		}
		_, copyErr := source.CopyArchive(writer, reader, maxBytes)
		closeErr := reader.Close()
		if copyErr != nil {
			tmp.Close()
			if errors.Is(copyErr, source.ErrArchiveTooLarge) {
				return invalidSource(copyErr)
			}
			return copyErr
		}
		if closeErr != nil {
			tmp.Close()
			return closeErr
		}
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	actual := "sha256:" + hex.EncodeToString(hash.Sum(nil))
	if actual != digest {
		return invalidSource(fmt.Errorf("source digest mismatch: expected %s, got %s", digest, actual))
	}
	if err := os.Chmod(tmpPath, 0o600); err != nil {
		return err
	}
	return os.Rename(tmpPath, output)
}

func referenceOptions(raw string, plainHTTP []string) []name.Option {
	options := []name.Option{name.WeakValidation}
	trimmed := strings.TrimPrefix(strings.TrimSpace(raw), "oci://")
	host := trimmed
	if slash := strings.IndexByte(host, '/'); slash >= 0 {
		host = host[:slash]
	}
	for _, candidate := range plainHTTP {
		if strings.EqualFold(strings.TrimSpace(candidate), host) {
			return append(options, name.Insecure)
		}
	}
	nameOnly, _, _ := strings.Cut(host, ":")
	if nameOnly == "localhost" || nameOnly == "127.0.0.1" || nameOnly == "host.docker.internal" || nameOnly == "kind-registry" {
		return append(options, name.Insecure)
	}
	return options
}

func validDigest(value string) bool {
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+64 {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil && value == strings.ToLower(value)
}
