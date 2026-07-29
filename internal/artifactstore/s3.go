package artifactstore

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type S3 struct {
	client *minio.Client
	bucket string
}

func NewS3(cfg Config) (*S3, error) {
	if strings.TrimSpace(cfg.S3Endpoint) == "" || strings.TrimSpace(cfg.S3Bucket) == "" {
		return nil, fmt.Errorf("S3 endpoint and bucket are required")
	}
	creds, err := s3Credentials(cfg)
	if err != nil {
		return nil, err
	}
	client, err := minio.New(cfg.S3Endpoint, &minio.Options{
		Creds:  creds,
		Secure: cfg.S3Secure,
		Region: cfg.S3Region,
	})
	if err != nil {
		return nil, fmt.Errorf("create S3 client: %w", err)
	}
	return &S3{client: client, bucket: cfg.S3Bucket}, nil
}

func s3Credentials(cfg Config) (*credentials.Credentials, error) {
	provider := strings.TrimSpace(cfg.S3CredentialProvider)
	if provider == "" {
		provider = S3CredentialProviderStatic
	}
	switch provider {
	case S3CredentialProviderStatic:
		if strings.TrimSpace(cfg.S3AccessKey) == "" || strings.TrimSpace(cfg.S3SecretKey) == "" {
			return nil, fmt.Errorf("static S3 credentials require access and secret keys")
		}
		return credentials.NewStaticV4(cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3SessionKey), nil
	case S3CredentialProviderFile:
		dir := strings.TrimSpace(cfg.S3CredentialDir)
		if dir == "" {
			dir = DefaultS3CredentialDir
		}
		provider := fileCredentialProvider{dir: dir}
		if _, err := provider.Retrieve(); err != nil {
			return nil, err
		}
		return credentials.New(provider), nil
	case S3CredentialProviderAnonymous:
		return credentials.NewStaticV4("", "", ""), nil
	default:
		return nil, fmt.Errorf("unsupported S3 credential provider %q", provider)
	}
}

type fileCredentialProvider struct {
	dir string
}

func (p fileCredentialProvider) Retrieve() (credentials.Value, error) {
	accessKey, err := readCredentialFile(p.dir, "KOVA_S3_ACCESS_KEY", true)
	if err != nil {
		return credentials.Value{}, err
	}
	secretKey, err := readCredentialFile(p.dir, "KOVA_S3_SECRET_KEY", true)
	if err != nil {
		return credentials.Value{}, err
	}
	sessionToken, err := readCredentialFile(p.dir, "KOVA_S3_SESSION_TOKEN", false)
	if err != nil {
		return credentials.Value{}, err
	}
	return credentials.Value{
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
		SessionToken:    sessionToken,
		SignerType:      credentials.SignatureV4,
	}, nil
}

func (p fileCredentialProvider) RetrieveWithCredContext(*credentials.CredContext) (credentials.Value, error) {
	return p.Retrieve()
}

func (fileCredentialProvider) IsExpired() bool { return true }

func readCredentialFile(dir, name string, required bool) (string, error) {
	raw, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		if !required && os.IsNotExist(err) {
			return "", nil
		}
		return "", fmt.Errorf("read S3 credential %s: %w", name, err)
	}
	value := strings.TrimRight(string(raw), "\r\n")
	if required && value == "" {
		return "", fmt.Errorf("S3 credential %s is empty", name)
	}
	return value, nil
}

func (s *S3) Put(ctx context.Context, key string, src io.Reader, size int64, contentType string) (string, error) {
	key, err := cleanS3Key(key)
	if err != nil {
		return "", err
	}
	if _, err := s.client.PutObject(ctx, s.bucket, key, src, size, minio.PutObjectOptions{ContentType: contentType}); err != nil {
		return "", fmt.Errorf("put S3 artifact: %w", err)
	}
	return (&url.URL{Scheme: "s3", Host: s.bucket, Path: "/" + key}).String(), nil
}

func (s *S3) Open(ctx context.Context, rawURI string) (io.ReadCloser, error) {
	key, err := s.keyForURI(rawURI)
	if err != nil {
		return nil, err
	}
	object, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("get S3 artifact: %w", err)
	}
	if _, err := object.Stat(); err != nil {
		_ = object.Close()
		return nil, fmt.Errorf("stat S3 artifact: %w", err)
	}
	return object, nil
}

func (s *S3) Delete(ctx context.Context, rawURI string) error {
	key, err := s.keyForURI(rawURI)
	if err != nil {
		return err
	}
	return s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{})
}

func (s *S3) List(ctx context.Context, prefix string) ([]Artifact, error) {
	prefix, err := cleanS3Key(strings.TrimSuffix(prefix, "/"))
	if err != nil {
		return nil, err
	}
	artifacts := []Artifact{}
	for object := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: prefix + "/", Recursive: true}) {
		if object.Err != nil {
			return nil, fmt.Errorf("list S3 artifacts: %w", object.Err)
		}
		artifacts = append(artifacts, Artifact{
			Key: object.Key, URI: (&url.URL{Scheme: "s3", Host: s.bucket, Path: "/" + object.Key}).String(), Modified: object.LastModified,
		})
	}
	return artifacts, nil
}

func (s *S3) keyForURI(rawURI string) (string, error) {
	u, err := ParseURI(rawURI)
	if err != nil {
		return "", err
	}
	if u.Scheme != "s3" || u.Host != s.bucket {
		return "", fmt.Errorf("S3 artifact URI must use configured bucket %q", s.bucket)
	}
	return cleanS3Key(strings.TrimPrefix(u.Path, "/"))
}

func cleanS3Key(key string) (string, error) {
	clean := path.Clean(strings.TrimSpace(key))
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || strings.HasPrefix(clean, "/") {
		return "", fmt.Errorf("invalid S3 artifact key")
	}
	return clean, nil
}
