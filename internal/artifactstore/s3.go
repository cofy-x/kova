package artifactstore

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"path"
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
	client, err := minio.New(cfg.S3Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3SessionKey),
		Secure: cfg.S3Secure,
		Region: cfg.S3Region,
	})
	if err != nil {
		return nil, fmt.Errorf("create S3 client: %w", err)
	}
	return &S3{client: client, bucket: cfg.S3Bucket}, nil
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
