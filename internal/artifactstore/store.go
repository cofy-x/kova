package artifactstore

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"
)

const (
	DriverFilesystem = "filesystem"
	DriverS3         = "s3"
	DefaultRoot      = "/var/lib/kova/artifacts"
)

type Store interface {
	Put(context.Context, string, io.Reader, int64, string) (string, error)
	Open(context.Context, string) (io.ReadCloser, error)
	Delete(context.Context, string) error
	List(context.Context, string) ([]Artifact, error)
}

type Artifact struct {
	Key      string
	URI      string
	Modified time.Time
}

type Config struct {
	Driver       string
	Root         string
	S3Endpoint   string
	S3Bucket     string
	S3Region     string
	S3AccessKey  string
	S3SecretKey  string
	S3SessionKey string
	S3Secure     bool
}

func New(cfg Config) (Store, error) {
	switch strings.TrimSpace(cfg.Driver) {
	case "", DriverFilesystem:
		return NewFilesystem(cfg.Root)
	case DriverS3:
		return NewS3(cfg)
	default:
		return nil, fmt.Errorf("unsupported artifact store driver %q", cfg.Driver)
	}
}

func ParseURI(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("parse artifact URI: %w", err)
	}
	if u.Scheme != "file" && u.Scheme != "s3" {
		return nil, fmt.Errorf("unsupported artifact URI scheme %q", u.Scheme)
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("artifact URI must not contain a query or fragment")
	}
	return u, nil
}
