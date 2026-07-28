package artifactstore

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

type Filesystem struct {
	root string
}

func NewFilesystem(root string) (*Filesystem, error) {
	if strings.TrimSpace(root) == "" {
		root = DefaultRoot
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve artifact root: %w", err)
	}
	return &Filesystem{root: filepath.Clean(abs)}, nil
}

func (s *Filesystem) Put(_ context.Context, key string, src io.Reader, _ int64, _ string) (string, error) {
	path, err := s.pathForKey(key)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return "", err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".artifact-*")
	if err != nil {
		return "", err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if _, err := io.Copy(tmp, src); err != nil {
		_ = tmp.Close()
		return "", err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return "", err
	}
	return (&url.URL{Scheme: "file", Path: path}).String(), nil
}

func (s *Filesystem) Open(_ context.Context, rawURI string) (io.ReadCloser, error) {
	path, err := s.pathForURI(rawURI)
	if err != nil {
		return nil, err
	}
	return os.Open(path)
}

func (s *Filesystem) Delete(_ context.Context, rawURI string) error {
	path, err := s.pathForURI(rawURI)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	for parent := filepath.Dir(path); parent != s.root && withinRoot(s.root, parent); parent = filepath.Dir(parent) {
		if err := os.Remove(parent); err != nil {
			break
		}
	}
	return nil
}

func (s *Filesystem) pathForKey(key string) (string, error) {
	if key == "" || filepath.IsAbs(key) {
		return "", fmt.Errorf("artifact key must be a non-empty relative path")
	}
	path := filepath.Join(s.root, filepath.FromSlash(key))
	if !withinRoot(s.root, path) {
		return "", fmt.Errorf("artifact key escapes configured root")
	}
	return path, nil
}

func (s *Filesystem) pathForURI(rawURI string) (string, error) {
	u, err := ParseURI(rawURI)
	if err != nil {
		return "", err
	}
	if u.Scheme != "file" || u.Host != "" {
		return "", fmt.Errorf("filesystem store requires a local file URI")
	}
	path := filepath.Clean(u.Path)
	if !withinRoot(s.root, path) {
		return "", fmt.Errorf("artifact URI escapes configured root")
	}
	return path, nil
}

func withinRoot(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
