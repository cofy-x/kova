package sourcestore

import (
	"fmt"
	"io"
	"mime/multipart"
	"os"
	"path/filepath"
)

const DefaultRoot = "/var/lib/kova/sources"

func Path(buildID string) string {
	return filepath.Join("builds", buildID, "source.zip")
}

func Dir(buildID string) string {
	return filepath.Join("builds", buildID)
}

func SaveUpload(root string, buildID string, file *multipart.FileHeader) (string, error) {
	rel := Path(buildID)
	full := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		return "", err
	}
	src, err := file.Open()
	if err != nil {
		return "", err
	}
	defer src.Close()
	tmp, err := os.CreateTemp(filepath.Dir(full), "source.zip.tmp.*")
	if err != nil {
		return "", err
	}
	tmpPath := tmp.Name()
	if _, err := io.Copy(tmp, src); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return "", err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return "", err
	}
	if err := os.Rename(tmpPath, full); err != nil {
		_ = os.Remove(tmpPath)
		return "", err
	}
	return rel, nil
}

func Remove(root string, buildID string) error {
	if buildID == "" {
		return fmt.Errorf("build id is empty")
	}
	return os.RemoveAll(filepath.Join(root, Dir(buildID)))
}
