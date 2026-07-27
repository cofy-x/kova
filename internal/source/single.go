package source

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
)

func PrepareSingleImageDir(imageDir, target string, buildVars map[string]string) (string, func(), error) {
	if err := requireRegularFile(filepath.Join(imageDir, "Dockerfile")); err != nil {
		return "", nil, fmt.Errorf("%s: %w", imageDir, err)
	}
	if strings.TrimSpace(target) == "" {
		if err := requireRegularFile(filepath.Join(imageDir, "metadata.json")); err != nil {
			return "", nil, fmt.Errorf("%s: %w", imageDir, err)
		}
	}

	preparedRoot, err := os.MkdirTemp("", "kova-image-dir-")
	if err != nil {
		return "", nil, fmt.Errorf("create temp image-dir: %w", err)
	}
	cleanup := func() {
		_ = os.RemoveAll(preparedRoot)
	}

	name := filepath.Base(filepath.Clean(imageDir))
	if name == "." || name == string(filepath.Separator) {
		name = "image"
	}
	dstDir := filepath.Join(preparedRoot, name)
	if err := copyDirectory(imageDir, dstDir); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("copy %s: %w", imageDir, err)
	}

	if err := replaceBuildVariablesInFile(filepath.Join(dstDir, "Dockerfile"), buildVars); err != nil {
		cleanup()
		return "", nil, err
	}
	metaPath := filepath.Join(dstDir, "metadata.json")
	if strings.TrimSpace(target) != "" {
		if err := writeImageMetadataTarget(metaPath, strings.TrimSpace(target)); err != nil {
			cleanup()
			return "", nil, err
		}
	} else if err := replaceBuildVariablesInFile(metaPath, buildVars); err != nil {
		cleanup()
		return "", nil, err
	}

	return preparedRoot, cleanup, nil
}

func CreateSingleImageArchive(imageDir, target, zipPath string) error {
	if err := requireRegularFile(filepath.Join(imageDir, "Dockerfile")); err != nil {
		return fmt.Errorf("%s: %w", imageDir, err)
	}
	if strings.TrimSpace(target) == "" {
		if err := requireRegularFile(filepath.Join(imageDir, "metadata.json")); err != nil {
			return fmt.Errorf("%s: %w", imageDir, err)
		}
	}

	out, err := os.OpenFile(zipPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer out.Close()

	zw := zip.NewWriter(out)

	top := filepath.Base(filepath.Clean(imageDir))
	if top == "." || top == string(filepath.Separator) {
		top = "image"
	}
	sawMetadata := false
	err = filepath.WalkDir(imageDir, func(srcPath string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(imageDir, srcPath)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		if rel == "metadata.json" {
			sawMetadata = true
			if strings.TrimSpace(target) != "" {
				return nil
			}
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		name := path.Join(top, filepath.ToSlash(rel))
		if d.IsDir() {
			_, err := zw.CreateHeader(&zip.FileHeader{Name: name + "/", Method: zip.Deflate})
			return err
		}
		if d.Type()&os.ModeSymlink != 0 {
			linkTarget, err := os.Readlink(srcPath)
			if err != nil {
				return err
			}
			safeLinkTarget, err := resolveArchiveSymlinkTarget(name, linkTarget)
			if err != nil {
				return err
			}
			header, err := zip.FileInfoHeader(info)
			if err != nil {
				return err
			}
			header.Name = name
			header.Method = zip.Deflate
			header.SetMode(info.Mode())
			w, err := zw.CreateHeader(header)
			if err != nil {
				return err
			}
			_, err = w.Write([]byte(safeLinkTarget))
			return err
		}
		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return err
		}
		header.Name = name
		header.Method = zip.Deflate
		w, err := zw.CreateHeader(header)
		if err != nil {
			return err
		}
		in, err := os.Open(srcPath)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(w, in)
		closeErr := in.Close()
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	})
	if err != nil {
		zw.Close()
		return err
	}
	if strings.TrimSpace(target) != "" || !sawMetadata {
		raw, err := imageMetadataWithTarget(filepath.Join(imageDir, "metadata.json"), strings.TrimSpace(target))
		if err != nil {
			return err
		}
		w, err := zw.Create(path.Join(top, "metadata.json"))
		if err != nil {
			return err
		}
		if _, err := w.Write(raw); err != nil {
			zw.Close()
			return err
		}
	}
	return zw.Close()
}

func writeImageMetadataTarget(metaPath, target string) error {
	raw, err := imageMetadataWithTarget(metaPath, target)
	if err != nil {
		return err
	}
	mode := fs.FileMode(0o644)
	if info, err := os.Stat(metaPath); err == nil {
		mode = info.Mode()
	} else if !os.IsNotExist(err) {
		return err
	}
	return os.WriteFile(metaPath, raw, mode)
}

func imageMetadataWithTarget(metaPath, target string) ([]byte, error) {
	target = strings.TrimSpace(target)
	if target == "" {
		raw, err := os.ReadFile(metaPath)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", metaPath, err)
		}
		return raw, nil
	}

	values := map[string]any{}
	raw, err := os.ReadFile(metaPath)
	if err == nil && len(bytes.TrimSpace(raw)) > 0 {
		if err := json.Unmarshal(raw, &values); err != nil {
			return nil, fmt.Errorf("invalid %s: %w", metaPath, err)
		}
	} else if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("read %s: %w", metaPath, err)
	}
	values["target"] = target
	raw, err = json.MarshalIndent(values, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(raw, '\n'), nil
}

func resolveArchiveSymlinkTarget(name, target string) (string, error) {
	normalized := strings.ReplaceAll(strings.TrimSpace(target), "\\", "/")
	if normalized == "" {
		return "", fmt.Errorf("symlink %s has empty target", name)
	}
	if strings.HasPrefix(normalized, "/") {
		return "", fmt.Errorf("symlink %s points to absolute path %q", name, target)
	}

	cleanedName := path.Clean(strings.ReplaceAll(name, "\\", "/"))
	buildRoot := strings.Split(cleanedName, "/")[0]
	resolved := path.Clean(path.Join(path.Dir(cleanedName), normalized))
	if resolved != buildRoot && !strings.HasPrefix(resolved, buildRoot+"/") {
		return "", fmt.Errorf("symlink %s points outside the build context: %q", name, target)
	}
	return path.Clean(normalized), nil
}
