package source

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

func prepareBuildImageDirs(imageDirs string, buildVars map[string]string) (string, error) {
	entries, err := os.ReadDir(imageDirs)
	if err != nil {
		return "", fmt.Errorf("read image-dirs: %w", err)
	}
	if len(entries) == 0 {
		return "", fmt.Errorf("image-dirs %s does not contain any image directories", imageDirs)
	}

	preparedRoot, err := os.MkdirTemp("", "kova-image-dirs-")
	if err != nil {
		return "", fmt.Errorf("create temp image-dirs: %w", err)
	}

	cleanup := func() {
		_ = os.RemoveAll(preparedRoot)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			cleanup()
			return "", fmt.Errorf("image-dirs %s must contain only directories, found %s", imageDirs, entry.Name())
		}

		srcDir := filepath.Join(imageDirs, entry.Name())
		if err := validateSourceImageDir(srcDir); err != nil {
			cleanup()
			return "", err
		}

		dstDir := filepath.Join(preparedRoot, entry.Name())
		if err := copyDirectory(srcDir, dstDir); err != nil {
			cleanup()
			return "", fmt.Errorf("copy %s: %w", srcDir, err)
		}

		if err := replaceBuildVariablesInFile(filepath.Join(dstDir, "Dockerfile"), buildVars); err != nil {
			cleanup()
			return "", err
		}
		if err := replaceBuildVariablesInFile(filepath.Join(dstDir, "metadata.json"), buildVars); err != nil {
			cleanup()
			return "", err
		}
	}

	return preparedRoot, nil
}

func validateSourceImageDir(dir string) error {
	if err := requireRegularFile(filepath.Join(dir, "Dockerfile")); err != nil {
		return fmt.Errorf("%s: %w", dir, err)
	}
	if err := requireRegularFile(filepath.Join(dir, "metadata.json")); err != nil {
		return fmt.Errorf("%s: %w", dir, err)
	}
	return nil
}

func requireRegularFile(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("missing required file %s", filepath.Base(path))
		}
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("required file %s must be a regular file", filepath.Base(path))
	}
	return nil
}

func copyDirectory(src, dst string) error {
	return filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		targetPath := filepath.Join(dst, relPath)

		info, err := d.Info()
		if err != nil {
			return err
		}

		if d.IsDir() {
			return os.MkdirAll(targetPath, info.Mode())
		}
		if d.Type()&os.ModeSymlink != 0 {
			linkTarget, err := os.Readlink(path)
			if err != nil {
				return err
			}
			return os.Symlink(linkTarget, targetPath)
		}
		return copyFile(path, targetPath, info.Mode())
	})
}

func copyFile(src, dst string, mode fs.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}

	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}

	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}
