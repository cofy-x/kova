package source

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

const maxArchiveMetadataBytes = 1 << 20

type buildArchiveTopLevel struct {
	name     string
	children map[string]struct{}
	rootFile bool
}

func ValidateBuildArchive(zipPath string) (int, error) {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return 0, err
	}
	defer r.Close()

	topLevels := make(map[string]*buildArchiveTopLevel)
	seenPaths := make(map[string]struct{}, len(r.File))
	for _, file := range r.File {
		cleaned, err := ValidateBuildArchivePath(file.Name)
		if err != nil {
			return 0, err
		}
		if cleaned == "" {
			continue
		}
		if _, exists := seenPaths[cleaned]; exists {
			return 0, fmt.Errorf("zip contains duplicate path %q", cleaned)
		}
		seenPaths[cleaned] = struct{}{}

		parts := strings.Split(cleaned, "/")
		top := parts[0]
		entry, ok := topLevels[top]
		if !ok {
			entry = &buildArchiveTopLevel{
				name:     top,
				children: make(map[string]struct{}),
			}
			topLevels[top] = entry
		}

		if len(parts) == 1 {
			if !file.FileInfo().IsDir() {
				entry.rootFile = true
			}
			continue
		}

		entry.children[parts[1]] = struct{}{}
	}

	if len(topLevels) == 0 {
		return 0, fmt.Errorf("zip archive is empty or does not contain any image directories")
	}

	var validCount int
	var rootFiles []string
	var invalidDirs []string
	for _, name := range sortedBuildArchiveKeys(topLevels) {
		entry := topLevels[name]
		if entry.rootFile {
			rootFiles = append(rootFiles, entry.name)
			continue
		}
		_, hasDockerfile := entry.children["Dockerfile"]
		_, hasMetadata := entry.children["metadata.json"]
		if hasDockerfile && hasMetadata {
			validCount++
			continue
		}

		missing := make([]string, 0, 2)
		if !hasDockerfile {
			missing = append(missing, "Dockerfile")
		}
		if !hasMetadata {
			missing = append(missing, "metadata.json")
		}

		childPreview := sortedBuildArchiveChildren(entry.children)
		if len(childPreview) > 3 {
			childPreview = childPreview[:3]
		}
		invalidDirs = append(invalidDirs, fmt.Sprintf("%s (missing %s", entry.name, strings.Join(missing, " and ")))
		if len(childPreview) > 0 {
			invalidDirs[len(invalidDirs)-1] += fmt.Sprintf(", contains %s", strings.Join(childPreview, ", "))
		}
		invalidDirs[len(invalidDirs)-1] += ")"
	}

	if len(rootFiles) > 0 {
		return 0, fmt.Errorf("zip root must contain only image directories, found root file entries: %s", strings.Join(limitBuildArchiveList(rootFiles, 5), ", "))
	}
	if len(invalidDirs) > 0 {
		return 0, fmt.Errorf("zip root must directly contain image directories with Dockerfile and metadata.json; invalid top-level directories: %s", strings.Join(limitBuildArchiveList(invalidDirs, 5), "; "))
	}
	if validCount == 0 {
		return 0, fmt.Errorf("zip archive does not contain any valid image directories")
	}

	return validCount, nil
}

// ValidateSingleBuildArchiveTarget makes the service request target and the
// target executed by the runner one immutable contract.
func ValidateSingleBuildArchiveTarget(zipPath, target string) error {
	count, err := ValidateBuildArchive(zipPath)
	if err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("service builds with an explicit target require exactly one image directory, got %d", count)
	}
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return err
	}
	defer r.Close()
	for _, file := range r.File {
		cleaned, err := ValidateBuildArchivePath(file.Name)
		if err != nil {
			return err
		}
		parts := strings.Split(cleaned, "/")
		if len(parts) != 2 || parts[1] != "metadata.json" {
			continue
		}
		if file.UncompressedSize64 > maxArchiveMetadataBytes {
			return fmt.Errorf("metadata.json exceeds 1 MiB")
		}
		reader, err := file.Open()
		if err != nil {
			return err
		}
		raw, readErr := io.ReadAll(io.LimitReader(reader, maxArchiveMetadataBytes+1))
		closeErr := reader.Close()
		if readErr != nil {
			return readErr
		}
		if closeErr != nil {
			return closeErr
		}
		var metadata ImageMetadata
		if err := json.Unmarshal(raw, &metadata); err != nil {
			return fmt.Errorf("invalid metadata.json: %w", err)
		}
		if strings.TrimSpace(metadata.Target) != strings.TrimSpace(target) {
			return fmt.Errorf("archive target %q does not match requested target %q", metadata.Target, target)
		}
		return nil
	}
	return fmt.Errorf("archive is missing metadata.json")
}

func ValidateBuildArchivePath(name string) (string, error) {
	normalized := strings.ReplaceAll(strings.TrimSpace(name), "\\", "/")
	if normalized == "" {
		return "", nil
	}
	if strings.HasPrefix(normalized, "/") {
		return "", fmt.Errorf("zip contains absolute path %q", name)
	}
	cleaned := path.Clean(normalized)
	if cleaned == "." {
		return "", nil
	}
	if cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", fmt.Errorf("zip contains invalid path %q", name)
	}
	return cleaned, nil
}

func sortedBuildArchiveKeys(entries map[string]*buildArchiveTopLevel) []string {
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedBuildArchiveChildren(children map[string]struct{}) []string {
	result := make([]string, 0, len(children))
	for child := range children {
		result = append(result, child)
	}
	sort.Strings(result)
	return result
}

func limitBuildArchiveList(values []string, limit int) []string {
	if len(values) <= limit {
		return values
	}
	trimmed := append([]string{}, values[:limit]...)
	trimmed = append(trimmed, fmt.Sprintf("... and %d more", len(values)-limit))
	return trimmed
}

func ExtractZip(zipPath, dest string) error {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return err
	}
	defer r.Close()

	destPrefix := filepath.Clean(dest) + string(os.PathSeparator)
	for _, f := range r.File {
		cleaned, err := ValidateBuildArchivePath(f.Name)
		if err != nil {
			return err
		}
		if cleaned == "" {
			continue
		}
		target := filepath.Clean(filepath.Join(dest, filepath.FromSlash(cleaned)))
		if !strings.HasPrefix(target, destPrefix) {
			return fmt.Errorf("zip contains invalid path %q", f.Name)
		}
		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
			continue
		}
		if f.FileInfo().Mode()&os.ModeSymlink != 0 {
			rc, err := f.Open()
			if err != nil {
				return err
			}
			linkTarget, readErr := io.ReadAll(rc)
			rc.Close()
			if readErr != nil {
				return readErr
			}
			if err := validateArchiveSymlinkTarget(f.Name, string(linkTarget)); err != nil {
				return err
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			_ = os.Remove(target)
			if err := os.Symlink(string(linkTarget), target); err != nil {
				return err
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		out, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			return err
		}
		rc, err := f.Open()
		if err != nil {
			out.Close()
			return err
		}
		_, copyErr := io.Copy(out, rc)
		rc.Close()
		out.Close()
		if copyErr != nil {
			return copyErr
		}
	}
	return nil
}
