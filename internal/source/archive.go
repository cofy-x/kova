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

const (
	maxArchiveMetadataBytes      = 1 << 20
	maxArchiveSymlinkTargetBytes = 4 << 10
)

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
	targets, err := BuildArchiveTargets(zipPath)
	if err != nil {
		return err
	}
	if len(targets) != 1 {
		return fmt.Errorf("service builds with an explicit target require exactly one image directory, got %d", len(targets))
	}
	if targets[0] != strings.TrimSpace(target) {
		return fmt.Errorf("archive target %q does not match requested target %q", targets[0], target)
	}
	return nil
}

func BuildArchiveTargets(zipPath string) ([]string, error) {
	count, err := ValidateBuildArchive(zipPath)
	if err != nil {
		return nil, err
	}
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	targetsByDirectory := make(map[string]string, count)
	directoriesByTarget := make(map[string]string, count)
	for _, file := range r.File {
		cleaned, err := ValidateBuildArchivePath(file.Name)
		if err != nil {
			return nil, err
		}
		parts := strings.Split(cleaned, "/")
		if len(parts) != 2 || parts[1] != "metadata.json" {
			continue
		}
		if file.UncompressedSize64 > maxArchiveMetadataBytes {
			return nil, fmt.Errorf("%s exceeds 1 MiB", cleaned)
		}
		reader, err := file.Open()
		if err != nil {
			return nil, err
		}
		raw, readErr := io.ReadAll(io.LimitReader(reader, maxArchiveMetadataBytes+1))
		closeErr := reader.Close()
		if readErr != nil {
			return nil, readErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
		var metadata ImageMetadata
		if err := json.Unmarshal(raw, &metadata); err != nil {
			return nil, fmt.Errorf("invalid %s: %w", cleaned, err)
		}
		target := strings.TrimSpace(metadata.Target)
		if target == "" {
			return nil, fmt.Errorf("%s target is required", cleaned)
		}
		if previous, exists := directoriesByTarget[target]; exists {
			return nil, fmt.Errorf("image directories %q and %q use duplicate target %q", previous, parts[0], target)
		}
		targetsByDirectory[parts[0]] = target
		directoriesByTarget[target] = parts[0]
	}
	if len(targetsByDirectory) != count {
		return nil, fmt.Errorf("expected %d metadata targets, found %d", count, len(targetsByDirectory))
	}
	directories := make([]string, 0, len(targetsByDirectory))
	for directory := range targetsByDirectory {
		directories = append(directories, directory)
	}
	sort.Strings(directories)
	targets := make([]string, 0, len(directories))
	for _, directory := range directories {
		targets = append(targets, targetsByDirectory[directory])
	}
	return targets, nil
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

	if err := os.MkdirAll(dest, 0o755); err != nil {
		return err
	}
	root, err := os.OpenRoot(dest)
	if err != nil {
		return err
	}
	defer root.Close()

	for _, f := range r.File {
		cleaned, err := ValidateBuildArchivePath(f.Name)
		if err != nil {
			return err
		}
		if cleaned == "" {
			continue
		}
		target := filepath.FromSlash(cleaned)
		if f.FileInfo().IsDir() {
			if err := root.MkdirAll(target, 0o755); err != nil {
				return err
			}
			continue
		}
		if f.FileInfo().Mode()&os.ModeSymlink != 0 {
			if f.UncompressedSize64 > maxArchiveSymlinkTargetBytes {
				return fmt.Errorf("symlink %s target exceeds 4 KiB", f.Name)
			}
			rc, err := f.Open()
			if err != nil {
				return err
			}
			linkTarget, readErr := io.ReadAll(io.LimitReader(rc, maxArchiveSymlinkTargetBytes+1))
			rc.Close()
			if readErr != nil {
				return readErr
			}
			if len(linkTarget) > maxArchiveSymlinkTargetBytes {
				return fmt.Errorf("symlink %s target exceeds 4 KiB", f.Name)
			}
			safeTarget, err := resolveArchiveSymlinkTarget(cleaned, string(linkTarget))
			if err != nil {
				return err
			}
			if err := root.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			if err := root.Remove(target); err != nil && !os.IsNotExist(err) {
				return err
			}
			if err := root.Symlink(safeTarget, target); err != nil {
				return err
			}
			continue
		}
		if err := root.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		out, err := root.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
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
