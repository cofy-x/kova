package source

import (
	"archive/zip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/cofy-x/kova/internal/buildcontract"
)

const (
	maxArchiveMetadataBytes      = 1 << 20
	maxArchiveSymlinkTargetBytes = 4 << 10
)

type archiveBudget struct {
	compressedBytes int64
	expandedBytes   uint64
	entries         int
}

var defaultArchiveBudget = archiveBudget{
	compressedBytes: MaxArchiveBytes,
	expandedBytes:   MaxExpandedBytes,
	entries:         MaxArchiveEntries,
}

type buildArchiveTopLevel struct {
	name     string
	children map[string]struct{}
	rootFile bool
}

func ValidateBuildArchive(zipPath string) (int, error) {
	count, err := validateBuildArchiveWithBudget(zipPath, defaultArchiveBudget)
	return count, classifyBuildArchiveError(err)
}

func invalidBuildArchive(err error) error {
	if errors.Is(err, ErrInvalidBuildArchive) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrInvalidBuildArchive, err)
}

func classifyBuildArchiveError(err error) error {
	if err == nil {
		return nil
	}
	for _, known := range []error{
		ErrInvalidBuildArchive, ErrArchiveTooLarge, ErrExpandedTooLarge,
		ErrTooManyEntries, ErrDockerfileTooLarge, ErrMetadataTooLarge,
		zip.ErrFormat, zip.ErrChecksum, zip.ErrAlgorithm,
	} {
		if errors.Is(err, known) {
			return invalidBuildArchive(err)
		}
	}
	return err
}

func validateBuildArchiveWithBudget(zipPath string, budget archiveBudget) (int, error) {
	if err := checkArchiveSize(zipPath, budget.compressedBytes); err != nil {
		return 0, err
	}
	if err := checkZipEntryCount(zipPath, budget.entries); err != nil {
		return 0, err
	}
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return 0, err
	}
	defer r.Close()
	if err := checkArchiveHeaders(r.File, budget); err != nil {
		return 0, err
	}

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
			return 0, invalidBuildArchive(fmt.Errorf("zip contains duplicate path %q", cleaned))
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
		return 0, invalidBuildArchive(errors.New("zip archive is empty or does not contain any image directories"))
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
		return 0, invalidBuildArchive(fmt.Errorf("zip root must contain only image directories, found root file entries: %s", strings.Join(limitBuildArchiveList(rootFiles, 5), ", ")))
	}
	if len(invalidDirs) > 0 {
		return 0, invalidBuildArchive(fmt.Errorf("zip root must directly contain image directories with Dockerfile and metadata.json; invalid top-level directories: %s", strings.Join(limitBuildArchiveList(invalidDirs, 5), "; ")))
	}
	if validCount == 0 {
		return 0, invalidBuildArchive(errors.New("zip archive does not contain any valid image directories"))
	}
	if err := validateArchiveContents(r.File, budget.expandedBytes); err != nil {
		return 0, err
	}

	return validCount, nil
}

func validateArchiveContents(files []*zip.File, maxExpandedBytes uint64) error {
	var expanded uint64
	for _, file := range files {
		if file.FileInfo().IsDir() {
			continue
		}
		cleaned, err := ValidateBuildArchivePath(file.Name)
		if err != nil {
			return err
		}
		remaining := maxExpandedBytes - expanded
		fileLimit, tooLarge := requiredBuildFileLimit(cleaned)
		if fileLimit > 0 && fileLimit < remaining {
			remaining = fileLimit
		}
		reader, err := file.Open()
		if err != nil {
			return fmt.Errorf("source archive member %q: %w", file.Name, err)
		}
		n, copyErr := copyExpanded(io.Discard, reader, remaining)
		closeErr := reader.Close()
		if copyErr != nil {
			if errors.Is(copyErr, ErrExpandedTooLarge) && fileLimit > 0 && remaining == fileLimit {
				return fmt.Errorf("source archive member %q: %w", file.Name, tooLarge)
			}
			return fmt.Errorf("source archive member %q: %w", file.Name, copyErr)
		}
		if closeErr != nil {
			return fmt.Errorf("source archive member %q: %w", file.Name, closeErr)
		}
		expanded += uint64(n)
	}
	return nil
}

func checkArchiveHeaders(files []*zip.File, budget archiveBudget) error {
	if len(files) > budget.entries {
		return ErrTooManyEntries
	}
	var expanded uint64
	cleanedPaths := make([]string, 0, len(files))
	symlinks := make(map[string]struct{})
	for _, file := range files {
		if file.UncompressedSize64 > budget.expandedBytes-expanded {
			return ErrExpandedTooLarge
		}
		cleaned, err := ValidateBuildArchivePath(file.Name)
		if err != nil {
			return err
		}
		cleanedPaths = append(cleanedPaths, cleaned)
		if file.FileInfo().Mode()&os.ModeSymlink != 0 {
			symlinks[cleaned] = struct{}{}
		}
		fileLimit, tooLarge := requiredBuildFileLimit(cleaned)
		if fileLimit > 0 && !file.FileInfo().Mode().IsRegular() {
			return invalidBuildArchive(fmt.Errorf("source archive member %q must be a regular file", file.Name))
		}
		if fileLimit > 0 && file.UncompressedSize64 > fileLimit {
			return fmt.Errorf("source archive member %q: %w", file.Name, tooLarge)
		}
		expanded += file.UncompressedSize64
	}
	for i, cleaned := range cleanedPaths {
		for parent := path.Dir(cleaned); parent != "."; parent = path.Dir(parent) {
			if _, ok := symlinks[parent]; ok {
				return invalidBuildArchive(fmt.Errorf("source archive member %q has symlink parent %q", files[i].Name, parent))
			}
		}
	}
	return nil
}

func requiredBuildFileLimit(cleaned string) (uint64, error) {
	parts := strings.Split(cleaned, "/")
	if len(parts) != 2 {
		return 0, nil
	}
	switch parts[1] {
	case "Dockerfile":
		return uint64(MaxDockerfileBytes), ErrDockerfileTooLarge
	case "metadata.json":
		return maxArchiveMetadataBytes, ErrMetadataTooLarge
	default:
		return 0, nil
	}
}

func BuildArchiveTargets(zipPath string) ([]buildcontract.TargetSpec, error) {
	count, err := ValidateBuildArchive(zipPath)
	if err != nil {
		return nil, err
	}
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return nil, classifyBuildArchiveError(err)
	}
	defer r.Close()
	targetsByDirectory := make(map[string]buildcontract.TargetSpec, count)
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
			return nil, invalidBuildArchive(fmt.Errorf("%s exceeds 1 MiB", cleaned))
		}
		reader, err := file.Open()
		if err != nil {
			return nil, classifyBuildArchiveError(err)
		}
		raw, readErr := io.ReadAll(io.LimitReader(reader, maxArchiveMetadataBytes+1))
		closeErr := reader.Close()
		if readErr != nil {
			return nil, classifyBuildArchiveError(readErr)
		}
		if closeErr != nil {
			return nil, classifyBuildArchiveError(closeErr)
		}
		var metadata ImageMetadata
		if err := json.Unmarshal(raw, &metadata); err != nil {
			return nil, invalidBuildArchive(fmt.Errorf("invalid %s: %w", cleaned, err))
		}
		target, err := buildcontract.NormalizeLogicalTarget(metadata.Target)
		if err != nil {
			return nil, invalidBuildArchive(fmt.Errorf("invalid target in %s: %w", cleaned, err))
		}
		platform, err := buildcontract.NormalizePlatform(metadata.Platform)
		if err != nil {
			return nil, invalidBuildArchive(fmt.Errorf("invalid platform in %s: %w", cleaned, err))
		}
		if previous, exists := directoriesByTarget[target]; exists {
			return nil, invalidBuildArchive(fmt.Errorf("image directories %q and %q use duplicate target %q", previous, parts[0], target))
		}
		targetsByDirectory[parts[0]] = buildcontract.TargetSpec{Target: target, Platform: platform}
		directoriesByTarget[target] = parts[0]
	}
	if len(targetsByDirectory) != count {
		return nil, invalidBuildArchive(fmt.Errorf("expected %d metadata targets, found %d", count, len(targetsByDirectory)))
	}
	directories := make([]string, 0, len(targetsByDirectory))
	for directory := range targetsByDirectory {
		directories = append(directories, directory)
	}
	sort.Strings(directories)
	targets := make([]buildcontract.TargetSpec, 0, len(directories))
	for _, directory := range directories {
		targets = append(targets, targetsByDirectory[directory])
	}
	normalized, err := buildcontract.NormalizeTargetSpecs(targets)
	return normalized, classifyTargetSpecError(err)
}

func classifyTargetSpecError(err error) error {
	if err == nil {
		return nil
	}
	return invalidBuildArchive(err)
}

func ValidateBuildArchivePath(name string) (string, error) {
	normalized := strings.ReplaceAll(strings.TrimSpace(name), "\\", "/")
	if normalized == "" {
		return "", nil
	}
	if strings.HasPrefix(normalized, "/") {
		return "", invalidBuildArchive(fmt.Errorf("zip contains absolute path %q", name))
	}
	cleaned := path.Clean(normalized)
	if cleaned == "." {
		return "", nil
	}
	if cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", invalidBuildArchive(fmt.Errorf("zip contains invalid path %q", name))
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
	return extractZipWithBudget(zipPath, dest, defaultArchiveBudget)
}

func extractZipWithBudget(zipPath, dest string, budget archiveBudget) error {
	if err := checkArchiveSize(zipPath, budget.compressedBytes); err != nil {
		return err
	}
	if err := checkZipEntryCount(zipPath, budget.entries); err != nil {
		return err
	}
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return err
	}
	defer r.Close()
	if err := checkArchiveHeaders(r.File, budget); err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	stage, err := os.MkdirTemp(filepath.Dir(dest), ".kova-extract-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stage)
	root, err := os.OpenRoot(stage)
	if err != nil {
		return err
	}
	defer root.Close()

	var expanded uint64
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
			readLimit := uint64(maxArchiveSymlinkTargetBytes + 1)
			if remaining := budget.expandedBytes - expanded; remaining+1 < readLimit {
				readLimit = remaining + 1
			}
			linkTarget, readErr := io.ReadAll(io.LimitReader(rc, int64(readLimit)))
			closeErr := rc.Close()
			if readErr != nil {
				return readErr
			}
			if uint64(len(linkTarget)) > budget.expandedBytes-expanded {
				return ErrExpandedTooLarge
			}
			if len(linkTarget) > maxArchiveSymlinkTargetBytes {
				return fmt.Errorf("symlink %s target exceeds 4 KiB", f.Name)
			}
			if closeErr != nil {
				return closeErr
			}
			expanded += uint64(len(linkTarget))
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
		remaining := budget.expandedBytes - expanded
		fileLimit, tooLarge := requiredBuildFileLimit(cleaned)
		if fileLimit > 0 && fileLimit < remaining {
			remaining = fileLimit
		}
		n, copyErr := copyExpanded(out, rc, remaining)
		closeReadErr := rc.Close()
		closeWriteErr := out.Close()
		if copyErr != nil {
			if errors.Is(copyErr, ErrExpandedTooLarge) && fileLimit > 0 && remaining == fileLimit {
				return fmt.Errorf("source archive member %q: %w", f.Name, tooLarge)
			}
			return copyErr
		}
		if closeReadErr != nil {
			return closeReadErr
		}
		if closeWriteErr != nil {
			return closeWriteErr
		}
		expanded += uint64(n)
	}
	return os.Rename(stage, dest)
}

func copyExpanded(dst io.Writer, src io.Reader, remaining uint64) (int64, error) {
	n, err := io.Copy(dst, io.LimitReader(src, int64(remaining)+1))
	if uint64(n) > remaining {
		return n, ErrExpandedTooLarge
	}
	return n, err
}
