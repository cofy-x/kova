package source

import (
	"archive/zip"
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func writeBudgetTestArchive(t *testing.T, entries map[string][]byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "source.zip")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := zip.NewWriter(file)
	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		data := entries[name]
		entry, err := writer.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := entry.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestCopyArchiveCountsActualBytes(t *testing.T) {
	var output bytes.Buffer
	if n, err := CopyArchive(&output, bytes.NewReader([]byte("12345678")), 8); err != nil || n != 8 {
		t.Fatalf("exact-limit copy = %d, %v", n, err)
	}
	output.Reset()
	if n, err := CopyArchive(&output, bytes.NewReader([]byte("123456789")), 8); !errors.Is(err, ErrArchiveTooLarge) || n != 9 {
		t.Fatalf("oversized copy = %d, %v", n, err)
	}
}

func TestArchiveBudgetsRejectCompressedBytesAndEntries(t *testing.T) {
	archive := writeBudgetTestArchive(t, map[string][]byte{
		"image/Dockerfile":    []byte("FROM scratch\n"),
		"image/metadata.json": []byte(`{"target":"registry.example.com/team/app:dev","platform":"linux/amd64"}`),
		"image/payload":       []byte("content"),
	})
	for _, tc := range []struct {
		name   string
		budget archiveBudget
		want   error
	}{
		{"compressed", archiveBudget{compressedBytes: 1, expandedBytes: MaxExpandedBytes, entries: MaxArchiveEntries}, ErrArchiveTooLarge},
		{"entries", archiveBudget{compressedBytes: MaxArchiveBytes, expandedBytes: MaxExpandedBytes, entries: 2}, ErrTooManyEntries},
		{"expanded", archiveBudget{compressedBytes: MaxArchiveBytes, expandedBytes: 50, entries: MaxArchiveEntries}, ErrExpandedTooLarge},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := validateBuildArchiveWithBudget(archive, tc.budget); !errors.Is(err, tc.want) {
				t.Fatalf("validation error = %v, want %v", err, tc.want)
			}
			dest := filepath.Join(t.TempDir(), "extracted")
			if err := extractZipWithBudget(archive, dest, tc.budget); !errors.Is(err, tc.want) {
				t.Fatalf("extraction error = %v, want %v", err, tc.want)
			}
			if _, err := os.Stat(dest); !os.IsNotExist(err) {
				t.Fatalf("rejected archive left destination: %v", err)
			}
		})
	}
}

func TestArchiveEntryPreflightCountsPastForgedEndRecord(t *testing.T) {
	archive := writeBudgetTestArchive(t, map[string][]byte{
		"image/Dockerfile":    []byte("FROM scratch\n"),
		"image/metadata.json": []byte(`{"target":"registry.example.com/team/app:dev","platform":"linux/amd64"}`),
		"image/payload":       []byte("content"),
	})
	raw, err := os.ReadFile(archive)
	if err != nil {
		t.Fatal(err)
	}
	end := bytes.LastIndex(raw, []byte("PK\x05\x06"))
	if end < 0 {
		t.Fatal("ZIP end record not found")
	}
	binary.LittleEndian.PutUint16(raw[end+10:end+12], 1)
	if err := os.WriteFile(archive, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := checkZipEntryCount(archive, 2); !errors.Is(err, ErrTooManyEntries) {
		t.Fatalf("entry preflight = %v, want entry-count limit", err)
	}
}

func TestArchiveEntryPreflightReadsZip64DirectoryCount(t *testing.T) {
	archive := writeBudgetTestArchive(t, map[string][]byte{"image/Dockerfile": []byte("FROM scratch\n")})
	raw, err := os.ReadFile(archive)
	if err != nil {
		t.Fatal(err)
	}
	end := bytes.LastIndex(raw, []byte("PK\x05\x06"))
	if end < 0 {
		t.Fatal("ZIP end record not found")
	}
	directorySize := binary.LittleEndian.Uint32(raw[end+12 : end+16])
	directoryOffset := binary.LittleEndian.Uint32(raw[end+16 : end+20])
	zip64End := make([]byte, 56)
	binary.LittleEndian.PutUint32(zip64End[0:4], zip64DirectoryEndSignature)
	binary.LittleEndian.PutUint64(zip64End[4:12], 44)
	binary.LittleEndian.PutUint16(zip64End[14:16], 45)
	binary.LittleEndian.PutUint64(zip64End[24:32], 1)
	binary.LittleEndian.PutUint64(zip64End[32:40], 1)
	binary.LittleEndian.PutUint64(zip64End[40:48], uint64(directorySize))
	binary.LittleEndian.PutUint64(zip64End[48:56], uint64(directoryOffset))
	locator := make([]byte, 20)
	binary.LittleEndian.PutUint32(locator[0:4], zip64LocatorSignature)
	binary.LittleEndian.PutUint64(locator[8:16], uint64(end))
	binary.LittleEndian.PutUint32(locator[16:20], 1)
	regularEnd := append([]byte(nil), raw[end:]...)
	binary.LittleEndian.PutUint16(regularEnd[8:10], 0xffff)
	binary.LittleEndian.PutUint16(regularEnd[10:12], 0xffff)
	binary.LittleEndian.PutUint32(regularEnd[12:16], 0xffffffff)
	binary.LittleEndian.PutUint32(regularEnd[16:20], 0xffffffff)
	modified := append(append(append([]byte(nil), raw[:end]...), zip64End...), locator...)
	modified = append(modified, regularEnd...)
	if err := os.WriteFile(archive, modified, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := checkZipEntryCount(archive, 1); err != nil {
		t.Fatalf("valid ZIP64 count was rejected: %v", err)
	}
	if err := checkZipEntryCount(archive, 0); !errors.Is(err, ErrTooManyEntries) {
		t.Fatalf("ZIP64 entry limit = %v, want entry-count limit", err)
	}
}

func TestExtractZipCountsActualExpansionAndCleansStage(t *testing.T) {
	var output bytes.Buffer
	if n, err := copyExpanded(&output, bytes.NewReader(bytes.Repeat([]byte("a"), 256)), 128); !errors.Is(err, ErrExpandedTooLarge) || n != 129 {
		t.Fatalf("streaming copy = %d, %v; want expanded-byte limit after 129 bytes", n, err)
	}
	archive := writeBudgetTestArchive(t, map[string][]byte{
		"image/Dockerfile":    []byte("FROM scratch\n"),
		"image/metadata.json": []byte(`{"target":"registry.example.com/team/app:dev","platform":"linux/amd64"}`),
		"image/payload":       bytes.Repeat([]byte("a"), 256),
	})
	// Understate the central-directory size. The header-only preflight passes,
	// so this exercises the budget while actual inflated bytes are copied.
	raw, err := os.ReadFile(archive)
	if err != nil {
		t.Fatal(err)
	}
	patched := false
	for offset := 0; offset+46 <= len(raw); offset++ {
		if !bytes.Equal(raw[offset:offset+4], []byte("PK\x01\x02")) {
			continue
		}
		nameSize := int(binary.LittleEndian.Uint16(raw[offset+28 : offset+30]))
		if offset+46+nameSize > len(raw) || string(raw[offset+46:offset+46+nameSize]) != "image/payload" {
			continue
		}
		binary.LittleEndian.PutUint32(raw[offset+24:offset+28], 1)
		patched = true
		break
	}
	if !patched {
		t.Fatal("payload central-directory entry not found")
	}
	if err := os.WriteFile(archive, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	budget := archiveBudget{compressedBytes: MaxArchiveBytes, expandedBytes: 128, entries: MaxArchiveEntries}
	r, err := zip.OpenReader(archive)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkArchiveHeaders(r.File, budget); err != nil {
		t.Fatalf("header preflight unexpectedly rejected archive: %v", err)
	}
	r.Close()
	if _, err := validateBuildArchiveWithBudget(archive, budget); err == nil {
		t.Fatal("expected malformed ZIP member to be rejected during streaming validation")
	}
	parent := t.TempDir()
	dest := filepath.Join(parent, "extracted")
	if err := extractZipWithBudget(archive, dest, budget); err == nil {
		t.Fatal("expected forged size to be rejected during extraction")
	}
	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("rejected extraction left files: %v", entries)
	}
}

func TestPublishedSourceExamplesFitBudgets(t *testing.T) {
	for _, name := range []string{"service-oci", "service-nydus"} {
		t.Run(name, func(t *testing.T) {
			archive := filepath.Join(t.TempDir(), "source.zip")
			contextDir := filepath.Join("..", "..", "examples", name)
			if err := CreateSingleImageArchive(contextDir, "registry.example.com/team/"+name+":dev", "linux/amd64", archive); err != nil {
				t.Fatal(err)
			}
			if count, err := ValidateBuildArchive(archive); err != nil || count != 1 {
				t.Fatalf("validate count=%d err=%v", count, err)
			}
			dest := filepath.Join(t.TempDir(), "extracted")
			if err := ExtractZip(archive, dest); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(filepath.Join(dest, name, "Dockerfile")); err != nil {
				t.Fatal(err)
			}
		})
	}
}
