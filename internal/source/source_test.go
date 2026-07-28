package source

import (
	"archive/zip"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestParseBuildVariablesRequiresPrefix(t *testing.T) {
	_, err := ParseBuildVariables([]string{"FOO=bar"})
	if err == nil || !strings.Contains(err.Error(), "KOVA_") {
		t.Fatalf("expected prefix validation error, got %v", err)
	}
}

func TestValidateBuildArchiveRejectsRootFile(t *testing.T) {
	zipPath := filepath.Join(t.TempDir(), "source.zip")
	file, err := os.Create(zipPath)
	if err != nil {
		t.Fatal(err)
	}
	zw := zip.NewWriter(file)
	w, err := zw.Create("Dockerfile")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("FROM scratch\n")); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = ValidateBuildArchive(zipPath)
	if err == nil || !strings.Contains(err.Error(), "root file") {
		t.Fatalf("expected root file validation error, got %v", err)
	}
}

func TestBuildArchiveTargetsReturnsSortedBatchTargets(t *testing.T) {
	zipPath := filepath.Join(t.TempDir(), "source.zip")
	file, err := os.Create(zipPath)
	if err != nil {
		t.Fatal(err)
	}
	zw := zip.NewWriter(file)
	for _, entry := range []struct{ name, target string }{{"b", "registry.example/b:dev"}, {"a", "registry.example/a:dev"}} {
		dockerfile, createErr := zw.Create(entry.name + "/Dockerfile")
		if createErr != nil {
			t.Fatal(createErr)
		}
		_, _ = dockerfile.Write([]byte("FROM scratch\n"))
		metadata, createErr := zw.Create(entry.name + "/metadata.json")
		if createErr != nil {
			t.Fatal(createErr)
		}
		_, _ = metadata.Write([]byte(`{"target":"` + entry.target + `"}`))
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	targets, err := BuildArchiveTargets(zipPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 2 || targets[0] != "registry.example/a:dev" || targets[1] != "registry.example/b:dev" {
		t.Fatalf("targets = %#v", targets)
	}
}

func TestParseBuildFormatsRejectsInvalidValue(t *testing.T) {
	if _, err := ParseBuildFormats("invalid"); err == nil {
		t.Fatal("expected invalid format to fail")
	}
}

func TestLoadBuildSpecsForFormatsExpandsBothFormatsFromImageDirs(t *testing.T) {
	root := t.TempDir()
	imageDir := filepath.Join(root, "image-a")
	if err := os.MkdirAll(imageDir, 0o755); err != nil {
		t.Fatalf("mkdir image dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(imageDir, "Dockerfile"), []byte("FROM scratch\n"), 0o644); err != nil {
		t.Fatalf("write Dockerfile: %v", err)
	}
	if err := os.WriteFile(filepath.Join(imageDir, "metadata.json"), []byte(`{"target":"example.com/ns/repo:tag"}`), 0o644); err != nil {
		t.Fatalf("write metadata.json: %v", err)
	}

	specs, cleanup, err := LoadBuildSpecsForFormats(root, "", []BuildFormat{BuildFormatNydus, BuildFormatOCI}, nil)
	if cleanup != nil {
		defer cleanup()
	}
	if err != nil {
		t.Fatalf("load specs: %v", err)
	}
	if len(specs) != 2 {
		t.Fatalf("expected 2 specs, got %d", len(specs))
	}

	got := make(map[string]bool, len(specs))
	for _, spec := range specs {
		if spec.Dir == "" {
			t.Fatal("expected prepared build directory")
		}
		got[spec.Target] = FormatIsOCI(spec.Format)
	}
	if oci, ok := got["example.com/ns/repo:tag"]; !ok || !oci {
		t.Fatalf("expected OCI target, got %#v", got)
	}
	if oci, ok := got["example.com/ns/repo:tag_nydus_v3"]; !ok || oci {
		t.Fatalf("expected nydus target, got %#v", got)
	}
}

func TestPrepareSingleImageDirUsesExplicitTargetWithoutMetadata(t *testing.T) {
	imageDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(imageDir, "Dockerfile"), []byte("FROM scratch\n"), 0o644); err != nil {
		t.Fatalf("write Dockerfile: %v", err)
	}

	root, cleanup, err := PrepareSingleImageDir(imageDir, "example.com/ns/app:dev", nil)
	if cleanup != nil {
		defer cleanup()
	}
	if err != nil {
		t.Fatalf("prepare single image dir: %v", err)
	}

	specs, cleanupSpecs, err := LoadBuildSpecsForFormats(root, "", []BuildFormat{BuildFormatOCI}, nil)
	if cleanupSpecs != nil {
		defer cleanupSpecs()
	}
	if err != nil {
		t.Fatalf("load specs: %v", err)
	}
	if len(specs) != 1 || specs[0].Target != "example.com/ns/app:dev" || !FormatIsOCI(specs[0].Format) {
		t.Fatalf("unexpected specs: %#v", specs)
	}
}

func TestCreateSingleImageArchiveOverridesMetadataTarget(t *testing.T) {
	imageDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(imageDir, "Dockerfile"), []byte("FROM scratch\n"), 0o644); err != nil {
		t.Fatalf("write Dockerfile: %v", err)
	}
	if err := os.WriteFile(filepath.Join(imageDir, "metadata.json"), []byte(`{"target":"example.com/ns/old:dev","keep":"yes"}`), 0o644); err != nil {
		t.Fatalf("write metadata: %v", err)
	}

	zipPath := filepath.Join(t.TempDir(), "source.zip")
	if err := CreateSingleImageArchive(imageDir, "example.com/ns/new:dev", zipPath); err != nil {
		t.Fatalf("create archive: %v", err)
	}
	if count, err := ValidateBuildArchive(zipPath); err != nil || count != 1 {
		t.Fatalf("validate archive count=%d err=%v", count, err)
	}

	extractDir := filepath.Join(t.TempDir(), "extract")
	if err := ExtractZip(zipPath, extractDir); err != nil {
		t.Fatalf("extract: %v", err)
	}
	entries, err := os.ReadDir(extractDir)
	if err != nil {
		t.Fatalf("read extract: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected one top-level dir, got %d", len(entries))
	}
	raw, err := os.ReadFile(filepath.Join(extractDir, entries[0].Name(), "metadata.json"))
	if err != nil {
		t.Fatalf("read metadata: %v", err)
	}
	content := string(raw)
	if !strings.Contains(content, `"target": "example.com/ns/new:dev"`) || !strings.Contains(content, `"keep": "yes"`) {
		t.Fatalf("unexpected metadata: %s", content)
	}
}

func TestCreateSingleImageArchivePreservesSafeSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink permissions vary on Windows")
	}
	imageDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(imageDir, "Dockerfile"), []byte("FROM scratch\n"), 0o644); err != nil {
		t.Fatalf("write Dockerfile: %v", err)
	}
	if err := os.WriteFile(filepath.Join(imageDir, "hello.txt"), []byte("hello\n"), 0o644); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	if err := os.Symlink("hello.txt", filepath.Join(imageDir, "hello-link")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	zipPath := filepath.Join(t.TempDir(), "source.zip")
	if err := CreateSingleImageArchive(imageDir, "example.com/ns/app:dev", zipPath); err != nil {
		t.Fatalf("create archive: %v", err)
	}
	extractDir := filepath.Join(t.TempDir(), "extract")
	if err := ExtractZip(zipPath, extractDir); err != nil {
		t.Fatalf("extract: %v", err)
	}
	entries, err := os.ReadDir(extractDir)
	if err != nil {
		t.Fatalf("read extract: %v", err)
	}
	linkPath := filepath.Join(extractDir, entries[0].Name(), "hello-link")
	target, err := os.Readlink(linkPath)
	if err != nil {
		t.Fatalf("readlink: %v", err)
	}
	if target != "hello.txt" {
		t.Fatalf("link target = %q", target)
	}
}

func TestCreateSingleImageArchiveRejectsUnsafeSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink permissions vary on Windows")
	}
	imageDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(imageDir, "Dockerfile"), []byte("FROM scratch\n"), 0o644); err != nil {
		t.Fatalf("write Dockerfile: %v", err)
	}
	if err := os.Symlink("../outside", filepath.Join(imageDir, "bad-link")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	zipPath := filepath.Join(t.TempDir(), "source.zip")
	err := CreateSingleImageArchive(imageDir, "example.com/ns/app:dev", zipPath)
	if err == nil || !strings.Contains(err.Error(), "outside the build context") {
		t.Fatalf("expected unsafe symlink error, got %v", err)
	}
}

func TestExtractZipRejectsSymlinkOutsideBuildContext(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink permissions vary on Windows")
	}
	zipPath := filepath.Join(t.TempDir(), "source.zip")
	file, err := os.Create(zipPath)
	if err != nil {
		t.Fatal(err)
	}
	zw := zip.NewWriter(file)
	header := &zip.FileHeader{Name: "image/bad-link"}
	header.SetMode(os.ModeSymlink | 0o777)
	w, err := zw.CreateHeader(header)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("../../outside")); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	err = ExtractZip(zipPath, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "outside the build context") {
		t.Fatalf("expected unsafe symlink error, got %v", err)
	}
}

func TestResolveArchiveSymlinkTargetAllowsParentWithinBuildContext(t *testing.T) {
	target, err := resolveArchiveSymlinkTarget("image/sub/link", "../hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	if target != "../hello.txt" {
		t.Fatalf("target = %q", target)
	}
}
