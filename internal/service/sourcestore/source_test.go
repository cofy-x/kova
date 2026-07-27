package sourcestore

import (
	"mime/multipart"
	"net/textproto"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSourcePathAndCleanup(t *testing.T) {
	root := t.TempDir()
	id := "abc"
	path := filepath.Join(root, Path(id))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("zip"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := Remove(root, id); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, Dir(id))); !os.IsNotExist(err) {
		t.Fatalf("source dir still exists or unexpected error: %v", err)
	}
}

func TestSaveUploadToSource(t *testing.T) {
	root := t.TempDir()
	var body strings.Builder
	writer := multipart.NewWriter(&body)
	part, err := writer.CreatePart(textproto.MIMEHeader{
		"Content-Disposition": []string{`form-data; name="file"; filename="source.zip"`},
		"Content-Type":        []string{"application/zip"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Write([]byte("zip")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	req, err := multipart.NewReader(strings.NewReader(body.String()), writer.Boundary()).ReadForm(1024)
	if err != nil {
		t.Fatal(err)
	}
	file := req.File["file"][0]
	rel, err := SaveUpload(root, "abc", file)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, rel))
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "zip" {
		t.Fatalf("source content = %q", raw)
	}
}
