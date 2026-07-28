package artifactstore

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFilesystemRoundTrip(t *testing.T) {
	root := t.TempDir()
	store, err := NewFilesystem(root)
	if err != nil {
		t.Fatal(err)
	}
	uri, err := store.Put(context.Background(), "builds/demo/source.zip", strings.NewReader("payload"), 7, "application/zip")
	if err != nil {
		t.Fatal(err)
	}
	reader, err := store.Open(context.Background(), uri)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	if closeErr := reader.Close(); err == nil {
		err = closeErr
	}
	if err != nil || string(got) != "payload" {
		t.Fatalf("payload=%q err=%v", got, err)
	}
	if err := store.Delete(context.Background(), uri); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Open(context.Background(), uri); err == nil {
		t.Fatal("expected deleted artifact to be absent")
	}
	if _, err := os.Stat(filepath.Join(root, "builds", "demo")); !os.IsNotExist(err) {
		t.Fatalf("empty artifact directory was not removed: %v", err)
	}
}

func TestFilesystemRejectsEscapes(t *testing.T) {
	store, err := NewFilesystem(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(context.Background(), "../escape", strings.NewReader("payload"), 7, "application/octet-stream"); err == nil {
		t.Fatal("expected escaping key to fail")
	}
	if _, err := store.Open(context.Background(), "file:///tmp/outside"); err == nil {
		t.Fatal("expected outside URI to fail")
	}
}
