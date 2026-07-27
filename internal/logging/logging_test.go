package logging

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
)

func TestInfofKeepsStderrFormat(t *testing.T) {
	var captured bytes.Buffer
	oldStderr := os.Stderr
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stderr = writer
	defer func() {
		os.Stderr = oldStderr
	}()

	Infof("hello %s", "world")
	writer.Close()
	_, _ = io.Copy(&captured, reader)

	got := captured.String()
	if !strings.Contains(got, "[INFO] hello world") {
		t.Fatalf("stderr log = %q, want INFO message", got)
	}
}
