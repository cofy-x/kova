package store

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func withFailureLogLimit(t *testing.T, limit int64) {
	t.Helper()
	previous := maxFailureLogFileSize
	maxFailureLogFileSize = limit
	t.Cleanup(func() {
		maxFailureLogFileSize = previous
	})
}

func TestFailureLogStoreAppendLoadAndFilter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "logs.jsonl")
	logs := newFailureLogStore(path)

	if err := logs.AppendFailure("localhost:5001/a:dev", "first failure"); err != nil {
		t.Fatalf("append first: %v", err)
	}
	if err := logs.AppendFailure("localhost:5001/b:dev", "second failure"); err != nil {
		t.Fatalf("append second: %v", err)
	}

	all, err := loadLatestFailureLogs(path, nil)
	if err != nil {
		t.Fatalf("load all: %v", err)
	}
	if got := all["localhost:5001/a:dev"]; got != "first failure" {
		t.Fatalf("unexpected first logs %q", got)
	}
	if got := all["localhost:5001/b:dev"]; got != "second failure" {
		t.Fatalf("unexpected second logs %q", got)
	}

	filtered, err := loadLatestFailureLogs(path, map[string]struct{}{"localhost:5001/b:dev": {}})
	if err != nil {
		t.Fatalf("load filtered: %v", err)
	}
	if len(filtered) != 1 || filtered["localhost:5001/b:dev"] != "second failure" {
		t.Fatalf("unexpected filtered logs: %#v", filtered)
	}
}

func TestFailureLogStoreTrimsOldLines(t *testing.T) {
	withFailureLogLimit(t, 170)
	path := filepath.Join(t.TempDir(), "logs.jsonl")
	logs := newFailureLogStore(path)

	if err := logs.AppendFailure("localhost:5001/old:dev", strings.Repeat("old", 20)); err != nil {
		t.Fatalf("append old: %v", err)
	}
	if err := logs.AppendFailure("localhost:5001/new:dev", strings.Repeat("new", 20)); err != nil {
		t.Fatalf("append new: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read logs: %v", err)
	}
	if int64(len(data)) > maxFailureLogFileSize {
		t.Fatalf("logs file size %d exceeds limit %d", len(data), maxFailureLogFileSize)
	}
	if strings.Contains(string(data), "old:dev") {
		t.Fatalf("expected old line to be trimmed, got %s", data)
	}
	if !strings.Contains(string(data), "new:dev") {
		t.Fatalf("expected new line to remain, got %s", data)
	}
}

func TestMarshalFailureLogLineTruncatesOversizedLogs(t *testing.T) {
	withFailureLogLimit(t, 140)
	line, err := marshalFailureLogLine(failureLogEntry{
		Target:  "localhost:5001/large:dev",
		Success: false,
		Logs:    strings.Repeat("x", 500),
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if int64(len(line)) > maxFailureLogFileSize {
		t.Fatalf("line size %d exceeds limit %d", len(line), maxFailureLogFileSize)
	}
	var entry failureLogEntry
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(line))), &entry); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !strings.Contains(entry.Logs, truncatedFailureLogSuffix) {
		t.Fatalf("expected truncation suffix in %q", entry.Logs)
	}
}

func TestLoadLatestFailureLogsRejectsBadJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "logs.jsonl")
	if err := os.WriteFile(path, []byte("{bad-json}\n"), 0o644); err != nil {
		t.Fatalf("write logs: %v", err)
	}
	if _, err := loadLatestFailureLogs(path, nil); err == nil {
		t.Fatal("expected bad JSON error")
	}
}
