package store

import (
	"encoding/json"
	"errors"
	"testing"
)

type stubWriter struct {
	err error
}

func (s stubWriter) UpsertResult(entry Entry) error {
	return s.err
}

func TestPersistBuildResultAppliesCounters(t *testing.T) {
	counters := NewOutcomeCounters(3, map[string]OutcomeState{})
	entry := Entry{Target: "target-a", Success: true}

	succeeded, total, failed, err := PersistBuildResult(stubWriter{}, counters, entry)
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if succeeded != 1 || total != 3 || failed != 0 {
		t.Fatalf("unexpected counters: success=%d total=%d failed=%d", succeeded, total, failed)
	}
}

func TestPersistBuildResultReturnsErrorOnStoreFailure(t *testing.T) {
	counters := NewOutcomeCounters(1, map[string]OutcomeState{})
	entry := Entry{Target: "target-b", Success: true}

	_, _, _, err := PersistBuildResult(stubWriter{err: errors.New("boom")}, counters, entry)
	if err == nil {
		t.Fatal("expected store error")
	}
	if succeeded, total, failed := counters.Snapshot(); succeeded != 0 || total != 1 || failed != 0 {
		t.Fatalf("counters changed unexpectedly: success=%d total=%d failed=%d", succeeded, total, failed)
	}
}

func TestEntryUnmarshalJSONAcceptsNumericElapsed(t *testing.T) {
	var entry Entry
	if err := json.Unmarshal([]byte(`{"target":"localhost:5001/app:dev","success":true,"elapsed":1.25}`), &entry); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if entry.Elapsed != "1.3s" {
		t.Fatalf("elapsed = %q, want 1.3s", entry.Elapsed)
	}
}

func TestEntryUnmarshalJSONRejectsNegativeElapsed(t *testing.T) {
	var entry Entry
	if err := json.Unmarshal([]byte(`{"target":"localhost:5001/app:dev","success":true,"elapsed":-1}`), &entry); err == nil {
		t.Fatal("expected negative elapsed error")
	}
}
