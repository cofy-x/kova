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

func TestEntryPreservesPushedManifestDigestInJSON(t *testing.T) {
	want := Entry{Target: "registry.example/app:dev", Success: true, ManifestDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	data, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	var got Entry
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.ManifestDigest != want.ManifestDigest {
		t.Fatalf("manifest digest = %q, want %q", got.ManifestDigest, want.ManifestDigest)
	}
}

func TestEntryOptionalObservationCannotInvalidateSuccessOrDigest(t *testing.T) {
	for _, observation := range []string{`true`, `{"availability":"secret"}`, `{"schemaVersion":1,"availability":"observed","vertexCount":999999}`} {
		var entry Entry
		if err := json.Unmarshal([]byte(`{"success":true,"manifest_digest":"sha256:exact","build_observation":`+observation+`}`), &entry); err != nil {
			t.Fatal(err)
		}
		if !entry.Success || entry.ManifestDigest != "sha256:exact" || entry.BuildObservation.Availability != "unavailable" || entry.BuildObservation.Reason != "invalid_observation" {
			t.Fatalf("entry=%#v", entry)
		}
	}
}
