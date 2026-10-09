package batch

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/cofy-x/kova/internal/buildobservation"
	"github.com/cofy-x/kova/internal/store"
)

func TestSelectExportEntriesUsesExactRequestedTargets(t *testing.T) {
	entries := []store.Entry{
		{Target: "registry.example.com/base:dev", Success: true},
		{Target: "registry.example.com/app:dev", Success: true},
		{Target: "registry.example.com/app:dev_nydus_v3", Success: true},
	}

	selected, err := selectExportEntries(entries, Options{
		OCI:           true,
		ExportTargets: []string{"registry.example.com/app:dev", "registry.example.com/base:dev"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 2 || selected[0].Target != "registry.example.com/app:dev" || selected[1].Target != "registry.example.com/base:dev" {
		t.Fatalf("unexpected selected entries: %#v", selected)
	}
}

func TestBoundedSummaryEntryDropsLogsAndTruncatesUTF8Reason(t *testing.T) {
	entry := store.Entry{Target: "registry.example/demo:dev", Success: false, Logs: strings.Repeat("secret log", 1000), Reason: strings.Repeat("a", 2047) + "界"}
	summary := boundedSummaryEntry(entry)
	if summary.Logs != "" || len(summary.Reason) > 2048 || !utf8.ValidString(summary.Reason) || summary.Target != entry.Target || entry.Logs == "" {
		t.Fatalf("summary=%#v", summary)
	}
}

func TestBoundedSummaryEntryCopiesOnlyFiniteObservation(t *testing.T) {
	entry := store.Entry{Target: "registry.example/app:dev", Success: true, BuildObservation: buildobservation.Unavailable("missing_stream")}
	summary := boundedSummaryEntry(entry)
	summary.BuildObservation.Reason = "invalid_observation"
	if entry.BuildObservation.Reason != "missing_stream" {
		t.Fatal("summary aliases retained observation")
	}
	entry.BuildObservation.Reason = strings.Repeat("arbitrary raw payload", 1000)
	summary = boundedSummaryEntry(entry)
	if summary.BuildObservation.Reason != "invalid_observation" || !summary.Success {
		t.Fatalf("summary=%#v", summary)
	}
}

func TestSelectExportEntriesRejectsMissingOrFilteredTarget(t *testing.T) {
	entries := []store.Entry{
		{Target: "registry.example.com/failed:dev", Success: false},
		{Target: "registry.example.com/app:dev_nydus_v3", Success: true},
	}
	for _, target := range []string{"registry.example.com/missing:dev", "registry.example.com/failed:dev", "registry.example.com/app:dev_nydus_v3"} {
		_, err := selectExportEntries(entries, Options{OCI: true, ExportTargets: []string{target}})
		if err == nil || !strings.Contains(err.Error(), target) {
			t.Fatalf("target %q: expected precise error, got %v", target, err)
		}
	}
}

func TestSelectExportEntriesIncludesFailureWhenRequested(t *testing.T) {
	entries := []store.Entry{{Target: "registry.example.com/failed:dev", Success: false}}
	selected, err := selectExportEntries(entries, Options{
		OCI:           true,
		WithFail:      true,
		ExportTargets: []string{"registry.example.com/failed:dev", "registry.example.com/failed:dev"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 1 || selected[0].Success {
		t.Fatalf("unexpected selected entries: %#v", selected)
	}
}
