package batch

import (
	"testing"

	"github.com/cofy-x/kova/internal/source"
	"github.com/cofy-x/kova/internal/store"
)

type stubBuildResultReader struct {
	entries map[string]store.Entry
	err     error
}

func (s stubBuildResultReader) Get(target string) (store.Entry, bool, error) {
	if s.err != nil {
		return store.Entry{}, false, s.err
	}
	entry, ok := s.entries[target]
	return entry, ok, nil
}

func TestGroupBuildSpecsOrdersNydusBeforeOCI(t *testing.T) {
	jobs := groupBuildSpecs([]source.Spec{
		{Target: "example.com/ns/repo:tag", Format: source.BuildFormatOCI},
		{Target: "example.com/ns/repo:tag_nydus_v3"},
	})
	if len(jobs) != 1 {
		t.Fatalf("expected 1 grouped job, got %d", len(jobs))
	}
	if jobs[0].key != "example.com/ns/repo:tag" {
		t.Fatalf("unexpected job key %q", jobs[0].key)
	}
	if len(jobs[0].specs) != 2 {
		t.Fatalf("expected 2 specs, got %d", len(jobs[0].specs))
	}
	if source.FormatIsOCI(jobs[0].specs[0].Format) {
		t.Fatalf("expected nydus spec first, got %#v", jobs[0].specs)
	}
	if !source.FormatIsOCI(jobs[0].specs[1].Format) {
		t.Fatalf("expected OCI spec second, got %#v", jobs[0].specs)
	}
}

func TestFilterBuildJobsResumesIncompleteBothFormatsTask(t *testing.T) {
	jobs := []buildJob{{
		key: "example.com/ns/repo:tag",
		specs: []source.Spec{
			{Target: "example.com/ns/repo:tag_nydus_v3"},
			{Target: "example.com/ns/repo:tag", Format: source.BuildFormatOCI},
		},
	}}

	filtered, existing, skippedSucceeded, skippedFailed, err := filterBuildJobs(jobs, stubBuildResultReader{entries: map[string]store.Entry{
		"example.com/ns/repo:tag_nydus_v3": {Target: "example.com/ns/repo:tag_nydus_v3", Success: true},
	}}, false)
	if err != nil {
		t.Fatalf("filter jobs: %v", err)
	}
	if skippedSucceeded != 0 || skippedFailed != 0 {
		t.Fatalf("unexpected skipped counts: success=%d failed=%d", skippedSucceeded, skippedFailed)
	}
	if len(existing) != 0 {
		t.Fatalf("expected no completed task outcomes, got %#v", existing)
	}
	if len(filtered) != 1 {
		t.Fatalf("expected 1 filtered job, got %d", len(filtered))
	}
	if len(filtered[0].specs) != 1 || !source.FormatIsOCI(filtered[0].specs[0].Format) {
		t.Fatalf("expected only pending OCI spec, got %#v", filtered[0].specs)
	}
}

func TestFilterBuildJobsTreatsBothFormatsAsSingleSuccess(t *testing.T) {
	jobs := []buildJob{{
		key: "example.com/ns/repo:tag",
		specs: []source.Spec{
			{Target: "example.com/ns/repo:tag_nydus_v3"},
			{Target: "example.com/ns/repo:tag", Format: source.BuildFormatOCI},
		},
	}}

	filtered, existing, skippedSucceeded, skippedFailed, err := filterBuildJobs(jobs, stubBuildResultReader{entries: map[string]store.Entry{
		"example.com/ns/repo:tag_nydus_v3": {Target: "example.com/ns/repo:tag_nydus_v3", Success: true},
		"example.com/ns/repo:tag":          {Target: "example.com/ns/repo:tag", Success: true},
	}}, false)
	if err != nil {
		t.Fatalf("filter jobs: %v", err)
	}
	if len(filtered) != 0 {
		t.Fatalf("expected no pending jobs, got %#v", filtered)
	}
	if skippedSucceeded != 1 || skippedFailed != 0 {
		t.Fatalf("unexpected skipped counts: success=%d failed=%d", skippedSucceeded, skippedFailed)
	}
	if existing["example.com/ns/repo:tag"] != store.OutcomeSucceeded {
		t.Fatalf("expected completed task state, got %#v", existing)
	}
}
