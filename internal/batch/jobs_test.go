package batch

import (
	"testing"

	"github.com/cofy-x/kova/internal/source"
)

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
