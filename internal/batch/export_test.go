package batch

import (
	"strings"
	"testing"

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
