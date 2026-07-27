package daemon

import (
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestBuildOptionsFromQuery(t *testing.T) {
	q := url.Values{
		"concurrency":  []string{"3"},
		"fail-fast":    []string{"true"},
		"format":       []string{"both"},
		"oom-cooldown": []string{"45s"},
		"timeout":      []string{"600"},
		"retry":        []string{"2"},
		"verbose":      []string{"yes"},
		"target":       []string{" localhost:5001/example:dev "},
		"skip-fail":    []string{"true"},
		"var":          []string{"KOVA_IMAGE_REGISTRY=localhost:5001", "KOVA_TAG=dev"},
	}

	opts, err := buildOptionsFromQuery(q, "127.0.0.1:9094", "/tmp/result.lmdb", "/tmp/logs.jsonl")
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}

	if opts.Concurrency != 3 || opts.Timeout != 600 || opts.Retry != 2 {
		t.Fatalf("unexpected numeric options: %#v", opts)
	}
	if !opts.Failfast || !opts.Verbose || !opts.SkipFail {
		t.Fatalf("unexpected bool options: %#v", opts)
	}
	if opts.BuildFormat != "both" {
		t.Fatalf("unexpected build format: %#v", opts)
	}
	if opts.OOMCooldown != 45*time.Second {
		t.Fatalf("unexpected cooldown: %s", opts.OOMCooldown)
	}
	if opts.Target != "localhost:5001/example:dev" {
		t.Fatalf("unexpected target %q", opts.Target)
	}
	if opts.ResultPath != "/tmp/result.lmdb" || opts.LogsPath != "/tmp/logs.jsonl" {
		t.Fatalf("unexpected store paths: %#v", opts)
	}
	if got := opts.Vars["KOVA_TAG"]; got != "dev" {
		t.Fatalf("unexpected build var: %q", got)
	}
	if len(opts.Addrs) != 1 || opts.Addrs[0].Addr != "tcp://127.0.0.1:9094" {
		t.Fatalf("unexpected addrs: %#v", opts.Addrs)
	}
}

func TestValidateQueryKeysRejectsUnsupportedAndDuplicateValues(t *testing.T) {
	if err := validateQueryKeys(url.Values{"unexpected": []string{"1"}}, "oci"); err == nil {
		t.Fatal("expected unsupported parameter error")
	}
	if err := validateQueryKeys(url.Values{"oci": []string{"true", "false"}}, "oci"); err == nil {
		t.Fatal("expected duplicate parameter error")
	}
	if err := validateQueryKeys(url.Values{"var": []string{"KOVA_A=a", "KOVA_B=b"}}, "var"); err != nil {
		t.Fatalf("expected repeated var to be allowed, got %v", err)
	}
}

func TestStrictQueryParsingRejectsInvalidValues(t *testing.T) {
	if _, err := queryBoolStrict(url.Values{"oci": []string{"maybe"}}, "oci", false); err == nil {
		t.Fatal("expected bool error")
	}
	if _, err := queryIntStrict(url.Values{"concurrency": []string{"0"}}, "concurrency", 1, 1); err == nil {
		t.Fatal("expected min integer error")
	}
	if _, err := queryDurationStrict(url.Values{"oom-cooldown": []string{"soon"}}, "oom-cooldown", time.Second, 0); err == nil {
		t.Fatal("expected duration error")
	}
}

func TestExportOptionsFromQueryAcceptsExactTargets(t *testing.T) {
	opts, err := exportOptionsFromQuery(url.Values{
		"oci":    []string{"true"},
		"target": []string{" registry.example.com/ns/app:dev ", "registry.example.com/ns/base:dev"},
	}, "/tmp/result.lmdb")
	if err != nil {
		t.Fatal(err)
	}
	if len(opts.ExportTargets) != 2 || opts.ExportTargets[0] != "registry.example.com/ns/app:dev" || opts.ExportTargets[1] != "registry.example.com/ns/base:dev" {
		t.Fatalf("unexpected export targets: %#v", opts.ExportTargets)
	}
}

func TestPreheatOptionsFromQueryRequiresScheduler(t *testing.T) {
	_, err := preheatOptionsFromQuery(url.Values{}, "/tmp/result.lmdb")
	if err == nil || !strings.Contains(err.Error(), "dragonfly-scheduler-addr") {
		t.Fatalf("expected scheduler error, got %v", err)
	}
}

func TestPreheatOptionsFromQueryConfiguresTLSVerification(t *testing.T) {
	opts, err := preheatOptionsFromQuery(url.Values{
		"dragonfly-scheduler-addr": []string{"dragonfly:8002"},
		"target":                   []string{"registry.example.com/ns/image:dev"},
	}, "/tmp/result.lmdb")
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if opts.PreheatInsecureSkipVerify {
		t.Fatal("expected insecure skip verify to be disabled")
	}
	if opts.Target != "registry.example.com/ns/image:dev" {
		t.Fatalf("unexpected target %q", opts.Target)
	}
}

func TestPreheatOptionsFromQueryAllowsExplicitInsecureRegistry(t *testing.T) {
	opts, err := preheatOptionsFromQuery(url.Values{
		"dragonfly-scheduler-addr": []string{"dragonfly:8002"},
		"insecure-skip-verify":     []string{"true"},
	}, "/tmp/result.lmdb")
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if !opts.PreheatInsecureSkipVerify {
		t.Fatal("expected insecure registry opt-in to be enabled")
	}
}
