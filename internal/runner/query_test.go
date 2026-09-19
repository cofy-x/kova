package runner

import (
	"net/url"
	"testing"
)

func TestBuildQueryEncodesFlagsVarsAndTarget(t *testing.T) {
	raw, err := BuildQuery([]string{
		"--format", "oci",
		"--fail-fast",
		"--verbose",
		"--concurrency", "2",
		"--timeout=600",
		"--oom-cooldown=45s",
		"--platform=linux/amd64",
		"--var", "KOVA_IMAGE_REGISTRY=host.docker.internal:5001",
		"--var=NAME=value with spaces",
		"localhost:5001/example/simple:dev",
	})
	if err != nil {
		t.Fatal(err)
	}
	values, err := url.ParseQuery(raw)
	if err != nil {
		t.Fatal(err)
	}

	assertQueryValue(t, values, "format", "oci")
	assertQueryValue(t, values, "fail-fast", "true")
	assertQueryValue(t, values, "verbose", "true")
	assertQueryValue(t, values, "concurrency", "2")
	assertQueryValue(t, values, "timeout", "600")
	assertQueryValue(t, values, "oom-cooldown", "45s")
	assertQueryValue(t, values, "target", "localhost:5001/example/simple:dev")
	assertQueryValue(t, values, "platform", "linux/amd64")
	if got := values["var"]; len(got) != 2 || got[0] != "KOVA_IMAGE_REGISTRY=host.docker.internal:5001" || got[1] != "NAME=value with spaces" {
		t.Fatalf("unexpected var values: %#v", got)
	}
}

func TestBuildQueryEncodesBothFormat(t *testing.T) {
	raw, err := BuildQuery([]string{"--format=both"})
	if err != nil {
		t.Fatal(err)
	}
	values, err := url.ParseQuery(raw)
	if err != nil {
		t.Fatal(err)
	}
	assertQueryValue(t, values, "format", "both")
}

func TestBuildQueryEncodesExplicitTarget(t *testing.T) {
	raw, err := BuildQuery([]string{"--target", "localhost:5002/example/simple:dev", "--platform", "linux/arm64"})
	if err != nil {
		t.Fatal(err)
	}
	values, err := url.ParseQuery(raw)
	if err != nil {
		t.Fatal(err)
	}
	assertQueryValue(t, values, "target", "localhost:5002/example/simple:dev")
	assertQueryValue(t, values, "platform", "linux/arm64")
}

func TestBuildQueryRequiresCanonicalPlatformWithTarget(t *testing.T) {
	for _, args := range [][]string{
		{"--target", "localhost:5002/example/simple:dev"},
		{"--target", "localhost:5002/example/simple:dev", "--platform", "linux/s390x"},
		{"--platform", "linux/amd64"},
	} {
		if _, err := BuildQuery(args); err == nil {
			t.Fatalf("expected %#v to fail", args)
		}
	}
}

func TestBuildQueryRejectsExplicitAndPositionalTarget(t *testing.T) {
	if _, err := BuildQuery([]string{"--target", "localhost:5002/example/simple:dev", "--platform", "linux/amd64", "localhost:5002/example/other:dev"}); err == nil {
		t.Fatal("expected target conflict to fail")
	}
}

func TestBuildQueryRejectsDaemonManagedFlags(t *testing.T) {
	if _, err := BuildQuery([]string{"--image-dirs", "/tmp/images"}); err == nil {
		t.Fatal("expected error for daemon-managed flag")
	}
	if _, err := BuildQuery([]string{"--registry", "example.com"}); err == nil {
		t.Fatal("expected error for removed registry flag")
	}
}

func TestExportQuery(t *testing.T) {
	raw, localResult, err := ExportQuery([]string{
		"--result", "out/result.jsonl",
		"--target", "registry.example.com/ns/app:dev",
		"--target=registry.example.com/ns/base:dev",
		"--oci",
		"--with-fail",
	})
	if err != nil {
		t.Fatal(err)
	}
	values, err := url.ParseQuery(raw)
	if err != nil {
		t.Fatal(err)
	}
	if localResult != "out/result.jsonl" {
		t.Fatalf("local result = %q", localResult)
	}
	assertQueryValue(t, values, "oci", "true")
	assertQueryValue(t, values, "with-fail", "true")
	if got := values["target"]; len(got) != 2 || got[0] != "registry.example.com/ns/app:dev" || got[1] != "registry.example.com/ns/base:dev" {
		t.Fatalf("unexpected target values: %#v", got)
	}
}

func TestPreheatQuery(t *testing.T) {
	raw, err := PreheatQuery([]string{
		"--target", "registry.example.com/ns/image:dev",
		"--dragonfly-scheduler-addr", "scheduler:8002",
		"--concurrency=3",
		"--interval", "2",
		"--timeout=9",
		"--insecure-skip-verify=false",
		"--registry-plain-http", "kind-registry:5000",
		"--registry-plain-http=mirror.local:5000",
		"--fail-fast",
		"--oci",
		"--verbose",
	})
	if err != nil {
		t.Fatal(err)
	}
	values, err := url.ParseQuery(raw)
	if err != nil {
		t.Fatal(err)
	}
	assertQueryValue(t, values, "dragonfly-scheduler-addr", "scheduler:8002")
	assertQueryValue(t, values, "target", "registry.example.com/ns/image:dev")
	assertQueryValue(t, values, "concurrency", "3")
	assertQueryValue(t, values, "interval", "2")
	assertQueryValue(t, values, "timeout", "9")
	assertQueryValue(t, values, "insecure-skip-verify", "false")
	if got := values["registry-plain-http"]; len(got) != 2 || got[0] != "kind-registry:5000" || got[1] != "mirror.local:5000" {
		t.Fatalf("plain HTTP registries = %#v", got)
	}
	assertQueryValue(t, values, "fail-fast", "true")
	assertQueryValue(t, values, "oci", "true")
	assertQueryValue(t, values, "verbose", "true")
}

func assertQueryValue(t *testing.T, values url.Values, key, want string) {
	t.Helper()
	if got := values.Get(key); got != want {
		t.Fatalf("%s = %q, want %q", key, got, want)
	}
}
