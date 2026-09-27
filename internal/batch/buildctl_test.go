package batch

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/cofy-x/kova/internal/scheduler"
	"github.com/cofy-x/kova/internal/source"
)

func installFakeBuildCommands(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	buildctl := `#!/bin/sh
set -eu
metadata=
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--metadata-file" ]; then metadata=$2; shift 2; else shift; fi
done
test -n "$metadata"
printf '{"containerimage.digest":"%s"}' "$FAKE_BUILDKIT_DIGEST" > "$metadata"
`
	if err := os.WriteFile(filepath.Join(dir, "buildctl"), []byte(buildctl), 0700); err != nil {
		t.Fatal(err)
	}
	nydusify := `#!/bin/sh
set -eu
printf '%s\n' "$@" > "$FAKE_NYDUS_ARGS"
metadata=
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--output-json" ]; then metadata=$2; shift 2; else shift; fi
done
test -n "$metadata"
printf '{"target_manifest_digest":"%s"}' "$FAKE_NYDUS_DIGEST" > "$metadata"
`
	if err := os.WriteFile(filepath.Join(dir, "nydusify"), []byte(nydusify), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return dir
}

func TestRunBuildCommandsReturnsExactBuildKitPushDigest(t *testing.T) {
	installFakeBuildCommands(t)
	want := "sha256:" + strings.Repeat("a", 64)
	t.Setenv("FAKE_BUILDKIT_DIGEST", want)
	var output bytes.Buffer
	got, err := runBuildCommands(context.Background(), source.Spec{
		Target: "registry.example.com/example:dev", Platform: "linux/amd64", Format: source.BuildFormatOCI,
	}, &scheduler.Addr{Addr: "tcp://buildkitd:9094"}, Options{}, &output)
	if err != nil || got != want {
		t.Fatalf("digest=%q err=%v, want %q", got, err, want)
	}
}

func TestRunBuildCommandsRejectsMissingPushDigest(t *testing.T) {
	installFakeBuildCommands(t)
	t.Setenv("FAKE_BUILDKIT_DIGEST", "")
	var output bytes.Buffer
	digest, err := runBuildCommands(context.Background(), source.Spec{
		Target: "registry.example.com/example:dev", Platform: "linux/amd64", Format: source.BuildFormatOCI,
	}, &scheduler.Addr{Addr: "tcp://buildkitd:9094"}, Options{}, &output)
	if err == nil || digest != "" || !strings.Contains(err.Error(), "valid pushed manifest digest") {
		t.Fatalf("digest=%q err=%v, want missing-digest failure", digest, err)
	}
}

func TestNydusConversionPinsThisBuildsOCISource(t *testing.T) {
	dir := installFakeBuildCommands(t)
	ociDigest := "sha256:" + strings.Repeat("b", 64)
	nydusDigest := "sha256:" + strings.Repeat("c", 64)
	t.Setenv("FAKE_BUILDKIT_DIGEST", ociDigest)
	t.Setenv("FAKE_NYDUS_DIGEST", nydusDigest)
	argsPath := filepath.Join(dir, "nydus-args")
	t.Setenv("FAKE_NYDUS_ARGS", argsPath)
	var output bytes.Buffer
	digest, err := runBuildCommands(context.Background(), source.Spec{
		Target: "registry.example.com/example:dev_nydus_v3", Platform: "linux/amd64", Format: source.BuildFormatNydus,
	}, &scheduler.Addr{Addr: "tcp://buildkitd:9094"}, Options{}, &output)
	if err != nil {
		t.Fatal(err)
	}
	if digest != nydusDigest {
		t.Fatalf("Nydus output digest = %q, want %q", digest, nydusDigest)
	}
	args, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(args), "registry.example.com/example@"+ociDigest) {
		t.Fatalf("Nydus source is not digest-pinned: %q", args)
	}
}

func TestNydusConversionUsesExplicitPlainHTTPRegistry(t *testing.T) {
	dir := installFakeBuildCommands(t)
	t.Setenv("FAKE_BUILDKIT_DIGEST", "sha256:"+strings.Repeat("a", 64))
	t.Setenv("FAKE_NYDUS_DIGEST", "sha256:"+strings.Repeat("b", 64))
	argsPath := filepath.Join(dir, "nydus-args")
	t.Setenv("FAKE_NYDUS_ARGS", argsPath)
	var output bytes.Buffer
	_, err := runBuildCommands(context.Background(), source.Spec{
		Target:   "kova-digest-fault-proxy.kova.svc.cluster.local:5000/example:dev_nydus_v3",
		Platform: "linux/amd64", Format: source.BuildFormatNydus,
	}, &scheduler.Addr{Addr: "tcp://buildkitd:9094"}, Options{
		RegistryPlainHTTP: []string{"kova-digest-fault-proxy.kova.svc.cluster.local:5000"},
	}, &output)
	if err != nil {
		t.Fatal(err)
	}
	args, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	joined := " " + strings.Join(strings.Fields(string(args)), " ") + " "
	for _, want := range []string{" --source-insecure ", " --target-insecure ", " --plain-http "} {
		if !strings.Contains(joined, want) {
			t.Fatalf("expected %q in Nydusify args %q", want, joined)
		}
	}
}

func TestNydusConversionRejectsMissingPushDigest(t *testing.T) {
	dir := installFakeBuildCommands(t)
	t.Setenv("FAKE_BUILDKIT_DIGEST", "sha256:"+strings.Repeat("a", 64))
	t.Setenv("FAKE_NYDUS_DIGEST", "")
	t.Setenv("FAKE_NYDUS_ARGS", filepath.Join(dir, "nydus-args"))
	var output bytes.Buffer
	digest, err := runBuildCommands(context.Background(), source.Spec{
		Target: "registry.example.com/example:dev_nydus_v3", Platform: "linux/amd64", Format: source.BuildFormatNydus,
	}, &scheduler.Addr{Addr: "tcp://buildkitd:9094"}, Options{}, &output)
	if err == nil || digest != "" || !strings.Contains(err.Error(), "Nydusify did not report a valid pushed manifest digest") {
		t.Fatalf("digest=%q err=%v, want missing-digest failure", digest, err)
	}
}

func TestBuildCommandArgsOCI(t *testing.T) {
	args := buildCommandArgs(
		source.Spec{Dir: "/tmp/src", Target: "localhost:5001/example:dev", Platform: "linux/amd64"},
		&scheduler.Addr{Addr: "tcp://127.0.0.1:9094"},
	)

	expected := []string{
		"--addr", "tcp://127.0.0.1:9094",
		"build",
		"--frontend=dockerfile.v0",
		"--opt", "platform=linux/amd64",
		"--local", "context=/tmp/src",
		"--local", "dockerfile=/tmp/src",
		"--output", "type=image,name=localhost:5001/example:dev,push=true,force-compression=true,oci-mediatypes=true,compression=gzip",
	}
	if !reflect.DeepEqual(args, expected) {
		t.Fatalf("unexpected args:\nwant %#v\n got %#v", expected, args)
	}
}

func TestBuildCommandArgsWithoutLocalDir(t *testing.T) {
	args := buildCommandArgs(
		source.Spec{Target: "registry.example.com/example:dev_nydus_v3", Platform: "linux/arm64"},
		&scheduler.Addr{Addr: "tcp://buildkitd:9094"},
	)

	joined := strings.Join(args, " ")
	if strings.Contains(joined, "--local") {
		t.Fatalf("did not expect local args, got %q", joined)
	}
	if !strings.Contains(joined, "type=image") {
		t.Fatalf("expected image output options, got %q", joined)
	}
}

func TestNydusConvertArgs(t *testing.T) {
	args := nydusConvertArgs(
		"host.docker.internal:5001/example:dev",
		"host.docker.internal:5001/example:dev_nydus_v3",
		nil,
	)
	joined := strings.Join(args, " ")
	for _, want := range []string{
		"convert",
		"--source host.docker.internal:5001/example:dev",
		"--target host.docker.internal:5001/example:dev_nydus_v3",
		"--fs-version 5",
		"--source-insecure",
		"--target-insecure",
		"--plain-http",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("expected %q in args %q", want, joined)
		}
	}
}

func TestNydusConvertArgsKeepsHTTPSRegistriesStrict(t *testing.T) {
	args := nydusConvertArgs(
		"registry.example.com/example:dev",
		"registry.example.com/example:dev_nydus_v3",
		nil,
	)
	joined := strings.Join(args, " ")
	for _, notWant := range []string{"--source-insecure", "--target-insecure", "--plain-http"} {
		if strings.Contains(joined, notWant) {
			t.Fatalf("did not expect %q in args %q", notWant, joined)
		}
	}
}

func TestNydusConvertArgsMatchesOnlyExactConfiguredRegistry(t *testing.T) {
	allowed := []string{"proxy.kova.svc.cluster.local:5000"}
	for _, tc := range []struct {
		name          string
		registry      string
		wantPlainHTTP bool
	}{
		{name: "exact host and port", registry: "proxy.kova.svc.cluster.local:5000", wantPlainHTTP: true},
		{name: "different port", registry: "proxy.kova.svc.cluster.local:5001"},
		{name: "host suffix", registry: "proxy.kova.svc.cluster.local.evil:5000"},
		{name: "unlisted cluster host", registry: "other.kova.svc.cluster.local:5000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := nydusConvertArgs(tc.registry+"/example:dev", tc.registry+"/example:dev_nydus_v3", allowed)
			joined := " " + strings.Join(args, " ") + " "
			for _, flag := range []string{" --source-insecure ", " --target-insecure ", " --plain-http "} {
				if strings.Contains(joined, flag) != tc.wantPlainHTTP {
					t.Fatalf("flag %q in args %q; want plain HTTP %t", flag, joined, tc.wantPlainHTTP)
				}
			}
		})
	}
}
