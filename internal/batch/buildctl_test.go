package batch

import (
	"reflect"
	"strings"
	"testing"

	"github.com/cofy-x/kova/internal/scheduler"
	"github.com/cofy-x/kova/internal/source"
)

func TestBuildCommandArgsOCI(t *testing.T) {
	args := buildCommandArgs(
		source.Spec{Dir: "/tmp/src", Target: "localhost:5001/example:dev"},
		&scheduler.Addr{Addr: "tcp://127.0.0.1:9094"},
	)

	expected := []string{
		"--addr", "tcp://127.0.0.1:9094",
		"build",
		"--frontend=dockerfile.v0",
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
		source.Spec{Target: "registry.example.com/example:dev_nydus_v3"},
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
	)
	joined := strings.Join(args, " ")
	for _, notWant := range []string{"--source-insecure", "--target-insecure", "--plain-http"} {
		if strings.Contains(joined, notWant) {
			t.Fatalf("did not expect %q in args %q", notWant, joined)
		}
	}
}
