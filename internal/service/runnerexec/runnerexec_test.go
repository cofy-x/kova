package runnerexec

import (
	"net/url"
	"testing"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
)

func TestBuildQueryUsesSortedExplicitPlatformPools(t *testing.T) {
	raw := BuildQuery(&kovav1.KovaBuild{Spec: kovav1.KovaBuildSpec{Build: kovav1.KovaBuildOptions{Format: "oci", Concurrency: 2}}}, map[string]string{
		"linux/arm64": "tcp://arm64.example:9094",
		"linux/amd64": "tcp://amd64.example:9094",
	})
	values, err := url.ParseQuery(raw)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"linux/amd64=tcp://amd64.example:9094", "linux/arm64=tcp://arm64.example:9094"}
	got := values["platform-addr"]
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("platform pools = %#v, want %#v", got, want)
	}
	if values.Get("addrs") != "" {
		t.Fatalf("unexpected unscoped BuildKit address: %q", values.Get("addrs"))
	}
}
