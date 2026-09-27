package runnerexec

import (
	"errors"
	"net/url"
	"testing"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBuildQueryUsesSortedExplicitPlatformPools(t *testing.T) {
	raw := BuildQuery(&kovav1.KovaBuild{ObjectMeta: metav1.ObjectMeta{UID: "build-uid"}, Spec: kovav1.KovaBuildSpec{Build: kovav1.KovaBuildOptions{Format: "oci", Concurrency: 2}}}, map[string]string{
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
	if values.Get("request-id") != "build-uid" {
		t.Fatalf("request ID = %q", values.Get("request-id"))
	}
}

func TestBoundedExportBufferRejectsOversizedRunnerResponse(t *testing.T) {
	var out boundedExportBuffer
	chunk := make([]byte, maxExportBytes)
	if n, err := out.Write(chunk); n != maxExportBytes || err != nil {
		t.Fatalf("first write n=%d err=%v", n, err)
	}
	if n, err := out.Write([]byte("x")); n != 0 || !errors.Is(err, ErrExportTooLarge) || !out.overflow || out.Len() != maxExportBytes {
		t.Fatalf("overflow n=%d err=%v flag=%v length=%d", n, err, out.overflow, out.Len())
	}
}

func TestBoundedExportErrorBufferKeepsTransportDiagnosticsFinite(t *testing.T) {
	var out boundedExportErrorBuffer
	chunk := make([]byte, maxExportErrorBytes+100)
	if n, err := out.Write(chunk); n != len(chunk) || err != nil || out.Len() != maxExportErrorBytes {
		t.Fatalf("write n=%d err=%v length=%d", n, err, out.Len())
	}
}
