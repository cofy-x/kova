package runnerexec

import (
	"context"
	"errors"
	"io"
	"net/url"
	"strings"
	"testing"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/kube"
	"github.com/cofy-x/kova/internal/sourcebundle"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilexec "k8s.io/client-go/util/exec"
)

type fakeExecKube struct {
	kube.API
	exec func(kube.ExecOptions) error
}

func (f fakeExecKube) Exec(_ context.Context, _, _ string, options kube.ExecOptions) error {
	return f.exec(options)
}

func TestBuildQueryUsesSortedExplicitPlatformPools(t *testing.T) {
	raw := BuildQuery(&kovav1.KovaBuild{ObjectMeta: metav1.ObjectMeta{UID: "build-uid"}, Spec: kovav1.KovaBuildSpec{Build: kovav1.KovaBuildOptions{Format: "oci", Concurrency: 2}}}, map[string]string{
		"linux/arm64": "tcp://arm64.example:9094",
		"linux/amd64": "tcp://amd64.example:9094",
	}, []string{"kind-registry:5000", "kova-digest-fault-proxy.kova.svc.cluster.local:5000"})
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
	if got := values["registry-plain-http"]; len(got) != 2 || got[0] != "kind-registry:5000" || got[1] != "kova-digest-fault-proxy.kova.svc.cluster.local:5000" {
		t.Fatalf("plain HTTP registries = %#v", got)
	}
}

func TestSubmitBuildForwardsPlainHTTPRegistriesToDaemon(t *testing.T) {
	build := &kovav1.KovaBuild{ObjectMeta: metav1.ObjectMeta{Namespace: "jobs", Name: "example", UID: "build-uid"}, Status: kovav1.KovaBuildStatus{RunnerPodName: "runner"}}
	client := Client{
		RegistryPlainHTTP: []string{"proxy.kova.svc.cluster.local:5000"},
		Kube: fakeExecKube{exec: func(options kube.ExecOptions) error {
			var query string
			for i, arg := range options.Command {
				if arg == "--query" && i+1 < len(options.Command) {
					query = options.Command[i+1]
				}
			}
			values, err := url.ParseQuery(query)
			if err != nil {
				t.Fatal(err)
			}
			if got := values["registry-plain-http"]; len(got) != 1 || got[0] != "proxy.kova.svc.cluster.local:5000" {
				t.Fatalf("daemon query registries = %#v", got)
			}
			_, _ = io.WriteString(options.Stdout, `{"status":"running"}`)
			return nil
		}},
	}
	if err := client.SubmitBuild(context.Background(), build, "/source"); err != nil {
		t.Fatal(err)
	}
}

func TestBoundedResponseBufferRejectsOversizedRunnerResponse(t *testing.T) {
	out := boundedResponseBuffer{overflowErr: ErrRunnerResponseTooLarge}
	chunk := make([]byte, maxRunnerResponseBytes)
	if n, err := out.Write(chunk); n != maxRunnerResponseBytes || err != nil {
		t.Fatalf("first write n=%d err=%v", n, err)
	}
	if n, err := io.WriteString(&out, "x"); n != 0 || !errors.Is(err, ErrRunnerResponseTooLarge) || !out.overflow || out.Len() != maxRunnerResponseBytes {
		t.Fatalf("overflow n=%d err=%v flag=%v length=%d", n, err, out.overflow, out.Len())
	}
}

func TestBoundedErrorBufferKeepsTransportDiagnosticsFinite(t *testing.T) {
	var out boundedErrorBuffer
	chunk := strings.Repeat("x", maxRunnerErrorBytes+100)
	if n, err := io.WriteString(&out, chunk); n != len(chunk) || err != nil || out.Len() != maxRunnerErrorBytes {
		t.Fatalf("write n=%d err=%v length=%d", n, err, out.Len())
	}
	if !strings.HasSuffix(string(out.Diagnostic()), "[runner stderr truncated]") {
		t.Fatalf("missing truncation marker: %q", out.Diagnostic()[out.Len():])
	}
}

func TestRunnerCommandsRejectOversizedStdoutWhenExecIgnoresWriterError(t *testing.T) {
	build := &kovav1.KovaBuild{ObjectMeta: metav1.ObjectMeta{Namespace: "jobs", Name: "example"}, Status: kovav1.KovaBuildStatus{RunnerPodName: "runner"}}
	oversized := strings.Repeat("x", maxRunnerResponseBytes+1)
	client := Client{Kube: fakeExecKube{exec: func(options kube.ExecOptions) error {
		_, _ = io.WriteString(options.Stdout, oversized)
		return nil
	}}}
	if _, err := client.SourceTargets(context.Background(), build, "/source"); !errors.Is(err, ErrRunnerResponseTooLarge) || errors.Is(err, ErrSourceInspectTransport) {
		t.Fatalf("source inspect error = %v", err)
	}
	if err := client.SubmitBuild(context.Background(), build, "/source"); !errors.Is(err, ErrRunnerResponseTooLarge) {
		t.Fatalf("submit error = %v", err)
	}
	if _, err := client.BuildStatus(context.Background(), build); !errors.Is(err, ErrRunnerResponseTooLarge) || !errors.Is(err, ErrInvalidBuildStatus) {
		t.Fatalf("build status error = %v", err)
	}
	if _, err := client.Post(context.Background(), build, "export", ""); !errors.Is(err, ErrExportTooLarge) {
		t.Fatalf("export error = %v", err)
	}
}

func TestSourceTargetsClassifiesOnlyExplicitInspectExitCodes(t *testing.T) {
	build := &kovav1.KovaBuild{ObjectMeta: metav1.ObjectMeta{Namespace: "jobs", Name: "example"}, Status: kovav1.KovaBuildStatus{RunnerPodName: "runner"}}
	for _, tc := range []struct {
		name      string
		exitCode  int
		wantError error
	}{
		{name: "invalid archive", exitCode: sourcebundle.FetchExitCodeInvalidSource, wantError: ErrSourceInspectInvalid},
		{name: "disk full", exitCode: sourcebundle.FetchExitCodeResourceExhausted, wantError: ErrSourceInspectResourceExhausted},
		{name: "unclassified exit", exitCode: 1, wantError: ErrSourceInspectUnavailable},
		{name: "possible signal", exitCode: 137, wantError: ErrSourceInspectUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := Client{Kube: fakeExecKube{exec: func(options kube.ExecOptions) error {
				_, _ = io.WriteString(options.Stderr, "inspect failed")
				return utilexec.CodeExitError{Err: errors.New("process exited"), Code: tc.exitCode}
			}}}
			_, err := client.SourceTargets(context.Background(), build, "/source")
			if !errors.Is(err, tc.wantError) || errors.Is(err, ErrSourceInspectTransport) {
				t.Fatalf("exit code %d classified as %v", tc.exitCode, err)
			}
		})
	}
}

func TestRunnerCommandStderrIsBounded(t *testing.T) {
	build := &kovav1.KovaBuild{ObjectMeta: metav1.ObjectMeta{Namespace: "jobs", Name: "example"}, Status: kovav1.KovaBuildStatus{RunnerPodName: "runner"}}
	client := Client{Kube: fakeExecKube{exec: func(options kube.ExecOptions) error {
		_, _ = io.WriteString(options.Stderr, strings.Repeat("x", maxRunnerErrorBytes+100))
		return errors.New("exec failed")
	}}}
	err := client.CancelBuild(context.Background(), build)
	if err == nil || !strings.Contains(err.Error(), "[runner stderr truncated]") || len(err.Error()) > maxRunnerErrorBytes+100 {
		t.Fatalf("bounded cancel error = %v", err)
	}
}
