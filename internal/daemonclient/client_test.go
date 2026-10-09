package daemonclient

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
)

func TestClientUsesUnixSocketAndStreamsResponse(t *testing.T) {
	socketFile, err := os.CreateTemp("", "kova-daemon-*.sock")
	if err != nil {
		t.Fatal(err)
	}
	socket := socketFile.Name()
	_ = socketFile.Close()
	_ = os.Remove(socket)
	t.Cleanup(func() { _ = os.Remove(socket) })
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != BuildPath || r.URL.Query().Get("format") != "oci" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.String())
		}
		body, _ := io.ReadAll(r.Body)
		_, _ = w.Write(body)
	})}
	go server.Serve(listener)
	t.Cleanup(func() { _ = server.Shutdown(context.Background()) })

	var output strings.Builder
	err = New(socket).Do(context.Background(), http.MethodPost, BuildPath, url.Values{"format": {"oci"}}, strings.NewReader("source"), &output)
	if err != nil {
		t.Fatal(err)
	}
	if output.String() != "source" {
		t.Fatalf("output = %q", output.String())
	}
}

func TestTransportCommandUsesTypedPath(t *testing.T) {
	got := TransportCommand(http.MethodPost, ExportPath, "oci=true", "")
	want := "kovad transport --method POST --path /api/v1/export --query oci=true"
	if strings.Join(got, " ") != want {
		t.Fatalf("command = %q, want %q", strings.Join(got, " "), want)
	}
}

func TestTransportCommandCarriesExpectedPodUIDOnlyWhenPresent(t *testing.T) {
	legacy := TransportCommand(http.MethodPost, BuildPath, "", "source.zip")
	if got := TransportCommandForPodUID(http.MethodPost, BuildPath, "", "source.zip", ""); strings.Join(got, " ") != strings.Join(legacy, " ") {
		t.Fatalf("legacy command changed: %v", got)
	}
	got := TransportCommandForPodUID(http.MethodPost, BuildPath, "", "source.zip", "pod-original")
	if strings.Join(got[len(got)-2:], " ") != "--expected-pod-uid pod-original" {
		t.Fatalf("expected Pod UID missing from transport command: %v", got)
	}
}

func TestExpectedPodUIDPreflightFailsClosed(t *testing.T) {
	t.Setenv(RunnerPodUIDEnv, "temporary")
	if err := os.Unsetenv(RunnerPodUIDEnv); err != nil {
		t.Fatal(err)
	}
	if err := ValidateExpectedPodUID(""); err != nil {
		t.Fatalf("legacy runner without UID fence was refused: %v", err)
	}
	if err := ValidateExpectedPodUID("pod-original"); err == nil {
		t.Fatal("caller claimed a Pod UID in an unpinned container")
	}
	t.Setenv(RunnerPodUIDEnv, "pod-replacement")
	for _, expected := range []string{"", "pod-original"} {
		if err := ValidateExpectedPodUID(expected); err == nil {
			t.Fatalf("replacement Pod accepted expected UID %q", expected)
		}
	}
	if err := ValidateExpectedPodUID("pod-replacement"); err != nil {
		t.Fatalf("exact target Pod UID was refused: %v", err)
	}
	t.Setenv(RunnerPodUIDEnv, "")
	if err := ValidateExpectedPodUID(""); err == nil {
		t.Fatal("empty Downward API Pod UID was treated as legacy")
	}
}
