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
