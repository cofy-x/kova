package daemon

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cofy-x/kova/internal/daemonclient"

	cli "github.com/urfave/cli/v2"
)

func TestTransportRejectsReplacementPodBeforeInputOrDaemonCall(t *testing.T) {
	socketFile, err := os.CreateTemp("", "kova-uid-*.sock")
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
	var calls atomic.Int32
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte("accepted"))
	})}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Shutdown(context.Background()) })
	app := &cli.App{Commands: []*cli.Command{TransportCLICommand()}}
	var output bytes.Buffer
	app.Writer = &output
	t.Setenv(daemonclient.RunnerPodUIDEnv, "pod-replacement")
	missingInput := filepath.Join(t.TempDir(), "missing-source.zip")
	for _, args := range [][]string{
		{"kovad", "transport", "--method", "POST", "--path", daemonclient.BuildPath, "--socket", socket,
			"--input", missingInput, "--expected-pod-uid", "pod-original"},
		{"kovad", "transport", "--method", "GET", "--path", daemonclient.StatusPath, "--socket", socket},
	} {
		if err := app.Run(args); err == nil || !strings.Contains(err.Error(), "Pod UID fence") {
			t.Fatalf("replacement/absent UID was not rejected before input and socket: args=%v err=%v", args, err)
		}
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("replacement Pod reached daemon %d time(s)", got)
	}
	if err := app.Run([]string{"kovad", "transport", "--method", "GET", "--path", daemonclient.StatusPath,
		"--socket", socket, "--expected-pod-uid", "pod-replacement"}); err != nil {
		t.Fatalf("exact Pod UID could not contact daemon: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("exact Pod UID produced %d daemon calls, want one", got)
	}
	if err := os.Unsetenv(daemonclient.RunnerPodUIDEnv); err != nil {
		t.Fatal(err)
	}
	if err := app.Run([]string{"kovad", "transport", "--method", "GET", "--path", daemonclient.StatusPath,
		"--socket", socket}); err != nil {
		t.Fatalf("legacy unpinned transport was refused: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("legacy transport produced %d daemon calls, want two total", got)
	}
}
