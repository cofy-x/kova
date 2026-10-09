package daemon

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cofy-x/kova/internal/batch"
	"github.com/cofy-x/kova/internal/daemonclient"
)

func testRetireHTTPClient(t *testing.T, srv *daemonServer) *daemonclient.Client {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "kova-retire-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	path := filepath.Join(dir, "daemon.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := &http.Server{Handler: newDaemonRouter(srv)}
	serveDone := make(chan error, 1)
	go func() { serveDone <- httpServer.Serve(listener) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := httpServer.Shutdown(ctx); err != nil {
			t.Errorf("shutdown daemon: %v", err)
		}
		if err := <-serveDone; err != nil && err != http.ErrServerClosed {
			t.Errorf("serve daemon: %v", err)
		}
	})
	return daemonclient.New(path)
}

func TestRetireUnixHTTPProtocolBlocksLateBodyAndWrongID(t *testing.T) {
	var builds atomic.Int32
	srv := testDaemonServer(serverBackend{
		runBuild: func(batch.Options) error {
			builds.Add(1)
			return nil
		},
	})
	client := testRetireHTTPClient(t, srv)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if _, err := client.ReadBuildRetire(ctx, "exact-request"); err == nil {
		t.Fatal("readback before install unexpectedly succeeded")
	}
	status, err := client.RetireBuild(ctx, "exact-request")
	if err != nil {
		t.Fatal(err)
	}
	if !status.LocalSettled || status.RemoteWorkerSettled || status.Build.Status != "idle" {
		t.Fatalf("retire-first status=%#v", status)
	}
	if _, err := client.RetireBuild(ctx, "wrong-request"); err == nil {
		t.Fatal("retire of a different request ID unexpectedly succeeded")
	}
	if _, err := client.ReadBuildRetire(ctx, "wrong-request"); err == nil {
		t.Fatal("readback for a different request ID unexpectedly succeeded")
	}
	if _, err := client.ReadBuildRetire(ctx, "exact-request"); err != nil {
		t.Fatalf("exact readback: %v", err)
	}
	query := url.Values{"request-id": {"exact-request"}}
	if err := client.Do(ctx, http.MethodPost, daemonclient.BuildPath, query, strings.NewReader("late zip body"), io.Discard); err == nil || !strings.Contains(err.Error(), "HTTP 409") {
		t.Fatalf("late build POST should conflict, got %v", err)
	}
	if builds.Load() != 0 {
		t.Fatalf("retired runner executed %d builds", builds.Load())
	}
}

func TestRetireUnixHTTPProtocolJoinsUploadingBuild(t *testing.T) {
	srv := testDaemonServer(serverBackend{
		validateBuildArchive: func(string) (int, error) { return 1, nil },
		extractZip:           func(string, string) error { return nil },
		runBuild: func(opts batch.Options) error {
			return opts.Ctx.Err()
		},
	})
	client := testRetireHTTPClient(t, srv)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	reader, writer := io.Pipe()
	defer writer.Close()
	posted := make(chan error, 1)
	go func() {
		posted <- client.Do(ctx, http.MethodPost, daemonclient.BuildPath, url.Values{"request-id": {"uploading-request"}}, reader, io.Discard)
	}()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		srv.mu.RLock()
		admitted := srv.buildRequestID == "uploading-request" && srv.buildDone != nil
		srv.mu.RUnlock()
		if admitted {
			break
		}
		time.Sleep(time.Millisecond)
	}
	srv.mu.RLock()
	admitted := srv.buildRequestID == "uploading-request" && srv.buildDone != nil
	srv.mu.RUnlock()
	if !admitted {
		t.Fatal("HTTP build POST was not admitted while its body remained open")
	}

	status, err := client.RetireBuild(ctx, "uploading-request")
	if err != nil {
		t.Fatal(err)
	}
	if status.Phase != "retiring" || status.LocalSettled || status.RemoteWorkerSettled {
		t.Fatalf("retire during HTTP upload=%#v", status)
	}
	status, err = client.ReadBuildRetire(ctx, "uploading-request")
	if err != nil || status.RemoteWorkerSettled || status.RequestID != "uploading-request" {
		t.Fatalf("readback after retire=%#v, %v", status, err)
	}
	for {
		status, err = client.ReadBuildRetire(ctx, "uploading-request")
		if err != nil {
			t.Fatal(err)
		}
		if status.LocalSettled {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("daemon never locally joined the retired build")
		case <-time.After(time.Millisecond):
		}
	}
	if status.RemoteWorkerSettled || status.Build.Status != "cancelled" {
		t.Fatalf("joined HTTP build=%#v", status)
	}
	// The upload client has deliberately neither written nor closed its Body.
	// The daemon must have interrupted that exact admitted request itself.
	select {
	case <-posted:
	case <-ctx.Done():
		t.Fatal("retired HTTP build remained blocked on an uncooperative upload")
	}
}
