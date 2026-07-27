package daemon

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cofy-x/kova/internal/batch"

	"github.com/labstack/echo/v4"
)

func testDaemonServer(backend serverBackend) *daemonServer {
	return newDaemonServer("127.0.0.1:9094", "/tmp/result.lmdb", "/tmp/logs.jsonl", backend)
}

func performEchoRequest(t *testing.T, e *echo.Echo, method string, path string, body string, handler echo.HandlerFunc) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	if err := handler(c); err != nil {
		t.Fatalf("handler returned error: %v", err)
	}
	return rec
}

func decodeDaemonState(t *testing.T, rec *httptest.ResponseRecorder) daemonState {
	t.Helper()
	var state daemonState
	if err := json.Unmarshal(rec.Body.Bytes(), &state); err != nil {
		t.Fatalf("decode state: %v; body=%s", err, rec.Body.String())
	}
	return state
}

func waitForState(t *testing.T, srv *daemonServer, status string) daemonState {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		state := srv.getBuildState()
		if state.Status == status {
			return state
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for status %q; last=%#v", status, srv.getBuildState())
	return daemonState{}
}

func TestHandleBuildPostRunsAsyncBuild(t *testing.T) {
	buildCalled := make(chan batch.Options, 1)
	srv := testDaemonServer(serverBackend{
		validateBuildArchive: func(string) (int, error) { return 1, nil },
		extractZip:           func(string, string) error { return nil },
		runBuild: func(opts batch.Options) error {
			buildCalled <- opts
			return nil
		},
	})
	e := echo.New()

	rec := performEchoRequest(t, e, http.MethodPost, "/api/v1/build?format=oci", "zip-body", srv.handleBuildPost)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected accepted, got %d body=%s", rec.Code, rec.Body.String())
	}
	waitForState(t, srv, "completed")

	select {
	case opts := <-buildCalled:
		if opts.ImageDirs != daemonImageDir {
			t.Fatalf("unexpected image dir %q", opts.ImageDirs)
		}
		if opts.BuildFormat != "oci" {
			t.Fatalf("expected OCI build format, got %q", opts.BuildFormat)
		}
	case <-time.After(time.Second):
		t.Fatal("expected build to be called")
	}
}

func TestHandleBuildPostRejectsConcurrentBuild(t *testing.T) {
	block := make(chan struct{})
	srv := testDaemonServer(serverBackend{
		validateBuildArchive: func(string) (int, error) { return 1, nil },
		extractZip:           func(string, string) error { return nil },
		runBuild: func(opts batch.Options) error {
			<-block
			return nil
		},
	})
	e := echo.New()

	first := performEchoRequest(t, e, http.MethodPost, "/api/v1/build", "zip-body", srv.handleBuildPost)
	if first.Code != http.StatusAccepted {
		t.Fatalf("expected first request accepted, got %d", first.Code)
	}
	second := performEchoRequest(t, e, http.MethodPost, "/api/v1/build", "zip-body", srv.handleBuildPost)
	if second.Code != http.StatusConflict {
		t.Fatalf("expected conflict, got %d body=%s", second.Code, second.Body.String())
	}
	close(block)
	waitForState(t, srv, "completed")
}

func TestHandleBuildCancelCancelsRunningBuild(t *testing.T) {
	buildDone := make(chan struct{})
	srv := testDaemonServer(serverBackend{
		validateBuildArchive: func(string) (int, error) { return 1, nil },
		extractZip:           func(string, string) error { return nil },
		runBuild: func(opts batch.Options) error {
			<-opts.Ctx.Done()
			close(buildDone)
			return opts.Ctx.Err()
		},
	})
	e := echo.New()

	rec := performEchoRequest(t, e, http.MethodPost, "/api/v1/build", "zip-body", srv.handleBuildPost)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected build accepted, got %d", rec.Code)
	}

	cancelRec := performEchoRequest(t, e, http.MethodPost, "/api/v1/build/cancel", "", srv.handleBuildCancel)
	if cancelRec.Code != http.StatusAccepted {
		t.Fatalf("expected cancel accepted, got %d body=%s", cancelRec.Code, cancelRec.Body.String())
	}
	if state := decodeDaemonState(t, cancelRec); state.Status != "cancelling" {
		t.Fatalf("expected cancelling response, got %#v", state)
	}
	select {
	case <-buildDone:
	case <-time.After(time.Second):
		t.Fatal("expected build context cancellation")
	}
	state := waitForState(t, srv, "cancelled")
	if state.Error == "" {
		t.Fatalf("expected cancellation error message, got %#v", state)
	}
}

func TestHandleBuildPostRejectsInvalidArchive(t *testing.T) {
	srv := testDaemonServer(serverBackend{
		validateBuildArchive: func(string) (int, error) { return 0, errors.New("bad archive") },
	})
	e := echo.New()

	rec := performEchoRequest(t, e, http.MethodPost, "/api/v1/build", "zip-body", srv.handleBuildPost)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected bad request, got %d body=%s", rec.Code, rec.Body.String())
	}
	state := decodeDaemonState(t, rec)
	if state.Status != "failed" || !strings.Contains(state.Error, "bad archive") {
		t.Fatalf("unexpected state %#v", state)
	}
}

func TestHandleExportStreamsRunExportOutput(t *testing.T) {
	srv := testDaemonServer(serverBackend{
		runExport: func(opts batch.Options) error {
			return os.WriteFile(opts.ResultPath, []byte("{\"success\":true}\n"), 0o644)
		},
	})
	e := echo.New()

	rec := performEchoRequest(t, e, http.MethodPost, "/api/v1/export?oci=true", "", srv.handleExport)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected OK, got %d body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != "{\"success\":true}\n" {
		t.Fatalf("unexpected export body %q", got)
	}
}

func TestHandlePreheatRunsDependency(t *testing.T) {
	called := false
	srv := testDaemonServer(serverBackend{
		runPreheat: func(opts batch.Options) error {
			called = true
			if opts.Ctx == nil {
				t.Fatal("expected request context")
			}
			if opts.DragonflySchedulerAddr != "dragonfly:8002" {
				t.Fatalf("unexpected scheduler %q", opts.DragonflySchedulerAddr)
			}
			return nil
		},
	})
	e := echo.New()

	rec := performEchoRequest(t, e, http.MethodPost, "/api/v1/preheat?dragonfly-scheduler-addr=dragonfly:8002", "", srv.handlePreheat)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected OK, got %d body=%s", rec.Code, rec.Body.String())
	}
	if !called {
		t.Fatal("expected preheat dependency to be called")
	}
}

func TestCancelActiveBuildMarksCancellableState(t *testing.T) {
	cancelled := false
	srv := testDaemonServer(serverBackend{})
	srv.mu.Lock()
	srv.build = daemonState{Status: "running"}
	srv.buildDone = make(chan struct{})
	srv.buildCancel = func() { cancelled = true }
	srv.mu.Unlock()

	done, ok := srv.cancelActiveBuild("test")
	if !ok || done == nil || !cancelled {
		t.Fatalf("expected cancellation, ok=%t done=%v cancelled=%t", ok, done, cancelled)
	}
	if state := srv.getBuildState(); state.Status != "cancelling" || state.Error != "test" {
		t.Fatalf("unexpected state %#v", state)
	}
}
