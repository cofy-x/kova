package daemon

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cofy-x/kova/internal/batch"
	"github.com/cofy-x/kova/internal/source"

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

func TestBuildStatusAdvertisesIdempotentRequestCapability(t *testing.T) {
	srv := testDaemonServer(serverBackend{})
	rec := performEchoRequest(t, echo.New(), http.MethodGet, "/api/v1/build/status", "", srv.handleBuildStatus)
	if rec.Code != http.StatusOK {
		t.Fatalf("status response=%d body=%s", rec.Code, rec.Body.String())
	}
	state := decodeDaemonState(t, rec)
	if state.Status != "idle" || len(state.Capabilities) != 1 || state.Capabilities[0] != "idempotent-build-request-v1" {
		t.Fatalf("status response=%#v", state)
	}
}

func TestHandleBuildPostRunsAsyncBuild(t *testing.T) {
	uploadDir := t.TempDir()
	t.Setenv("TMPDIR", uploadDir)
	buildCalled := make(chan batch.Options, 1)
	var uploadReleased atomic.Bool
	srv := testDaemonServer(serverBackend{
		validateBuildArchive: func(string) (int, error) { return 1, nil },
		extractZip:           func(string, string) error { return nil },
		runBuild: func(opts batch.Options) error {
			entries, err := os.ReadDir(uploadDir)
			uploadReleased.Store(err == nil && len(entries) == 0)
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
	if !uploadReleased.Load() {
		t.Fatal("runner kept the redundant uploaded ZIP through the build")
	}

	select {
	case opts := <-buildCalled:
		if opts.ImageDirs != daemonImageDir {
			t.Fatalf("unexpected image dir %q", opts.ImageDirs)
		}
		if !opts.ImageDirsAlreadyIsolated {
			t.Fatal("runner extraction must use the single-use, in-place source path")
		}
		if opts.BuildFormat != "oci" {
			t.Fatalf("expected OCI build format, got %q", opts.BuildFormat)
		}
	case <-time.After(time.Second):
		t.Fatal("expected build to be called")
	}
}

func TestHandleBuildPostRejectsOversizedArchiveAndCleansTemp(t *testing.T) {
	tempDir := t.TempDir()
	t.Setenv("TMPDIR", tempDir)
	srv := testDaemonServer(serverBackend{})
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/build", strings.NewReader("short"))
	req.ContentLength = source.MaxArchiveBytes + 1
	rec := httptest.NewRecorder()
	if err := srv.handleBuildPost(e.NewContext(req, rec)); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413: %s", rec.Code, rec.Body.String())
	}
	if state := decodeDaemonState(t, rec); !strings.Contains(state.Error, source.ErrArchiveTooLarge.Error()) {
		t.Fatalf("daemon error = %q", state.Error)
	}
	if entries, err := os.ReadDir(tempDir); err != nil || len(entries) != 0 {
		t.Fatalf("rejected upload left temporary files: %v, %v", entries, err)
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

func TestHandleBuildPostDeduplicatesSameRequestWhileRunningAndAfterCompletion(t *testing.T) {
	block := make(chan struct{})
	var builds atomic.Int32
	srv := testDaemonServer(serverBackend{
		validateBuildArchive: func(string) (int, error) { return 1, nil },
		extractZip:           func(string, string) error { return nil },
		runBuild: func(batch.Options) error {
			builds.Add(1)
			<-block
			return nil
		},
	})
	e := echo.New()
	path := "/api/v1/build?request-id=build-uid"
	first := performEchoRequest(t, e, http.MethodPost, path, "zip-body", srv.handleBuildPost)
	if first.Code != http.StatusAccepted {
		t.Fatalf("first response=%d body=%s", first.Code, first.Body.String())
	}
	duplicate := performEchoRequest(t, e, http.MethodPost, path, "zip-body", srv.handleBuildPost)
	if duplicate.Code != http.StatusOK || decodeDaemonState(t, duplicate).RequestID != "build-uid" {
		t.Fatalf("duplicate response=%d body=%s", duplicate.Code, duplicate.Body.String())
	}
	other := performEchoRequest(t, e, http.MethodPost, "/api/v1/build?request-id=other-uid", "zip-body", srv.handleBuildPost)
	if other.Code != http.StatusConflict {
		t.Fatalf("different request response=%d body=%s", other.Code, other.Body.String())
	}
	close(block)
	waitForState(t, srv, "completed")
	terminalDuplicate := performEchoRequest(t, e, http.MethodPost, path, "zip-body", srv.handleBuildPost)
	if terminalDuplicate.Code != http.StatusOK || decodeDaemonState(t, terminalDuplicate).Status != "completed" {
		t.Fatalf("terminal duplicate response=%d body=%s", terminalDuplicate.Code, terminalDuplicate.Body.String())
	}
	lateOther := performEchoRequest(t, e, http.MethodPost, "/api/v1/build?request-id=other-uid", "zip-body", srv.handleBuildPost)
	if lateOther.Code != http.StatusConflict {
		t.Fatalf("different request after completion=%d body=%s", lateOther.Code, lateOther.Body.String())
	}
	if builds.Load() != 1 {
		t.Fatalf("build runs = %d, want 1", builds.Load())
	}
}

func TestHandleBuildPostDoesNotReusePartiallySubstitutedSourceOnRetry(t *testing.T) {
	ownedRoot := t.TempDir()
	imageDir := filepath.Join(ownedRoot, "image")
	if err := os.Mkdir(imageDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(imageDir, "Dockerfile"), []byte("FROM scratch\n# ${KOVA_VALUE}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(imageDir, "metadata.json"), []byte(`{"target":"${KOVA_MISSING}","platform":"linux/amd64"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	var builds atomic.Int32
	srv := testDaemonServer(serverBackend{
		validateBuildArchive: func(string) (int, error) { return 1, nil },
		extractZip:           func(string, string) error { return nil },
		runBuild: func(opts batch.Options) error {
			builds.Add(1)
			if !opts.ImageDirsAlreadyIsolated {
				return errors.New("expected isolated runner source")
			}
			_, _, err := source.LoadBuildSpecsForFormatsInPlace(ownedRoot, "", "", []source.BuildFormat{source.BuildFormatOCI}, map[string]string{"KOVA_VALUE": "ready"})
			return err
		},
	})
	e := echo.New()
	path := "/api/v1/build?request-id=partial-source-error"
	first := performEchoRequest(t, e, http.MethodPost, path, "zip-body", srv.handleBuildPost)
	if first.Code != http.StatusAccepted {
		t.Fatalf("first response=%d body=%s", first.Code, first.Body.String())
	}
	state := waitForState(t, srv, "failed")
	if !strings.Contains(state.Error, "KOVA_MISSING") {
		t.Fatalf("build did not fail during partial source substitution: %#v", state)
	}
	dockerfile, err := os.ReadFile(filepath.Join(imageDir, "Dockerfile"))
	if err != nil || !strings.Contains(string(dockerfile), "# ready") {
		t.Fatalf("test did not reach partial substitution: %q, %v", dockerfile, err)
	}
	retry := performEchoRequest(t, e, http.MethodPost, path, "zip-body", srv.handleBuildPost)
	if retry.Code != http.StatusOK || decodeDaemonState(t, retry).Status != "failed" {
		t.Fatalf("same-ID retry=%d body=%s", retry.Code, retry.Body.String())
	}
	other := performEchoRequest(t, e, http.MethodPost, "/api/v1/build?request-id=different-build", "zip-body", srv.handleBuildPost)
	if other.Code != http.StatusConflict {
		t.Fatalf("different-ID retry=%d body=%s", other.Code, other.Body.String())
	}
	if got := builds.Load(); got != 1 {
		t.Fatalf("partially substituted source ran %d times", got)
	}
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
	if state := decodeDaemonState(t, cancelRec); (state.Status != "cancelling" && state.Status != "cancelled") || state.Error == "" {
		// The asynchronous build can finish cancelling before the handler reads
		// the state for its response. Both states confirm the accepted request.
		t.Fatalf("expected cancelling or cancelled response with a reason, got %#v", state)
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
