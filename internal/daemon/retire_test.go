package daemon

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cofy-x/kova/internal/batch"
	"github.com/cofy-x/kova/internal/source"
	"github.com/labstack/echo/v4"
)

func decodeRetireState(t *testing.T, rec *httptest.ResponseRecorder) retireState {
	t.Helper()
	var state retireState
	if err := json.Unmarshal(rec.Body.Bytes(), &state); err != nil {
		t.Fatalf("decode retire state: %v; body=%s", err, rec.Body.String())
	}
	return state
}

func waitForLocalJoin(t *testing.T, srv *daemonServer, requestID string) retireState {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		rec := performEchoRequest(t, echo.New(), http.MethodGet, "/api/v1/build/retire?request-id="+requestID, "", srv.handleBuildRetireGet)
		if rec.Code == http.StatusOK {
			state := decodeRetireState(t, rec)
			if state.Phase != "locally-joined" {
				t.Fatalf("joined response has phase %q", state.Phase)
			}
			return state
		}
		if rec.Code != http.StatusAccepted {
			t.Fatalf("retire readback=%d body=%s", rec.Code, rec.Body.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for local build join")
	return retireState{}
}

// retireHeldReadBody lets this direct-handler test distinguish closing the
// admitted Body from the build POST actually returning. The Unix HTTP protocol
// test separately verifies that closing a real stalled upload unblocks it.
type retireHeldReadBody struct {
	reading     chan struct{}
	closed      chan struct{}
	release     chan struct{}
	readOnce    sync.Once
	closeOnce   sync.Once
	releaseOnce sync.Once
}

func newRetireHeldReadBody() *retireHeldReadBody {
	return &retireHeldReadBody{
		reading: make(chan struct{}),
		closed:  make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (b *retireHeldReadBody) Read([]byte) (int, error) {
	b.readOnce.Do(func() { close(b.reading) })
	<-b.release
	return 0, io.ErrClosedPipe
}

func (b *retireHeldReadBody) Close() error {
	b.closeOnce.Do(func() { close(b.closed) })
	return nil
}

func (b *retireHeldReadBody) allowReadReturn() {
	b.releaseOnce.Do(func() { close(b.release) })
}

func TestRetireBeforeBuildAdmissionBlocksEveryLaterPost(t *testing.T) {
	var builds atomic.Int32
	srv := testDaemonServer(serverBackend{runBuild: func(batch.Options) error {
		builds.Add(1)
		return nil
	}})
	e := echo.New()
	path := "/api/v1/build/retire?request-id=exact-request"

	before := performEchoRequest(t, e, http.MethodGet, path, "", srv.handleBuildRetireGet)
	if before.Code != http.StatusNotFound {
		t.Fatalf("uninstalled readback=%d body=%s", before.Code, before.Body.String())
	}
	for _, badPath := range []string{"/api/v1/build/retire", "/api/v1/build/retire?request-id=", "/api/v1/build/retire?request-id=a&request-id=b"} {
		bad := performEchoRequest(t, e, http.MethodPost, badPath, "", srv.handleBuildRetirePost)
		if bad.Code != http.StatusBadRequest {
			t.Fatalf("bad retire %q=%d body=%s", badPath, bad.Code, bad.Body.String())
		}
	}
	installed := performEchoRequest(t, e, http.MethodPost, path, "", srv.handleBuildRetirePost)
	if installed.Code != http.StatusOK {
		t.Fatalf("install=%d body=%s", installed.Code, installed.Body.String())
	}
	if state := decodeRetireState(t, installed); state.RequestID != "exact-request" || state.Phase != "locally-joined" || state.Build.Status != "idle" {
		t.Fatalf("installed state=%#v", state)
	}
	for _, buildPath := range []string{"/api/v1/build?request-id=exact-request", "/api/v1/build?request-id=other-request", "/api/v1/build"} {
		post := performEchoRequest(t, e, http.MethodPost, buildPath, "zip-body", srv.handleBuildPost)
		if post.Code != http.StatusConflict {
			t.Fatalf("build after barrier %q=%d body=%s", buildPath, post.Code, post.Body.String())
		}
	}
	if builds.Load() != 0 {
		t.Fatalf("build ran %d times after retire", builds.Load())
	}
	if repeat := performEchoRequest(t, e, http.MethodPost, path, "", srv.handleBuildRetirePost); repeat.Code != http.StatusOK {
		t.Fatalf("repeat retire=%d body=%s", repeat.Code, repeat.Body.String())
	}
	if readback := performEchoRequest(t, e, http.MethodGet, path, "", srv.handleBuildRetireGet); readback.Code != http.StatusOK {
		t.Fatalf("retire readback=%d body=%s", readback.Code, readback.Body.String())
	}
}

func TestRetireAfterRejectedUploadHasNoStaleBodyOwner(t *testing.T) {
	srv := testDaemonServer(serverBackend{})
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/build?request-id=bad-upload", io.NopCloser(strings.NewReader("short")))
	req.ContentLength = source.MaxArchiveBytes + 1
	rec := httptest.NewRecorder()
	if err := srv.handleBuildPost(e.NewContext(req, rec)); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("rejected upload=%d body=%s", rec.Code, rec.Body.String())
	}
	srv.mu.RLock()
	staleBody := srv.buildBody != nil || srv.buildReadDeadline != nil || srv.buildUploadDone != nil
	srv.mu.RUnlock()
	if staleBody {
		t.Fatal("rejected upload left an owned request Body behind")
	}
	retire := performEchoRequest(t, e, http.MethodPost, "/api/v1/build/retire?request-id=bad-upload", "", srv.handleBuildRetirePost)
	if retire.Code != http.StatusOK || !decodeRetireState(t, retire).LocalSettled {
		t.Fatalf("retire after rejected upload=%d body=%s", retire.Code, retire.Body.String())
	}
}

func TestRetireAcceptedBuildCancelsButJoinsOnlyAfterReturn(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	srv := testDaemonServer(serverBackend{
		validateBuildArchive: func(string) (int, error) { return 1, nil },
		extractZip:           func(string, string) error { return nil },
		runBuild: func(opts batch.Options) error {
			close(started)
			<-opts.Ctx.Done()
			<-release
			return opts.Ctx.Err()
		},
	})
	e := echo.New()
	build := performEchoRequest(t, e, http.MethodPost, "/api/v1/build?request-id=exact-request", "zip-body", srv.handleBuildPost)
	if build.Code != http.StatusAccepted {
		t.Fatalf("build=%d body=%s", build.Code, build.Body.String())
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("build did not start")
	}

	path := "/api/v1/build/retire?request-id=exact-request"
	retire := performEchoRequest(t, e, http.MethodPost, path, "", srv.handleBuildRetirePost)
	if retire.Code != http.StatusAccepted {
		t.Fatalf("retire before join=%d body=%s", retire.Code, retire.Body.String())
	}
	if state := decodeRetireState(t, retire); state.Phase != "retiring" || state.Build.Status != "cancelling" {
		t.Fatalf("retiring state=%#v", state)
	}
	readback := performEchoRequest(t, e, http.MethodGet, path, "", srv.handleBuildRetireGet)
	if readback.Code != http.StatusAccepted {
		t.Fatalf("unjoined readback=%d body=%s", readback.Code, readback.Body.String())
	}
	lateBuild := performEchoRequest(t, e, http.MethodPost, "/api/v1/build?request-id=exact-request", "zip-body", srv.handleBuildPost)
	if lateBuild.Code != http.StatusConflict {
		t.Fatalf("same-ID build after retire=%d body=%s", lateBuild.Code, lateBuild.Body.String())
	}
	close(release)
	state := waitForLocalJoin(t, srv, "exact-request")
	if state.Build.Status != "cancelled" {
		t.Fatalf("joined build state=%#v", state.Build)
	}
}

func TestRetireWaitsForBuildStillUploading(t *testing.T) {
	var builds atomic.Int32
	srv := testDaemonServer(serverBackend{
		validateBuildArchive: func(string) (int, error) { return 1, nil },
		extractZip:           func(string, string) error { return nil },
		runBuild: func(opts batch.Options) error {
			builds.Add(1)
			return opts.Ctx.Err()
		},
	})
	e := echo.New()
	body := newRetireHeldReadBody()
	defer body.allowReadReturn()
	type postResult struct {
		status int
		err    error
	}
	posted := make(chan postResult, 1)
	go func() {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/build?request-id=uploading-request", body)
		rec := httptest.NewRecorder()
		err := srv.handleBuildPost(e.NewContext(req, rec))
		posted <- postResult{status: rec.Code, err: err}
	}()

	select {
	case <-body.reading:
	case <-time.After(time.Second):
		t.Fatal("build did not begin reading its upload")
	}
	srv.mu.RLock()
	admitted := srv.buildRequestID == "uploading-request" && srv.buildDone != nil
	srv.mu.RUnlock()
	if !admitted {
		t.Fatal("build was not admitted before upload completed")
	}

	path := "/api/v1/build/retire?request-id=uploading-request"
	retire := performEchoRequest(t, e, http.MethodPost, path, "", srv.handleBuildRetirePost)
	if retire.Code != http.StatusAccepted || decodeRetireState(t, retire).Phase != "retiring" {
		t.Fatalf("retire during upload=%d body=%s", retire.Code, retire.Body.String())
	}
	select {
	case <-body.closed:
	default:
		t.Fatal("retire did not close the admitted upload Body")
	}
	select {
	case result := <-posted:
		t.Fatalf("build POST returned before upload read was released: %+v", result)
	default:
	}
	if readback := performEchoRequest(t, e, http.MethodGet, path, "", srv.handleBuildRetireGet); readback.Code != http.StatusAccepted || decodeRetireState(t, readback).LocalSettled {
		t.Fatalf("readback during upload=%d body=%s", readback.Code, readback.Body.String())
	}
	body.allowReadReturn()
	select {
	case result := <-posted:
		if result.err != nil || result.status != http.StatusAccepted {
			t.Fatalf("upload completion=%d error=%v", result.status, result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("build POST did not finish uploading")
	}
	state := waitForLocalJoin(t, srv, "uploading-request")
	if state.Build.Status != "cancelled" {
		t.Fatalf("joined uploaded build state=%#v", state.Build)
	}
	if !state.LocalSettled || state.RemoteWorkerSettled || builds.Load() != 0 {
		t.Fatalf("joined state=%#v; runBuild calls=%d", state, builds.Load())
	}
}

func TestRetireRejectsDifferentOrAnonymousBuild(t *testing.T) {
	for _, requestID := range []string{"first-request", ""} {
		t.Run(requestID, func(t *testing.T) {
			release := make(chan struct{})
			srv := testDaemonServer(serverBackend{
				validateBuildArchive: func(string) (int, error) { return 1, nil },
				extractZip:           func(string, string) error { return nil },
				runBuild: func(batch.Options) error {
					<-release
					return nil
				},
			})
			e := echo.New()
			path := "/api/v1/build"
			if requestID != "" {
				path += "?request-id=" + requestID
			}
			build := performEchoRequest(t, e, http.MethodPost, path, "zip-body", srv.handleBuildPost)
			if build.Code != http.StatusAccepted {
				t.Fatalf("build=%d body=%s", build.Code, build.Body.String())
			}
			wrong := performEchoRequest(t, e, http.MethodPost, "/api/v1/build/retire?request-id=other-request", "", srv.handleBuildRetirePost)
			if wrong.Code != http.StatusConflict {
				t.Fatalf("wrong retire=%d body=%s", wrong.Code, wrong.Body.String())
			}
			if srv.retiredRequestID != "" {
				t.Fatalf("wrong retire installed barrier %q", srv.retiredRequestID)
			}
			close(release)
			waitForState(t, srv, "completed")
		})
	}
}

func TestRetireDoesNotRewriteSuccessfulBuildAsCancelled(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	srv := testDaemonServer(serverBackend{
		validateBuildArchive: func(string) (int, error) { return 1, nil },
		extractZip:           func(string, string) error { return nil },
		runBuild: func(batch.Options) error {
			close(started)
			<-release
			return nil
		},
	})
	e := echo.New()
	build := performEchoRequest(t, e, http.MethodPost, "/api/v1/build?request-id=exact-request", "zip-body", srv.handleBuildPost)
	if build.Code != http.StatusAccepted {
		t.Fatalf("build=%d body=%s", build.Code, build.Body.String())
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("build did not start")
	}
	retire := performEchoRequest(t, e, http.MethodPost, "/api/v1/build/retire?request-id=exact-request", "", srv.handleBuildRetirePost)
	if retire.Code != http.StatusAccepted {
		t.Fatalf("retire=%d body=%s", retire.Code, retire.Body.String())
	}
	close(release)
	state := waitForLocalJoin(t, srv, "exact-request")
	if state.Build.Status != "completed" {
		t.Fatalf("successful build was rewritten as %#v", state.Build)
	}
}
