package daemon

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/cofy-x/kova/internal/logging"

	"github.com/labstack/echo/v4"
)

func (s *daemonServer) handleHealth(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string]string{"status": "ok"})
}

func (s *daemonServer) handleBuildStatus(c echo.Context) error {
	return c.JSON(http.StatusOK, s.getBuildState())
}

func (s *daemonServer) handleBuildCancel(c echo.Context) error {
	_, cancelled := s.cancelActiveBuild("cancel requested")
	if !cancelled {
		return c.JSON(http.StatusOK, s.getBuildState())
	}

	logging.Infof("Cancelling current build on request from %s", c.RealIP())
	return c.JSON(http.StatusAccepted, s.getBuildState())
}

// ---------------- /api/v1/build ----------------

func (s *daemonServer) handleBuildPost(c echo.Context) error {
	logging.ResetCommandStartTime(time.Now())

	s.mu.Lock()
	if buildActive(s.build.Status) {
		s.mu.Unlock()
		logging.Errorf("Rejected build request: another build is already running")
		return c.JSON(http.StatusConflict, daemonState{Status: "error", Error: "a build is already running"})
	}
	s.build = daemonState{Status: "running"}
	done := make(chan struct{})
	buildCtx, buildCancel := context.WithCancel(context.Background())
	s.buildCancel = buildCancel
	s.buildDone = done
	s.mu.Unlock()

	logging.Infof("Accepted build request from %s with query %q", c.RealIP(), c.QueryString())

	// Receive zip body → temp file
	tmpZip, err := os.CreateTemp("", "kova-upload-*.zip")
	if err != nil {
		logging.Errorf("Build request: create temp zip failed: %v", err)
		s.setBuildState(daemonState{Status: "failed", Error: err.Error()})
		buildCancel()
		close(done)
		s.clearBuildExecution(done)
		return c.JSON(http.StatusInternalServerError, s.getBuildState())
	}
	bytesWritten, err := io.Copy(tmpZip, c.Request().Body)
	if err != nil {
		tmpZip.Close()
		os.Remove(tmpZip.Name())
		logging.Errorf("Build request: receive zip failed: %v", err)
		s.setBuildState(daemonState{Status: "failed", Error: "receive zip: " + err.Error()})
		buildCancel()
		close(done)
		s.clearBuildExecution(done)
		return c.JSON(http.StatusInternalServerError, s.getBuildState())
	}
	if err := tmpZip.Close(); err != nil {
		os.Remove(tmpZip.Name())
		logging.Errorf("Build request: close temp zip failed: %v", err)
		s.setBuildState(daemonState{Status: "failed", Error: "close temp zip: " + err.Error()})
		buildCancel()
		close(done)
		s.clearBuildExecution(done)
		return c.JSON(http.StatusInternalServerError, s.getBuildState())
	}
	logging.Infof("Build request body stored at %s (%d bytes)", tmpZip.Name(), bytesWritten)

	q := c.QueryParams()
	if _, err := buildOptionsFromQuery(q, s.defaultAddrs, s.resultDB, s.logsFile); err != nil {
		logging.Errorf("Build request: invalid query %q: %v", c.QueryString(), err)
		s.setBuildState(daemonState{Status: "failed", Error: err.Error()})
		buildCancel()
		close(done)
		s.clearBuildExecution(done)
		os.Remove(tmpZip.Name())
		return c.JSON(http.StatusBadRequest, s.getBuildState())
	}
	imageDirCount, err := s.backend.validateBuildArchive(tmpZip.Name())
	if err != nil {
		logging.Errorf("Build request: invalid zip layout in %s: %v", tmpZip.Name(), err)
		s.setBuildState(daemonState{Status: "failed", Error: err.Error()})
		buildCancel()
		close(done)
		s.clearBuildExecution(done)
		os.Remove(tmpZip.Name())
		return c.JSON(http.StatusBadRequest, s.getBuildState())
	}
	logging.Infof("Build request zip layout validated: %d image directorie(s)", imageDirCount)

	logging.Infof("Build request accepted for async processing: zip=%s", tmpZip.Name())
	go s.runBuildAsync(buildCtx, tmpZip.Name(), q, done)
	return c.JSON(http.StatusAccepted, daemonState{Status: "running"})
}

func (s *daemonServer) runBuildAsync(buildCtx context.Context, zipPath string, q url.Values, done chan struct{}) {
	defer close(done)
	defer s.clearBuildExecution(done)
	defer os.Remove(zipPath)
	defer func() {
		_ = os.RemoveAll(daemonImageDir)
	}()
	logging.Infof("Async build started: zip=%s", zipPath)

	opts, err := buildOptionsFromQuery(q, s.defaultAddrs, s.resultDB, s.logsFile)
	if err != nil {
		logging.Errorf("Async build: parse batch.Options failed: %v", err)
		s.setBuildState(daemonState{Status: "failed", Error: err.Error()})
		return
	}
	logging.Infof("Async build batch.Options resolved: addrs=%d concurrency=%d timeout=%ds retry=%d verbose=%t format=%q target=%q skip-fail=%t",
		len(opts.Addrs), opts.Concurrency, opts.Timeout, opts.Retry, opts.Verbose, opts.BuildFormat, opts.Target, opts.SkipFail)
	opts.Ctx = buildCtx

	_ = os.RemoveAll(daemonImageDir)
	if err := s.backend.extractZip(zipPath, daemonImageDir); err != nil {
		logging.Errorf("Async build: extract zip %s to %s failed: %v", zipPath, daemonImageDir, err)
		s.setBuildState(daemonState{Status: "failed", Error: "extract zip: " + err.Error()})
		return
	}
	logging.Infof("Async build extracted zip to %s", daemonImageDir)
	opts.ImageDirs = daemonImageDir

	logging.Infof("Async build entering batch.RunBuild")
	if err := s.backend.runBuild(opts); err != nil {
		logging.Errorf("Async build failed: %v", err)
		if errors.Is(buildCtx.Err(), context.Canceled) {
			s.setBuildState(daemonState{Status: "cancelled", Error: "build cancelled"})
			return
		}
		s.setBuildState(daemonState{Status: "failed", Error: err.Error()})
		return
	}
	if errors.Is(buildCtx.Err(), context.Canceled) {
		logging.Infof("Async build cancelled")
		s.setBuildState(daemonState{Status: "cancelled", Error: "build cancelled"})
		return
	}

	logging.Infof("Async build completed successfully")
	s.setBuildState(daemonState{Status: "completed"})
}

// ---------------- /api/v1/export ----------------

func (s *daemonServer) handleExport(c echo.Context) error {
	q := c.QueryParams()
	opts, err := exportOptionsFromQuery(q, s.resultDB)
	if err != nil {
		return c.JSON(http.StatusBadRequest, daemonState{Status: "error", Error: err.Error()})
	}

	tmpOut, err := os.CreateTemp("", "kova-export-*.jsonl")
	if err != nil {
		return c.JSON(http.StatusInternalServerError, daemonState{Status: "error", Error: err.Error()})
	}
	tmpPath := tmpOut.Name()
	if err := tmpOut.Close(); err != nil {
		os.Remove(tmpPath)
		return c.JSON(http.StatusInternalServerError, daemonState{Status: "error", Error: err.Error()})
	}
	defer os.Remove(tmpPath)

	opts.ResultPath = tmpPath

	if err := s.backend.runExport(opts); err != nil {
		return c.JSON(http.StatusInternalServerError, daemonState{Status: "error", Error: err.Error()})
	}

	f, err := os.Open(tmpPath)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, daemonState{Status: "error", Error: err.Error()})
	}
	defer f.Close()

	return c.Stream(http.StatusOK, "application/x-ndjson", f)
}

// ---------------- /api/v1/preheat ----------------

func (s *daemonServer) handlePreheat(c echo.Context) error {
	q := c.QueryParams()
	opts, err := preheatOptionsFromQuery(q, s.resultDB)
	if err != nil {
		return c.JSON(http.StatusBadRequest, daemonState{Status: "error", Error: err.Error()})
	}
	opts.Ctx = c.Request().Context()

	if err := s.backend.runPreheat(opts); err != nil {
		return c.JSON(http.StatusInternalServerError, daemonState{Status: "error", Error: err.Error()})
	}

	return c.JSON(http.StatusOK, daemonState{Status: "completed"})
}

// ---------------- helpers ----------------

func (s *daemonServer) setBuildState(st daemonState) {
	s.mu.Lock()
	s.build = st
	s.mu.Unlock()
	if st.Error != "" {
		logging.Errorf("Build state changed to %s: %s", st.Status, st.Error)
		return
	}
	logging.Infof("Build state changed to %s", st.Status)
}

func (s *daemonServer) clearBuildExecution(done chan struct{}) {
	s.mu.Lock()
	if s.buildDone == done {
		s.buildDone = nil
		s.buildCancel = nil
	}
	s.mu.Unlock()
}

func (s *daemonServer) getBuildState() daemonState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.build
}

func (s *daemonServer) cancelActiveBuild(reason string) (chan struct{}, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !buildActive(s.build.Status) || s.buildCancel == nil {
		return s.buildDone, false
	}
	s.buildCancel()
	s.build = daemonState{Status: "cancelling", Error: reason}
	return s.buildDone, true
}

func buildActive(status string) bool {
	return status == "running" || status == "cancelling"
}
