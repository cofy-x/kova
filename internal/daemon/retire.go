package daemon

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

const exactBuildRetireCapability = "exact-build-retire-v1"

// retireState confirms a local admission barrier and, separately, whether the
// daemon's accepted build invocation has returned. A locally joined invocation
// is not evidence that a remote BuildKit operation has been settled.
type retireState struct {
	RequestID string      `json:"requestId"`
	Phase     string      `json:"phase"`
	Build     daemonState `json:"build"`
}

func retireRequestID(c echo.Context) (string, bool) {
	ids := c.QueryParams()["request-id"]
	return firstRetireRequestID(ids)
}

func firstRetireRequestID(ids []string) (string, bool) {
	if len(ids) != 1 || ids[0] == "" || len(ids[0]) > 128 {
		return "", false
	}
	return ids[0], true
}

func (s *daemonServer) handleBuildRetirePost(c echo.Context) error {
	requestID, ok := retireRequestID(c)
	if !ok {
		return c.JSON(http.StatusBadRequest, daemonState{Status: "error", Error: "exactly one non-empty request-id of at most 128 bytes is required"})
	}

	s.mu.Lock()
	if s.retiredRequestID != "" && s.retiredRequestID != requestID {
		s.mu.Unlock()
		return c.JSON(http.StatusConflict, daemonState{Status: "error", Error: "runner already retired for another request"})
	}
	if s.retiredRequestID == "" {
		// A runner with an anonymous or different accepted build cannot prove
		// that retiring this request ID contains its effects.
		if s.buildRequestID != "" && s.buildRequestID != requestID ||
			s.buildRequestID == "" && s.build.Status != "idle" {
			s.mu.Unlock()
			return c.JSON(http.StatusConflict, daemonState{Status: "error", Error: "runner belongs to another build"})
		}
		s.retiredRequestID = requestID
		s.retireDone = s.buildDone
		s.retireUnjoinable = buildActive(s.build.Status) && s.buildDone == nil
	}
	if s.build.Status == "running" && s.buildCancel != nil {
		s.buildCancel()
		s.build = daemonState{Status: "cancelling", Error: "retire requested", RequestID: s.buildRequestID}
	}
	state := s.retireStateLocked()
	s.mu.Unlock()

	if state.Phase == "retiring" {
		return c.JSON(http.StatusAccepted, state)
	}
	return c.JSON(http.StatusOK, state)
}

func (s *daemonServer) handleBuildRetireGet(c echo.Context) error {
	requestID, ok := retireRequestID(c)
	if !ok {
		return c.JSON(http.StatusBadRequest, daemonState{Status: "error", Error: "exactly one non-empty request-id of at most 128 bytes is required"})
	}

	s.mu.RLock()
	if s.retiredRequestID == "" {
		s.mu.RUnlock()
		return c.JSON(http.StatusNotFound, daemonState{Status: "error", Error: "retire barrier not installed"})
	}
	if s.retiredRequestID != requestID {
		s.mu.RUnlock()
		return c.JSON(http.StatusConflict, daemonState{Status: "error", Error: "runner retired for another request"})
	}
	state := s.retireStateLocked()
	s.mu.RUnlock()

	if state.Phase == "retiring" {
		return c.JSON(http.StatusAccepted, state)
	}
	return c.JSON(http.StatusOK, state)
}

// Caller holds s.mu for reading or writing.
func (s *daemonServer) retireStateLocked() retireState {
	phase := "locally-joined"
	if s.retireUnjoinable {
		phase = "retiring"
	} else if s.retireDone != nil {
		select {
		case <-s.retireDone:
		default:
			phase = "retiring"
		}
	}
	return retireState{RequestID: s.retiredRequestID, Phase: phase, Build: s.build}
}
