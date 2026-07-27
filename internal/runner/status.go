package runner

import (
	"encoding/json"
	"fmt"
	"time"
)

type BuildState struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

func ParseBuildState(raw []byte) (BuildState, error) {
	var state BuildState
	if err := json.Unmarshal(raw, &state); err != nil {
		return BuildState{}, err
	}
	return state, nil
}

func WaitDecision(status string) (done bool, success bool, err error) {
	switch status {
	case "running", "cancelling":
		return false, false, nil
	case "completed":
		return true, true, nil
	case "failed", "error", "cancelled":
		return true, false, nil
	case "idle":
		return true, false, fmt.Errorf("no build is running; wait requires an active or completed build")
	case "":
		return true, false, fmt.Errorf("failed to parse build status from daemon response")
	default:
		return true, false, fmt.Errorf("unknown build status: %s", status)
	}
}

func deadlineExceeded(start time.Time, timeoutSeconds int) bool {
	return timeoutSeconds > 0 && int(time.Since(start).Seconds()) >= timeoutSeconds
}
