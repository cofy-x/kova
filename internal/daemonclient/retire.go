package daemonclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

const (
	RetirePath                 = "/api/v1/build/retire"
	ExactBuildRetireCapability = "exact-build-retire-v1"
)

// RetireStatus proves only that this daemon has installed a per-process
// admission barrier for the exact request ID. LocalSettled means its accepted
// invocation has returned; RemoteWorkerSettled is always false in v1 and must
// never be inferred from LocalSettled.
type RetireStatus struct {
	RequestID           string
	Phase               string
	LocalSettled        bool
	RemoteWorkerSettled bool
	Build               RetiredBuildStatus
}

type RetiredBuildStatus struct {
	Status    string `json:"status"`
	Error     string `json:"error"`
	RequestID string `json:"requestId"`
}

// RetireBuild installs an idempotent barrier and asks a matching active build
// to cancel. It does not wait for local or remote work to finish.
func (c *Client) RetireBuild(ctx context.Context, requestID string) (RetireStatus, error) {
	return c.requestRetire(ctx, http.MethodPost, requestID)
}

// ReadBuildRetire reads back a previously installed barrier without creating
// one. A 404 is not proof that a lost retirement request never reached a
// different daemon instance or that any remote work has settled.
func (c *Client) ReadBuildRetire(ctx context.Context, requestID string) (RetireStatus, error) {
	return c.requestRetire(ctx, http.MethodGet, requestID)
}

func (c *Client) requestRetire(ctx context.Context, method, requestID string) (RetireStatus, error) {
	if requestID == "" || len(requestID) > 128 {
		return RetireStatus{}, fmt.Errorf("retire requires an exact request ID of 1 to 128 bytes")
	}
	query := url.Values{"request-id": {requestID}}
	var body bytes.Buffer
	if err := c.Do(ctx, method, RetirePath, query, nil, &body); err != nil {
		return RetireStatus{}, fmt.Errorf("%s retire request %q: %w", method, requestID, err)
	}
	var wire struct {
		RequestID           string             `json:"requestId"`
		Phase               string             `json:"phase"`
		LocalSettled        *bool              `json:"localSettled"`
		RemoteWorkerSettled *bool              `json:"remoteWorkerSettled"`
		Build               RetiredBuildStatus `json:"build"`
	}
	if err := json.Unmarshal(body.Bytes(), &wire); err != nil {
		return RetireStatus{}, fmt.Errorf("decode retire response: %w", err)
	}
	if wire.RequestID != requestID || wire.LocalSettled == nil || wire.RemoteWorkerSettled == nil ||
		*wire.RemoteWorkerSettled || wire.Build.RequestID != "" && wire.Build.RequestID != requestID ||
		wire.Build.Status == "" ||
		wire.Phase != "retiring" && wire.Phase != "locally-joined" ||
		*wire.LocalSettled != (wire.Phase == "locally-joined") ||
		*wire.LocalSettled && (wire.Build.Status == "running" || wire.Build.Status == "cancelling") {
		return RetireStatus{}, fmt.Errorf("retire response does not attest the exact local barrier")
	}
	return RetireStatus{
		RequestID:           wire.RequestID,
		Phase:               wire.Phase,
		LocalSettled:        *wire.LocalSettled,
		RemoteWorkerSettled: *wire.RemoteWorkerSettled,
		Build:               wire.Build,
	}, nil
}
