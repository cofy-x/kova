package daemonclient

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

type retireRoundTripFunc func(*http.Request) (*http.Response, error)

func (f retireRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestRetireBuildRequiresExactQualifiedResponse(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		ok   bool
	}{
		{name: "valid", body: `{"requestId":"exact","phase":"locally-joined","localSettled":true,"remoteWorkerSettled":false,"build":{"status":"idle"}}`, ok: true},
		{name: "wrong ID", body: `{"requestId":"other","phase":"locally-joined","localSettled":true,"remoteWorkerSettled":false,"build":{"status":"idle"}}`},
		{name: "missing local settlement", body: `{"requestId":"exact","phase":"locally-joined","remoteWorkerSettled":false,"build":{"status":"idle"}}`},
		{name: "missing remote settlement", body: `{"requestId":"exact","phase":"locally-joined","localSettled":true,"build":{"status":"idle"}}`},
		{name: "remote settled claim", body: `{"requestId":"exact","phase":"locally-joined","localSettled":true,"remoteWorkerSettled":true,"build":{"status":"idle"}}`},
		{name: "phase mismatch", body: `{"requestId":"exact","phase":"retiring","localSettled":true,"remoteWorkerSettled":false,"build":{"status":"running","requestId":"exact"}}`},
		{name: "wrong build ID", body: `{"requestId":"exact","phase":"retiring","localSettled":false,"remoteWorkerSettled":false,"build":{"status":"cancelling","requestId":"other"}}`},
		{name: "empty build", body: `{"requestId":"exact","phase":"locally-joined","localSettled":true,"remoteWorkerSettled":false,"build":{}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &Client{httpClient: &http.Client{Transport: retireRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.Method != http.MethodPost || req.URL.Path != RetirePath || req.URL.Query().Get("request-id") != "exact" {
					t.Errorf("retire request was %s %s", req.Method, req.URL.String())
				}
				return &http.Response{StatusCode: http.StatusAccepted, Body: io.NopCloser(strings.NewReader(tc.body)), Header: make(http.Header)}, nil
			})}}
			_, err := client.RetireBuild(context.Background(), "exact")
			if (err == nil) != tc.ok {
				t.Fatalf("qualified=%t, error=%v", tc.ok, err)
			}
		})
	}
}

func TestRetireBuildRejectsMissingRequestIDBeforeWireCall(t *testing.T) {
	called := false
	client := &Client{httpClient: &http.Client{Transport: retireRoundTripFunc(func(*http.Request) (*http.Response, error) {
		called = true
		return nil, nil
	})}}
	if _, err := client.RetireBuild(context.Background(), ""); err == nil || called {
		t.Fatalf("empty request ID error=%v, called=%t", err, called)
	}
}
