package httpapi

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"syscall"
	"testing"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestServiceHTTPTimeoutsConfigured(t *testing.T) {
	httpSrv := newTestServer(t, &fakeKube{}).httpServer()
	if httpSrv.ReadHeaderTimeout != 5*time.Second || httpSrv.ReadTimeout != 30*time.Second || httpSrv.IdleTimeout != time.Minute {
		t.Fatalf("read header=%s read=%s idle=%s", httpSrv.ReadHeaderTimeout, httpSrv.ReadTimeout, httpSrv.IdleTimeout)
	}
	if httpSrv.WriteTimeout != 0 {
		t.Fatalf("write timeout=%s, want unset until handler operations have a separate deadline contract", httpSrv.WriteTimeout)
	}
}

func TestServiceHTTPClosesSlowIncompleteHeaders(t *testing.T) {
	httpSrv := newTestServer(t, &fakeKube{}).httpServer()
	httpSrv.ReadHeaderTimeout = 150 * time.Millisecond
	httpSrv.ReadTimeout = time.Second
	addr := serveServiceTCP(t, httpSrv)
	conn := dialServiceTCP(t, addr)
	defer conn.Close()
	if _, err := io.WriteString(conn, "GET /healthz HTTP/1.1\r\nHost: localhost\r\nX-Stalled: "); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodGet})
	if err == nil {
		if response.StatusCode != http.StatusRequestTimeout {
			t.Fatalf("status=%d, want 408 for incomplete headers", response.StatusCode)
		}
		if _, err := io.Copy(io.Discard, response.Body); err != nil {
			t.Fatal(err)
		}
		if err := response.Body.Close(); err != nil {
			t.Fatal(err)
		}
	} else if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("incomplete header did not time out as a closed connection or 408: %v", err)
	}
	assertServiceTCPClosed(t, reader)
}

func TestServiceHTTPClosesTrickledBodyWithoutAdmittingBuild(t *testing.T) {
	srv := newTestServer(t, &fakeKube{})
	httpSrv := srv.httpServer()
	httpSrv.ReadHeaderTimeout = time.Second
	httpSrv.ReadTimeout = 300 * time.Millisecond
	addr := serveServiceTCP(t, httpSrv)
	conn := dialServiceTCP(t, addr)
	defer conn.Close()
	if _, err := io.WriteString(conn, "POST /v1/builds HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer token\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n "); err != nil {
		t.Fatal(err)
	}
	// Progress every 50 ms, but never complete the declared body. A total
	// read deadline must still expire; an idle-only deadline would not.
	for attempt := 0; attempt < 10; attempt++ {
		time.Sleep(50 * time.Millisecond)
		if _, err := io.WriteString(conn, " "); err != nil {
			break
		}
	}
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	response := readServiceTCPResponse(t, reader, http.MethodPost)
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d, want 400 for incomplete request body", response.StatusCode)
	}
	assertServiceTCPClosed(t, reader)
	var builds kovav1.KovaBuildList
	if err := srv.reader.List(context.Background(), &builds, client.InNamespace(srv.cfg.Namespace)); err != nil {
		t.Fatal(err)
	}
	if len(builds.Items) != 0 {
		t.Fatalf("incomplete request admitted %d builds", len(builds.Items))
	}
}

func TestServiceHTTPValidRequestsAndIdleConnection(t *testing.T) {
	srv := newTestServer(t, &fakeKube{})
	httpSrv := srv.httpServer()
	httpSrv.ReadHeaderTimeout = time.Second
	httpSrv.ReadTimeout = time.Second
	httpSrv.IdleTimeout = 150 * time.Millisecond
	addr := serveServiceTCP(t, httpSrv)
	conn := dialServiceTCP(t, addr)
	defer conn.Close()
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	for attempt := 0; attempt < 2; attempt++ {
		if _, err := io.WriteString(conn, "GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n"); err != nil {
			t.Fatal(err)
		}
		response := readServiceTCPResponse(t, reader, http.MethodGet)
		if response.StatusCode != http.StatusOK {
			t.Fatalf("health attempt %d status=%d", attempt, response.StatusCode)
		}
	}
	assertServiceTCPClosed(t, reader)

	request := multipartBuildRequest(t, map[string]string{"format": "oci", "target": "registry.local/example:dev"})
	request.URL.Scheme = "http"
	request.URL.Host = addr
	request.Host = addr
	request.RequestURI = ""
	request.Header.Set("Authorization", "Bearer token")
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("complete build request status=%d body=%s", response.StatusCode, body)
	}
}

func serveServiceTCP(t *testing.T, httpSrv *http.Server) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- httpSrv.Serve(listener) }()
	t.Cleanup(func() {
		if err := httpSrv.Close(); err != nil {
			t.Errorf("close service: %v", err)
		}
		if err := <-done; !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("serve service: %v", err)
		}
	})
	return listener.Addr().String()
}

func dialServiceTCP(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

func readServiceTCPResponse(t *testing.T, reader *bufio.Reader, method string) *http.Response {
	t.Helper()
	response, err := http.ReadResponse(reader, &http.Request{Method: method})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(io.Discard, response.Body); err != nil {
		t.Fatal(err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
	return response
}

func assertServiceTCPClosed(t *testing.T, reader *bufio.Reader) {
	t.Helper()
	// An idle socket uses its prior read deadline; a stalled socket sets one
	// before this check. Unexpected timeout is a failure, not a close.
	if _, err := reader.ReadByte(); !errors.Is(err, io.EOF) && !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("connection remained readable or timed out instead of closing: %v", err)
	}
}
