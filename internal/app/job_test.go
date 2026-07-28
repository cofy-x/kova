package app

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cofy-x/kova/internal/ctxconfig"
)

func TestJobListUsesServiceContext(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(`{"jobs":[{"id":"job-1","status":"running","requester":"alice","created_at":"2026-01-01T00:00:00Z"}]}`))
	}))
	defer server.Close()
	path := filepath.Join(t.TempDir(), "config.json")
	if err := ctxconfig.Save(path, ctxconfig.Config{
		Current: "service",
		Contexts: map[string]ctxconfig.Context{
			"service": {Mode: ctxconfig.ModeService, ServiceURL: server.URL},
		},
	}); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KOVA_SERVICE_TOKEN", "test-token")
	app := NewCLIApp()
	var out bytes.Buffer
	app.Writer = &out
	app.ErrWriter = &out
	if err := app.Run([]string{"kova", "--ctx-config", path, "job", "list"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "job-1") || !strings.Contains(out.String(), "alice") {
		t.Fatalf("output = %q", out.String())
	}
}

func TestJobCommandRejectsDirectContext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := ctxconfig.Save(path, ctxconfig.Config{
		Current:  "direct",
		Contexts: map[string]ctxconfig.Context{"direct": {Mode: ctxconfig.ModeDirect}},
	}); err != nil {
		t.Fatal(err)
	}
	app := NewCLIApp()
	app.Writer = &bytes.Buffer{}
	app.ErrWriter = &bytes.Buffer{}
	err := app.Run([]string{"kova", "--ctx-config", path, "job", "list"})
	if err == nil || !strings.Contains(err.Error(), "direct mode") {
		t.Fatalf("expected direct mode error, got %v", err)
	}
}
