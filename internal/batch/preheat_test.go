package batch

import (
	"testing"

	"github.com/cofy-x/kova/internal/store"
)

func TestBuildPreheatURL(t *testing.T) {
	tests := []struct {
		name   string
		target string
		want   string
	}{
		{
			name:   "tag",
			target: "registry.example.com/ns/image:dev",
			want:   "https://registry.example.com/v2/ns/image/manifests/dev",
		},
		{
			name:   "digest",
			target: "registry.example.com/ns/image@sha256:abc123",
			want:   "https://registry.example.com/v2/ns/image/manifests/sha256:abc123",
		},
		{
			name:   "docker prefix",
			target: "docker://localhost:5001/ns/image:dev",
			want:   "http://localhost:5001/v2/ns/image/manifests/dev",
		},
		{
			name:   "kind host registry",
			target: "host.docker.internal:5001/ns/image:dev",
			want:   "http://host.docker.internal:5001/v2/ns/image/manifests/dev",
		},
		{
			name:   "http url passthrough",
			target: "http://registry.example.com/v2/ns/image/manifests/dev",
			want:   "http://registry.example.com/v2/ns/image/manifests/dev",
		},
		{
			name:   "default latest",
			target: "registry.example.com/ns/image",
			want:   "https://registry.example.com/v2/ns/image/manifests/latest",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := buildPreheatURL(tt.target)
			if err != nil {
				t.Fatalf("expected success, got %v", err)
			}
			if got != tt.want {
				t.Fatalf("unexpected url: want %q got %q", tt.want, got)
			}
		})
	}
}

func TestBuildPreheatURLRejectsInvalidTargets(t *testing.T) {
	for _, target := range []string{"", "busybox:latest", "registry.example.com/"} {
		t.Run(target, func(t *testing.T) {
			if _, err := buildPreheatURL(target); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestPreheatRequestBodyConfiguresTLSVerification(t *testing.T) {
	body := preheatRequestBody("https://registry.example.com/v2/ns/image/manifests/dev", "user", "pass", false)
	if got := body["insecureSkipVerify"]; got != false {
		t.Fatalf("insecureSkipVerify = %#v, want false", got)
	}
	if body["username"] != "user" || body["password"] != "pass" {
		t.Fatalf("unexpected registry credentials: %#v", body)
	}
}

func TestFilterPreheatTargetsRequiresExactSuccessfulModeMatch(t *testing.T) {
	entries := []store.Entry{
		{Target: "registry.example.com/ns/oci:dev", Success: true},
		{Target: "registry.example.com/ns/image:dev-nydus", Success: true},
		{Target: "registry.example.com/ns/failed:dev", Success: false},
	}

	targets := filterPreheatTargets(entries, "registry.example.com/ns/oci:dev", true)
	if len(targets) != 1 || targets[0].Target != "registry.example.com/ns/oci:dev" {
		t.Fatalf("unexpected targets: %#v", targets)
	}
	if targets := filterPreheatTargets(entries, "registry.example.com/ns/missing:dev", true); len(targets) != 0 {
		t.Fatalf("expected no targets, got %#v", targets)
	}
}
