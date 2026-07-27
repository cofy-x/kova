package runner

import "testing"

func TestRequireBuildkitAddr(t *testing.T) {
	if err := (Config{BuildkitAddr: " tcp://buildkit.example:9094 "}).requireBuildkitAddr(); err != nil {
		t.Fatalf("expected buildkit addr to be accepted: %v", err)
	}
	if err := (Config{BuildkitAddr: " \t "}).requireBuildkitAddr(); err == nil {
		t.Fatal("expected empty buildkit addr error")
	}
}

func TestDefaultConfigReadsKubeconfigEnv(t *testing.T) {
	t.Setenv("KOVA_KUBECONFIG", "/tmp/kova.kubeconfig")
	if got := DefaultConfig().Kubeconfig; got != "/tmp/kova.kubeconfig" {
		t.Fatalf("Kubeconfig = %q, want /tmp/kova.kubeconfig", got)
	}
}

func TestObservabilityEnvFromDaemonEnv(t *testing.T) {
	t.Setenv("KOVA_DAEMON_OTEL_ENABLED", "true")
	t.Setenv("KOVA_DAEMON_OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector.kova.svc:4317")
	t.Setenv("KOVA_DAEMON_OTEL_SERVICE_NAME", "custom-runner")
	t.Setenv("KOVA_OTEL_ENABLED", "false")
	env := observabilityEnvFromHost("kova-runner")
	if env["KOVA_OTEL_ENABLED"] != "true" {
		t.Fatalf("KOVA_OTEL_ENABLED = %q, want true", env["KOVA_OTEL_ENABLED"])
	}
	if env["OTEL_EXPORTER_OTLP_ENDPOINT"] != "otel-collector.kova.svc:4317" {
		t.Fatalf("OTEL_EXPORTER_OTLP_ENDPOINT = %q", env["OTEL_EXPORTER_OTLP_ENDPOINT"])
	}
	if env["OTEL_SERVICE_NAME"] != "custom-runner" {
		t.Fatalf("OTEL_SERVICE_NAME = %q, want custom-runner", env["OTEL_SERVICE_NAME"])
	}
}
