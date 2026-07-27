package runner

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	defaultBuildkitServiceAddr = "tcp://kova.kova.svc:9094"
	kovaWorkerNamespace        = "kova"
	kovaWorkerDeployment       = "kova"
	daemonSocket               = "/tmp/kova.sock"
)

type Config struct {
	Kubeconfig                string
	Namespace                 string
	PodName                   string
	WaitTimeout               string
	StateFile                 string
	BuildkitAddr              string
	RunnerImage               string
	RunnerImagePullPolicy     string
	ImagePullSecret           string
	DaemonReadyTimeoutSeconds int
	WaitBuildIntervalSeconds  int
	DaemonPprofServer         string
	DaemonEnv                 map[string]string
}

func DefaultConfig() Config {
	wd, err := os.Getwd()
	if err != nil {
		wd = "."
	}
	return Config{
		Kubeconfig:                strings.TrimSpace(os.Getenv("KOVA_KUBECONFIG")),
		Namespace:                 envDefault("KOVA_NAMESPACE", "default"),
		WaitTimeout:               envDefault("KOVA_WAIT_TIMEOUT", "180s"),
		StateFile:                 filepath.Join(wd, ".kova.state"),
		BuildkitAddr:              envDefault("KOVA_BUILDKIT_ADDR", defaultBuildkitServiceAddr),
		RunnerImage:               strings.TrimSpace(os.Getenv("KOVA_IMAGE")),
		RunnerImagePullPolicy:     envDefault("KOVA_IMAGE_PULL_POLICY", "Always"),
		ImagePullSecret:           envDefault("KOVA_IMAGE_PULL_SECRET", "kova-registry"),
		DaemonReadyTimeoutSeconds: envIntDefault("KOVA_DAEMON_READY_TIMEOUT", 60),
		WaitBuildIntervalSeconds:  envIntDefault("KOVA_STATUS_INTERVAL", 5),
		DaemonPprofServer:         envDefault("KOVA_DAEMON_PPROF_SERVER", "0.0.0.0:6060"),
		DaemonEnv:                 observabilityEnvFromHost("kova-runner"),
	}
}

func observabilityEnvFromHost(defaultServiceName string) map[string]string {
	mappings := map[string]string{
		"KOVA_DAEMON_OTEL_ENABLED":                "KOVA_OTEL_ENABLED",
		"KOVA_DAEMON_OTEL_TRACES_ENABLED":         "KOVA_OTEL_TRACES_ENABLED",
		"KOVA_DAEMON_OTEL_METRICS_ENABLED":        "KOVA_OTEL_METRICS_ENABLED",
		"KOVA_DAEMON_OTEL_LOGS_ENABLED":           "KOVA_OTEL_LOGS_ENABLED",
		"KOVA_DAEMON_OTEL_METRIC_INTERVAL":        "KOVA_OTEL_METRIC_INTERVAL",
		"KOVA_DAEMON_OTEL_EXPORTER_OTLP_ENDPOINT": "OTEL_EXPORTER_OTLP_ENDPOINT",
		"KOVA_DAEMON_OTEL_EXPORTER_OTLP_INSECURE": "OTEL_EXPORTER_OTLP_INSECURE",
		"KOVA_DAEMON_OTEL_RESOURCE_ATTRIBUTES":    "OTEL_RESOURCE_ATTRIBUTES",
		"KOVA_DAEMON_OTEL_SERVICE_NAME":           "OTEL_SERVICE_NAME",
		"KOVA_DAEMON_OTEL_SERVICE_VERSION":        "OTEL_SERVICE_VERSION",
	}
	env := make(map[string]string)
	for hostName, daemonName := range mappings {
		if value := strings.TrimSpace(os.Getenv(hostName)); value != "" {
			env[daemonName] = value
		}
	}
	if len(env) == 0 {
		names := []string{
			"KOVA_OTEL_ENABLED",
			"KOVA_OTEL_TRACES_ENABLED",
			"KOVA_OTEL_METRICS_ENABLED",
			"KOVA_OTEL_LOGS_ENABLED",
			"KOVA_OTEL_METRIC_INTERVAL",
			"OTEL_EXPORTER_OTLP_ENDPOINT",
			"OTEL_EXPORTER_OTLP_INSECURE",
			"OTEL_RESOURCE_ATTRIBUTES",
			"OTEL_SERVICE_VERSION",
		}
		for _, name := range names {
			if value := strings.TrimSpace(os.Getenv(name)); value != "" {
				env[name] = value
			}
		}
		if value := strings.TrimSpace(os.Getenv("OTEL_SERVICE_NAME")); value != "" {
			env["OTEL_SERVICE_NAME"] = value
		}
	}
	if strings.TrimSpace(env["OTEL_SERVICE_NAME"]) == "" && strings.TrimSpace(env["KOVA_OTEL_ENABLED"]) != "" {
		env["OTEL_SERVICE_NAME"] = defaultServiceName
	}
	return env
}

func (c Config) requireKubeconfig() error {
	if strings.TrimSpace(c.Kubeconfig) == "" {
		return fmt.Errorf("--kubeconfig is required")
	}
	return nil
}

func (c Config) requirePodName() error {
	if strings.TrimSpace(c.PodName) == "" {
		return fmt.Errorf("--name is required")
	}
	return nil
}

func (c Config) requireBuildkitAddr() error {
	if strings.TrimSpace(c.BuildkitAddr) == "" {
		return fmt.Errorf("--buildkit-addr is required")
	}
	return nil
}

func envDefault(key, def string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return def
}

func envIntDefault(key string, def int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return def
	}
	var parsed int
	if _, err := fmt.Sscanf(value, "%d", &parsed); err != nil || parsed <= 0 {
		return def
	}
	return parsed
}
