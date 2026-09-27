package service

import (
	"strings"
	"testing"
	"time"

	"github.com/cofy-x/kova/internal/buildcontract"
	"github.com/cofy-x/kova/internal/service/config"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestParseNodeSelector(t *testing.T) {
	got, err := parseNodeSelector([]string{"kova.cofy.io/source-node=true", "topology.kubernetes.io/zone=zone-b"})
	if err != nil {
		t.Fatal(err)
	}
	if got["kova.cofy.io/source-node"] != "true" || got["topology.kubernetes.io/zone"] != "zone-b" {
		t.Fatalf("selectors = %#v", got)
	}
}

func TestParseRegistryHosts(t *testing.T) {
	hosts, err := parseRegistryHosts([]string{" Registry.Example:5000 ", "registry.example:5000"})
	if err != nil {
		t.Fatal(err)
	}
	if len(hosts) != 1 || hosts[0] != "registry.example:5000" {
		t.Fatalf("hosts = %#v", hosts)
	}
	if _, err := parseRegistryHosts([]string{"http://registry.example:5000"}); err == nil {
		t.Fatal("expected scheme to be rejected")
	}
}

func TestParseNodeSelectorRejectsInvalidAndDuplicateValues(t *testing.T) {
	for _, values := range [][]string{
		{"missing-value"},
		{"=true"},
		{"not a key=true"},
		{"kova.cofy.io/source-node=true", "kova.cofy.io/source-node=false"},
	} {
		if _, err := parseNodeSelector(values); err == nil {
			t.Fatalf("parseNodeSelector(%#v) succeeded", values)
		}
	}
}

func TestParseNodeSelectorAllowsEmptyLabelValue(t *testing.T) {
	got, err := parseNodeSelector([]string{"kova.cofy.io/source-node="})
	if err != nil {
		t.Fatal(err)
	}
	if value, exists := got["kova.cofy.io/source-node"]; !exists || value != "" {
		t.Fatalf("selectors = %#v", got)
	}
}

func TestRunnerObservabilityEnvUsesRunnerServiceName(t *testing.T) {
	t.Setenv("KOVA_OTEL_ENABLED", "true")
	t.Setenv("OTEL_SERVICE_NAME", "kova-controller")
	t.Setenv("KOVA_RUNNER_OTEL_SERVICE_NAME", "kova-runner")
	env := runnerObservabilityEnv()
	if env["KOVA_OTEL_ENABLED"] != "true" || env["OTEL_SERVICE_NAME"] != "kova-runner" {
		t.Fatalf("runner env = %#v", env)
	}
}

func TestValidateCapacityConfigRejectsUnboundedWorkerSlots(t *testing.T) {
	valid := config.Config{
		MaxActiveJobs: 20, MaxActiveJobsPerRequester: 4, MaxQueuedJobs: 1000, MaxQueuedJobsPerRequester: 100,
		WorkerSlots: 20, ControllerConcurrency: buildcontract.DefaultControllerConcurrency,
		PollRetryWindow: time.Minute, MaxBuildDuration: time.Hour,
	}
	if err := validateCapacityConfig(valid); err != nil {
		t.Fatal(err)
	}
	withoutRetryWindow := valid
	withoutRetryWindow.PollRetryWindow = 0
	if err := validateCapacityConfig(withoutRetryWindow); err == nil {
		t.Fatal("expected poll-retry-window=0 to be rejected")
	}
	withoutBuildLimit := valid
	withoutBuildLimit.MaxBuildDuration = 0
	if err := validateCapacityConfig(withoutBuildLimit); err == nil {
		t.Fatal("expected max-build-duration=0 to be rejected")
	}
	valid.WorkerSlots = 0
	if err := validateCapacityConfig(valid); err == nil {
		t.Fatal("expected worker-slots=0 to be rejected")
	}
}

func TestSourcePodResourceOverridesKeepDiskBudget(t *testing.T) {
	runnerResources, err := parsePodResources(`{"limits":{"ephemeral-storage":"6Gi","memory":"3Gi"},"requests":{"ephemeral-storage":"6Gi","memory":"3Gi"}}`)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateSourcePodBudget(resource.MustParse("5Gi"), runnerResources, corev1.ResourceRequirements{}); err != nil {
		t.Fatal(err)
	}
	if err := validateSourcePodBudget(resource.MustParse("6Gi"), runnerResources, corev1.ResourceRequirements{}); err == nil || !strings.Contains(err.Error(), "runner ephemeral-storage") {
		t.Fatalf("expected runner disk budget rejection, got %v", err)
	}
	fetchResources, err := parsePodResources(`{"limits":{"ephemeral-storage":"256Mi"},"requests":{"ephemeral-storage":"256Mi"}}`)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateSourcePodBudget(resource.MustParse("4Gi"), corev1.ResourceRequirements{}, fetchResources); err == nil || !strings.Contains(err.Error(), "source fetch ephemeral-storage") {
		t.Fatalf("expected fetch disk budget rejection, got %v", err)
	}
	if _, err := parsePodResources(`{"limits":{"memory":"0"}}`); err == nil {
		t.Fatal("expected zero memory limit rejection")
	}
	underReserved, err := parsePodResources(`{"requests":{"ephemeral-storage":"4Gi"}}`)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateSourcePodBudget(resource.MustParse("4Gi"), underReserved, corev1.ResourceRequirements{}); err == nil || !strings.Contains(err.Error(), "request must equal its limit") {
		t.Fatalf("expected under-reserved runner rejection, got %v", err)
	}
}
