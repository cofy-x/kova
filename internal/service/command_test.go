package service

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cofy-x/kova/internal/buildcontract"
	"github.com/cofy-x/kova/internal/service/config"
	"github.com/urfave/cli/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/rest"
)

func serviceFactoryReceiptArgs() []string {
	return []string{"kova-controller", "service", "--admission-genesis-receipt-file=/unused",
		"--admission-genesis-receipt-secret-namespace=control", "--admission-genesis-receipt-secret-name=receipt",
		"--admission-genesis-receipt-secret-uid=original", "--buildkit-platform-addr=linux/amd64=tcp://127.0.0.1:1"}
}

func TestServiceFactoryUsesOnlyInjectedConfigLoaderAfterValidation(t *testing.T) {
	sentinel := errors.New("pinned config unavailable")
	calls := 0
	app := &cli.App{Commands: []*cli.Command{serviceCLICommand(func() (*rest.Config, error) {
		calls++
		return nil, sentinel
	})}}
	if err := app.Run([]string{"kova-controller", "service"}); err == nil || calls != 0 {
		t.Fatal("factory bypassed mandatory receipt validation")
	}
	if err := app.Run(serviceFactoryReceiptArgs()); !errors.Is(err, sentinel) || calls != 1 {
		t.Fatalf("factory loader calls=%d error=%v", calls, err)
	}
}

func TestServiceFactoryPropagatesRunContextCancellation(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	app := &cli.App{Commands: []*cli.Command{serviceCLICommand(func() (*rest.Config, error) {
		return &rest.Config{Host: server.URL}, nil
	})}}
	path := filepath.Join(t.TempDir(), "receipt.json")
	if err := os.WriteFile(path, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	args := serviceFactoryReceiptArgs()
	args[2] = "--admission-genesis-receipt-file=" + path
	// The file is readable, so startup reaches the direct Secret check. The
	// canceled parent must prevent even that first request from reaching a wire.
	if err := app.RunContext(ctx, args); !errors.Is(err, context.Canceled) || requests.Load() != 0 {
		t.Fatalf("canceled Service startup error=%v requests=%d", err, requests.Load())
	}
}

func TestShippedServiceRefusesLegacyAdmissionBeforeKubernetes(t *testing.T) {
	app := &cli.App{Commands: []*cli.Command{CLICommand()}}
	err := app.Run([]string{"kovad", "service"})
	if err == nil || !strings.Contains(err.Error(), "requires an externally installed admission Genesis receipt") {
		t.Fatalf("unreceipted runtime entered Kubernetes startup: %v", err)
	}
	err = app.Run([]string{"kovad", "service", "--admission-genesis-receipt-file=/unused"})
	if err == nil || !strings.Contains(err.Error(), "exact Secret namespace, name, and UID") {
		t.Fatalf("partial receipt was accepted: %v", err)
	}
}

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
		VerificationAttemptTimeout: 10 * time.Second, VerificationWindow: 5 * time.Minute,
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
	withoutVerificationAttempt := valid
	withoutVerificationAttempt.VerificationAttemptTimeout = 0
	if err := validateCapacityConfig(withoutVerificationAttempt); err == nil {
		t.Fatal("expected verification-attempt-timeout=0 to be rejected")
	}
	tooShortVerificationWindow := valid
	tooShortVerificationWindow.VerificationWindow = time.Second
	if err := validateCapacityConfig(tooShortVerificationWindow); err == nil {
		t.Fatal("expected verification-window shorter than attempt timeout to be rejected")
	}
	singleReconciler := valid
	singleReconciler.ControllerConcurrency = 1
	if err := validateCapacityConfig(singleReconciler); err == nil || !strings.Contains(err.Error(), "between 2") {
		t.Fatalf("expected controller-concurrency=1 to be rejected, got %v", err)
	}
	valid.WorkerSlots = 0
	if err := validateCapacityConfig(valid); err == nil {
		t.Fatal("expected worker-slots=0 to be rejected")
	}
}

func TestValidateKubeClientRateLimit(t *testing.T) {
	for _, pair := range [][2]int{{5, 10}, {20, 40}, {100, 200}} {
		if err := validateKubeClientRateLimit(pair[0], pair[1]); err != nil {
			t.Fatalf("qps=%d burst=%d: %v", pair[0], pair[1], err)
		}
	}
	for _, pair := range [][2]int{{0, 10}, {-1, 10}, {101, 200}, {20, 19}, {20, 201}} {
		if err := validateKubeClientRateLimit(pair[0], pair[1]); err == nil {
			t.Fatalf("qps=%d burst=%d unexpectedly accepted", pair[0], pair[1])
		}
	}
}

func TestValidateMetricsBindAddress(t *testing.T) {
	for _, address := range []string{"0", "127.0.0.1:1", "127.0.0.1:8081", "127.0.0.1:65535"} {
		if err := validateMetricsBindAddress(address, ":8080"); err != nil {
			t.Fatalf("address %q rejected: %v", address, err)
		}
	}
	for _, address := range []string{"", ":8081", "0.0.0.0:8081", "localhost:8081", "[::1]:8081", "127.0.0.1:0", "127.0.0.1:65536", "127.0.0.1:not-a-port"} {
		if err := validateMetricsBindAddress(address, ":8080"); err == nil {
			t.Fatalf("unsafe or invalid metrics address %q accepted", address)
		}
	}
	if err := validateMetricsBindAddress("127.0.0.1:8080", ":8080"); err == nil {
		t.Fatal("metrics listener shared the Service HTTP port")
	}
}

func TestKubeClientRateLimitIsSharedAcrossConfigCopies(t *testing.T) {
	config := &rest.Config{}
	leader, readiness, http := configureKubeClientRateLimits(config, 1, 2)
	first := rest.CopyConfig(config)
	second := rest.CopyConfig(config)
	if first.QPS != 1 || first.Burst != 2 || first.RateLimiter != second.RateLimiter {
		t.Fatalf("Kubernetes client configs do not share the configured rate limiter")
	}
	if !first.RateLimiter.TryAccept() || !first.RateLimiter.TryAccept() || second.RateLimiter.TryAccept() {
		t.Fatal("separate config copies did not consume the same two-token burst")
	}
	if leader.QPS != 5 || leader.Burst != 10 || leader.RateLimiter == first.RateLimiter {
		t.Fatal("leader-election API traffic does not have an independent budget")
	}
	if !leader.RateLimiter.TryAccept() {
		t.Fatal("a saturated build-control limiter blocked leader-election traffic")
	}
	if readiness.QPS != 5 || readiness.Burst != 10 || readiness.RateLimiter == first.RateLimiter || readiness.RateLimiter == leader.RateLimiter {
		t.Fatal("readiness probes do not have an independent API budget")
	}
	if !readiness.RateLimiter.TryAccept() {
		t.Fatal("a saturated build-control limiter blocked readiness traffic")
	}
	if http.QPS != 1 || http.Burst != 2 || http.RateLimiter != rest.CopyConfig(http).RateLimiter || http.RateLimiter == first.RateLimiter || http.RateLimiter == readiness.RateLimiter || http.RateLimiter == leader.RateLimiter {
		t.Fatal("HTTP admission does not have an independent Kubernetes API budget")
	}
	if !http.RateLimiter.TryAccept() || !http.RateLimiter.TryAccept() || http.RateLimiter.TryAccept() {
		t.Fatal("HTTP admission client copies do not enforce their shared budget")
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
