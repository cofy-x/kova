package service

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/buildcontract"
	"github.com/cofy-x/kova/internal/kube"
	"github.com/cofy-x/kova/internal/logging"
	"github.com/cofy-x/kova/internal/runner"
	serviceauth "github.com/cofy-x/kova/internal/service/auth"
	"github.com/cofy-x/kova/internal/service/buildcontroller"
	"github.com/cofy-x/kova/internal/service/config"
	"github.com/cofy-x/kova/internal/service/httpapi"
	"github.com/cofy-x/kova/internal/source"

	"github.com/urfave/cli/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	ctrlzap "sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

func CLICommand() *cli.Command {
	defaults := runner.DefaultConfig()
	return &cli.Command{
		Name:  "service",
		Usage: "start the Kova service HTTP gateway for runner-backed builds",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "listen", Value: ":8080", Usage: "HTTP listen address"},
			&cli.StringFlag{Name: "namespace", Value: defaults.Namespace, Usage: "Kubernetes namespace for runner Pods"},
			&cli.StringFlag{Name: "runner-image", EnvVars: []string{"KOVA_RUNNER_IMAGE"}, Usage: "runner image used for created Pods"},
			&cli.StringFlag{Name: "runner-image-pull-policy", Value: defaults.RunnerImagePullPolicy, Usage: "runner image pull policy"},
			&cli.StringFlag{Name: "runner-image-pull-secret", Value: defaults.ImagePullSecret, Usage: "runner image pull secret name"},
			&cli.StringSliceFlag{Name: "runner-node-selector", Usage: "node selector for runner Pods; repeatable key=value"},
			&cli.StringFlag{Name: "runner-resources", Usage: "JSON resource requests and limits for runner Pods"},
			&cli.StringFlag{Name: "source-fetch-resources", Usage: "JSON resource requests and limits for source fetch init containers"},
			&cli.StringFlag{Name: "source-volume-size-limit", Value: "4Gi", Usage: "job-local source and runner temporary volume size limit (at least 4Gi)"},
			&cli.StringSliceFlag{Name: "registry-plain-http", EnvVars: []string{"KOVA_SERVICE_REGISTRY_PLAIN_HTTP"}, Usage: "output registry host that uses plain HTTP; repeatable and intended for development"},
			&cli.StringSliceFlag{Name: "buildkit-platform-addr", Usage: "BuildKit worker pool as platform=address; repeatable"},
			&cli.DurationFlag{Name: "job-ttl", Value: 2 * time.Hour, Usage: "duration to retain terminal jobs before cleanup"},
			&cli.StringFlag{Name: "auth-mode", Value: serviceauth.ModeTokenReview, EnvVars: []string{"KOVA_SERVICE_AUTH_MODE"}, Usage: "API authentication mode: tokenreview, static, or unsafe-none"},
			&cli.StringFlag{Name: "auth-token", EnvVars: []string{"KOVA_SERVICE_AUTH_TOKEN"}, Usage: "bearer token required by static authentication"},
			&cli.StringFlag{Name: "auth-static-principal", Value: "kova:static", EnvVars: []string{"KOVA_SERVICE_AUTH_STATIC_PRINCIPAL"}, Usage: "Kubernetes username represented by the static token"},
			&cli.DurationFlag{Name: "wait", Value: 3 * time.Minute, Usage: "timeout for runner Pod readiness"},
			&cli.DurationFlag{Name: "poll-interval", Value: 5 * time.Second, Usage: "build status polling interval"},
			&cli.DurationFlag{Name: "poll-retry-window", Value: time.Minute, Usage: "maximum time to retry runner status errors while the Pod remains available"},
			&cli.DurationFlag{Name: "max-build-duration", Value: 2 * time.Hour, Usage: "maximum time from runner admission to terminal build status"},
			&cli.DurationFlag{Name: "verification-attempt-timeout", Value: 10 * time.Second, Usage: "maximum time for one result export or registry verification round"},
			&cli.DurationFlag{Name: "verification-window", Value: 5 * time.Minute, Usage: "overall result verification window after runner completion"},
			&cli.IntFlag{Name: "max-active-jobs", Value: 20, Usage: "maximum concurrently active service jobs"},
			&cli.IntFlag{Name: "max-active-jobs-per-requester", Value: 4, Usage: "maximum concurrently active jobs for one authenticated requester"},
			&cli.IntFlag{Name: "max-queued-jobs", Value: 1000, Usage: "maximum globally queued HTTP service jobs (1-1000)"},
			&cli.IntFlag{Name: "max-queued-jobs-per-requester", Value: 100, Usage: "maximum queued jobs for one authenticated requester"},
			&cli.IntFlag{Name: "worker-slots", Value: 20, Usage: "total build slots shared fairly across active jobs"},
			&cli.IntFlag{Name: "controller-concurrency", Value: buildcontract.DefaultControllerConcurrency, Usage: "maximum concurrent KovaBuild reconciliations"},
			&cli.IntFlag{Name: "kube-client-qps", Value: 20, Usage: "shared per-Service-Pod Kubernetes API QPS budget (1-100)"},
			&cli.IntFlag{Name: "kube-client-burst", Value: 40, Usage: "shared per-Service-Pod Kubernetes API burst budget (at least QPS, at most 200)"},
			&cli.BoolFlag{Name: "leader-elect", Value: true, Usage: "enable controller-runtime leader election"},
			&cli.StringFlag{Name: "leader-election-namespace", Usage: "namespace used for controller leader election leases; defaults to --namespace"},
		},
		Action: func(c *cli.Context) error {
			ctrl.SetLogger(ctrlzap.New(ctrlzap.UseDevMode(false), ctrlzap.WriteTo(os.Stderr)))
			if err := validateKubeClientRateLimit(c.Int("kube-client-qps"), c.Int("kube-client-burst")); err != nil {
				return err
			}
			runnerNodeSelector, err := parseNodeSelector(c.StringSlice("runner-node-selector"))
			if err != nil {
				return err
			}
			platformAddrs, err := buildcontract.ParsePlatformAddresses(c.StringSlice("buildkit-platform-addr"))
			if err != nil {
				return err
			}
			plainHTTPRegistries, err := parseRegistryHosts(c.StringSlice("registry-plain-http"))
			if err != nil {
				return err
			}
			runnerResources, err := parsePodResources(c.String("runner-resources"))
			if err != nil {
				return fmt.Errorf("--runner-resources: %w", err)
			}
			sourceFetchResources, err := parsePodResources(c.String("source-fetch-resources"))
			if err != nil {
				return fmt.Errorf("--source-fetch-resources: %w", err)
			}
			sourceVolumeSizeLimit, err := resource.ParseQuantity(c.String("source-volume-size-limit"))
			if err != nil || sourceVolumeSizeLimit.Cmp(runner.DefaultSourceVolumeSizeLimit()) < 0 {
				return fmt.Errorf("--source-volume-size-limit must be at least 4Gi")
			}
			if err := validateSourcePodBudget(sourceVolumeSizeLimit, runnerResources, sourceFetchResources); err != nil {
				return err
			}
			restConfig, err := rest.InClusterConfig()
			if err != nil {
				return err
			}
			restConfig = singleAttemptWrites(restConfig)
			// Lease renewal must not queue behind a saturated build-control
			// client. Keep its small independent budget before assigning the
			// shared hot-path limiter to the other clients.
			leaderConfig := configureKubeClientRateLimits(restConfig, c.Int("kube-client-qps"), c.Int("kube-client-burst"))
			kubeClient, err := kube.NewClientForConfig(restConfig)
			if err != nil {
				return err
			}
			clientset, err := kubernetes.NewForConfig(restConfig)
			if err != nil {
				return err
			}
			scheme := runtime.NewScheme()
			utilruntime.Must(corev1.AddToScheme(scheme))
			utilruntime.Must(kovav1.AddToScheme(scheme))
			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			cfg := config.Config{
				Listen:                     c.String("listen"),
				Namespace:                  c.String("namespace"),
				RunnerImage:                strings.TrimSpace(c.String("runner-image")),
				RunnerImagePullPolicy:      c.String("runner-image-pull-policy"),
				RunnerImagePullSecret:      c.String("runner-image-pull-secret"),
				RunnerNodeSelector:         runnerNodeSelector,
				RunnerEnv:                  runnerObservabilityEnv(),
				RunnerResources:            runnerResources,
				SourceFetchResources:       sourceFetchResources,
				SourceVolumeSizeLimit:      &sourceVolumeSizeLimit,
				RegistryPlainHTTP:          plainHTTPRegistries,
				BuildkitPlatformAddrs:      platformAddrs,
				JobTTL:                     c.Duration("job-ttl"),
				AuthToken:                  c.String("auth-token"),
				AuthMode:                   strings.TrimSpace(c.String("auth-mode")),
				AuthStaticPrincipal:        strings.TrimSpace(c.String("auth-static-principal")),
				WaitTimeout:                c.Duration("wait"),
				PollInterval:               c.Duration("poll-interval"),
				PollRetryWindow:            c.Duration("poll-retry-window"),
				MaxBuildDuration:           c.Duration("max-build-duration"),
				VerificationAttemptTimeout: c.Duration("verification-attempt-timeout"),
				VerificationWindow:         c.Duration("verification-window"),
				MaxActiveJobs:              c.Int("max-active-jobs"),
				MaxActiveJobsPerRequester:  c.Int("max-active-jobs-per-requester"),
				MaxQueuedJobs:              c.Int("max-queued-jobs"),
				MaxQueuedJobsPerRequester:  c.Int("max-queued-jobs-per-requester"),
				WorkerSlots:                c.Int("worker-slots"),
				ControllerConcurrency:      c.Int("controller-concurrency"),
			}
			if err := validateCapacityConfig(cfg); err != nil {
				return err
			}
			authenticator, err := serviceauth.New(cfg.AuthMode, cfg.AuthToken, cfg.AuthStaticPrincipal, clientset.AuthenticationV1().TokenReviews())
			if err != nil {
				return err
			}
			var authorizer serviceauth.Authorizer = serviceauth.AllowAllAuthorizer{}
			if cfg.AuthMode != serviceauth.ModeUnsafeNone {
				authorizer, err = serviceauth.NewSubjectAccessReviewAuthorizer(clientset.AuthorizationV1().SubjectAccessReviews())
				if err != nil {
					return err
				}
			}
			mgr, err := ctrl.NewManager(restConfig, ctrl.Options{
				Scheme:                  scheme,
				Cache:                   cache.Options{DefaultNamespaces: map[string]cache.Config{cfg.Namespace: {}}},
				Metrics:                 metricsserver.Options{BindAddress: "0"},
				LeaderElection:          c.Bool("leader-elect"),
				LeaderElectionConfig:    leaderConfig,
				LeaderElectionID:        "kova-service.kova.cofy.dev",
				LeaderElectionNamespace: leaderElectionNamespace(c, cfg.Namespace),
			})
			if err != nil {
				return err
			}
			if err := (&buildcontroller.KovaBuildReconciler{
				Client:    mgr.GetClient(),
				APIReader: mgr.GetAPIReader(),
				Scheme:    mgr.GetScheme(),
				Kube:      kubeClient,
				Cfg:       cfg,
				Recorder:  mgr.GetEventRecorderFor("kova-service"),
			}).SetupWithManager(mgr); err != nil {
				return err
			}
			go func() {
				if err := httpapi.NewServer(cfg, kubeClient, mgr.GetClient(), mgr.GetAPIReader(), authenticator, authorizer).Start(ctx); err != nil {
					logging.Errorf("Kova Service HTTP server stopped: %v", err)
					stop()
				}
			}()
			return mgr.Start(ctx)
		},
	}
}

func validateKubeClientRateLimit(qps, burst int) error {
	if qps < 1 || qps > 100 {
		return fmt.Errorf("kube-client-qps must be between 1 and 100")
	}
	if burst < qps || burst > 200 {
		return fmt.Errorf("kube-client-burst must be between kube-client-qps and 200")
	}
	return nil
}

func configureKubeClientRateLimit(config *rest.Config, qps, burst int) {
	config.QPS = float32(qps)
	config.Burst = burst
	// Reuse one limiter across kube.Client, auth clientset, and the controller
	// manager. Separate clientsets silently multiply a per-client limit.
	config.RateLimiter = flowcontrol.NewTokenBucketRateLimiter(config.QPS, burst)
}

func configureKubeClientRateLimits(config *rest.Config, qps, burst int) *rest.Config {
	leaderConfig := rest.CopyConfig(config)
	configureKubeClientRateLimit(leaderConfig, 5, 10)
	configureKubeClientRateLimit(config, qps, burst)
	return leaderConfig
}

func validateCapacityConfig(cfg config.Config) error {
	if cfg.PollRetryWindow <= 0 {
		return fmt.Errorf("poll-retry-window must be positive")
	}
	if cfg.MaxBuildDuration <= 0 {
		return fmt.Errorf("max-build-duration must be positive")
	}
	if cfg.VerificationAttemptTimeout <= 0 || cfg.VerificationAttemptTimeout > time.Minute {
		return fmt.Errorf("verification-attempt-timeout must be between 1ns and 1m")
	}
	if cfg.VerificationWindow < cfg.VerificationAttemptTimeout || cfg.VerificationWindow > time.Hour {
		return fmt.Errorf("verification-window must be between verification-attempt-timeout and 1h")
	}
	if cfg.MaxBuildDuration > time.Duration(1<<63-1)-cfg.VerificationWindow {
		return fmt.Errorf("max-build-duration plus verification-window overflows")
	}
	if cfg.MaxActiveJobs < 1 {
		return fmt.Errorf("max-active-jobs must be at least 1")
	}
	if cfg.MaxActiveJobsPerRequester < 1 || cfg.MaxActiveJobsPerRequester > cfg.MaxActiveJobs {
		return fmt.Errorf("max-active-jobs-per-requester must be between 1 and max-active-jobs")
	}
	if cfg.MaxQueuedJobsPerRequester < 1 {
		return fmt.Errorf("max-queued-jobs-per-requester must be at least 1")
	}
	if cfg.MaxQueuedJobs < 1 || cfg.MaxQueuedJobs > 1000 {
		return fmt.Errorf("max-queued-jobs must be between 1 and 1000")
	}
	if cfg.MaxQueuedJobsPerRequester > cfg.MaxQueuedJobs {
		return fmt.Errorf("max-queued-jobs-per-requester must not exceed max-queued-jobs")
	}
	if cfg.WorkerSlots < 1 {
		return fmt.Errorf("worker-slots must be at least 1")
	}
	// Result verification can block one reconciler until its bounded I/O
	// attempt ends. Keep another worker available for cancellation and cleanup.
	if cfg.ControllerConcurrency < 2 || cfg.ControllerConcurrency > buildcontract.MaxControllerConcurrency {
		return fmt.Errorf("controller-concurrency must be between 2 and %d", buildcontract.MaxControllerConcurrency)
	}
	return nil
}

func runnerObservabilityEnv() map[string]string {
	names := []string{
		"KOVA_OTEL_ENABLED",
		"KOVA_OTEL_TRACES_ENABLED",
		"KOVA_OTEL_METRICS_ENABLED",
		"KOVA_OTEL_LOGS_ENABLED",
		"KOVA_OTEL_METRIC_INTERVAL",
		"OTEL_EXPORTER_OTLP_ENDPOINT",
		"OTEL_EXPORTER_OTLP_INSECURE",
		"OTEL_RESOURCE_ATTRIBUTES",
	}
	env := make(map[string]string, len(names)+1)
	for _, name := range names {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			env[name] = value
		}
	}
	if serviceName := strings.TrimSpace(os.Getenv("KOVA_RUNNER_OTEL_SERVICE_NAME")); serviceName != "" {
		env["OTEL_SERVICE_NAME"] = serviceName
	}
	return env
}

func parseNodeSelector(values []string) (map[string]string, error) {
	selectors := make(map[string]string, len(values))
	for _, raw := range values {
		key, value, ok := strings.Cut(strings.TrimSpace(raw), "=")
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if !ok || key == "" {
			return nil, fmt.Errorf("invalid runner node selector %q: expected key=value", raw)
		}
		if problems := validation.IsQualifiedName(key); len(problems) > 0 {
			return nil, fmt.Errorf("invalid runner node selector key %q: %s", key, strings.Join(problems, ", "))
		}
		if problems := validation.IsValidLabelValue(value); len(problems) > 0 {
			return nil, fmt.Errorf("invalid runner node selector value for %q: %s", key, strings.Join(problems, ", "))
		}
		if _, exists := selectors[key]; exists {
			return nil, fmt.Errorf("duplicate runner node selector key %q", key)
		}
		selectors[key] = value
	}
	return selectors, nil
}

func parseRegistryHosts(values []string) ([]string, error) {
	seen := make(map[string]struct{}, len(values))
	hosts := make([]string, 0, len(values))
	for _, value := range values {
		host := strings.ToLower(strings.TrimSpace(value))
		if host == "" || strings.Contains(host, "://") || strings.Contains(host, "/") {
			return nil, fmt.Errorf("invalid plain HTTP registry %q: expected host or host:port", value)
		}
		if _, exists := seen[host]; exists {
			continue
		}
		seen[host] = struct{}{}
		hosts = append(hosts, host)
	}
	return hosts, nil
}

func parsePodResources(raw string) (corev1.ResourceRequirements, error) {
	var resources corev1.ResourceRequirements
	if strings.TrimSpace(raw) == "" {
		return resources, nil
	}
	if err := json.Unmarshal([]byte(raw), &resources); err != nil {
		return resources, err
	}
	for name, quantity := range resources.Requests {
		if quantity.Sign() <= 0 {
			return resources, fmt.Errorf("request %s must be positive", name)
		}
	}
	for name, quantity := range resources.Limits {
		if quantity.Sign() <= 0 {
			return resources, fmt.Errorf("limit %s must be positive", name)
		}
	}
	return resources, nil
}

func validateSourcePodBudget(volumeLimit resource.Quantity, runnerResources, fetchResources corev1.ResourceRequirements) error {
	if err := validateReservedLimits("runner", runner.DefaultRunnerResources(), runnerResources); err != nil {
		return err
	}
	if err := validateReservedLimits("source fetch", runner.DefaultSourceFetchResources(), fetchResources); err != nil {
		return err
	}
	runnerLimit := runner.DefaultRunnerResources().Limits[corev1.ResourceEphemeralStorage]
	if override, ok := runnerResources.Limits[corev1.ResourceEphemeralStorage]; ok {
		runnerLimit = override
	}
	minimumRunnerLimit := volumeLimit.DeepCopy()
	minimumRunnerLimit.Add(resource.MustParse("1Gi"))
	if runnerLimit.Cmp(minimumRunnerLimit) < 0 {
		return fmt.Errorf("runner ephemeral-storage limit must exceed source volume size limit by at least 1Gi")
	}
	initLimit := runner.DefaultSourceFetchResources().Limits[corev1.ResourceEphemeralStorage]
	if override, ok := fetchResources.Limits[corev1.ResourceEphemeralStorage]; ok {
		initLimit = override
	}
	if initLimit.Cmp(*resource.NewQuantity(source.MaxArchiveBytes, resource.BinarySI)) < 0 {
		return fmt.Errorf("source fetch ephemeral-storage limit must be at least 512Mi")
	}
	return nil
}

func validateReservedLimits(name string, defaults, overrides corev1.ResourceRequirements) error {
	for _, key := range []corev1.ResourceName{corev1.ResourceMemory, corev1.ResourceEphemeralStorage} {
		request := defaults.Requests[key]
		if override, ok := overrides.Requests[key]; ok {
			request = override
		}
		limit := defaults.Limits[key]
		if override, ok := overrides.Limits[key]; ok {
			limit = override
		}
		if request.Cmp(limit) != 0 {
			return fmt.Errorf("%s %s request must equal its limit", name, key)
		}
	}
	return nil
}

func leaderElectionNamespace(c *cli.Context, fallback string) string {
	if ns := strings.TrimSpace(c.String("leader-election-namespace")); ns != "" {
		return ns
	}
	return fallback
}
