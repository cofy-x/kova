package service

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/artifactstore"
	"github.com/cofy-x/kova/internal/kube"
	"github.com/cofy-x/kova/internal/runner"
	serviceauth "github.com/cofy-x/kova/internal/service/auth"
	"github.com/cofy-x/kova/internal/service/buildcontroller"
	"github.com/cofy-x/kova/internal/service/config"
	"github.com/cofy-x/kova/internal/service/httpapi"

	"github.com/urfave/cli/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
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
			&cli.StringFlag{Name: "buildkit-addr", Value: defaults.BuildkitAddr, Usage: "BuildKit address passed to runner daemon and build requests"},
			&cli.StringFlag{Name: "source-pvc-claim", Usage: "PVC claim backing filesystem artifacts; not used by S3 storage"},
			&cli.StringFlag{Name: "artifact-driver", Value: artifactstore.DriverFilesystem, EnvVars: []string{"KOVA_ARTIFACT_DRIVER"}, Usage: "artifact storage driver: filesystem or s3"},
			&cli.StringFlag{Name: "artifact-root", Value: artifactstore.DefaultRoot, EnvVars: []string{"KOVA_ARTIFACT_ROOT"}, Usage: "filesystem artifact root"},
			&cli.StringFlag{Name: "artifact-secret", EnvVars: []string{"KOVA_ARTIFACT_SECRET"}, Usage: "Secret whose environment variables allow runner init containers to read artifacts"},
			&cli.StringFlag{Name: "s3-endpoint", EnvVars: []string{"KOVA_S3_ENDPOINT"}},
			&cli.StringFlag{Name: "s3-bucket", EnvVars: []string{"KOVA_S3_BUCKET"}},
			&cli.StringFlag{Name: "s3-region", EnvVars: []string{"KOVA_S3_REGION"}},
			&cli.StringFlag{Name: "s3-access-key", EnvVars: []string{"KOVA_S3_ACCESS_KEY"}},
			&cli.StringFlag{Name: "s3-secret-key", EnvVars: []string{"KOVA_S3_SECRET_KEY"}},
			&cli.StringFlag{Name: "s3-session-token", EnvVars: []string{"KOVA_S3_SESSION_TOKEN"}},
			&cli.BoolFlag{Name: "s3-secure", Value: true, EnvVars: []string{"KOVA_S3_SECURE"}},
			&cli.DurationFlag{Name: "job-ttl", Value: 2 * time.Hour, Usage: "duration to retain terminal jobs before cleanup"},
			&cli.Int64Flag{Name: "max-upload-bytes", Value: 1 << 30, Usage: "maximum multipart build request size in bytes"},
			&cli.StringFlag{Name: "auth-mode", Value: serviceauth.ModeTokenReview, EnvVars: []string{"KOVA_SERVICE_AUTH_MODE"}, Usage: "API authentication mode: tokenreview, static, or unsafe-none"},
			&cli.StringFlag{Name: "auth-token", EnvVars: []string{"KOVA_SERVICE_AUTH_TOKEN"}, Usage: "bearer token required by static authentication"},
			&cli.DurationFlag{Name: "wait", Value: 3 * time.Minute, Usage: "timeout for runner Pod readiness"},
			&cli.DurationFlag{Name: "poll-interval", Value: 5 * time.Second, Usage: "build status polling interval"},
			&cli.IntFlag{Name: "max-active-jobs", Value: 20, Usage: "maximum concurrently active service jobs"},
			&cli.IntFlag{Name: "worker-slots", Value: 20, Usage: "total build slots shared fairly across active jobs"},
			&cli.BoolFlag{Name: "leader-elect", Value: true, Usage: "enable controller-runtime leader election"},
			&cli.StringFlag{Name: "leader-election-namespace", Usage: "namespace used for controller leader election leases; defaults to --namespace"},
		},
		Action: func(c *cli.Context) error {
			runnerNodeSelector, err := parseNodeSelector(c.StringSlice("runner-node-selector"))
			if err != nil {
				return err
			}
			restConfig, err := rest.InClusterConfig()
			if err != nil {
				return err
			}
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
				Listen:                c.String("listen"),
				Namespace:             c.String("namespace"),
				RunnerImage:           strings.TrimSpace(c.String("runner-image")),
				RunnerImagePullPolicy: c.String("runner-image-pull-policy"),
				RunnerImagePullSecret: c.String("runner-image-pull-secret"),
				RunnerNodeSelector:    runnerNodeSelector,
				RunnerEnv:             runnerObservabilityEnv(),
				BuildkitAddr:          c.String("buildkit-addr"),
				SourcePVCClaim:        strings.TrimSpace(c.String("source-pvc-claim")),
				ArtifactDriver:        c.String("artifact-driver"),
				ArtifactRoot:          c.String("artifact-root"),
				ArtifactSecret:        c.String("artifact-secret"),
				S3Endpoint:            c.String("s3-endpoint"),
				S3Bucket:              c.String("s3-bucket"),
				S3Region:              c.String("s3-region"),
				S3AccessKey:           c.String("s3-access-key"),
				S3SecretKey:           c.String("s3-secret-key"),
				S3SessionToken:        c.String("s3-session-token"),
				S3Secure:              c.Bool("s3-secure"),
				JobTTL:                c.Duration("job-ttl"),
				MaxUploadBytes:        c.Int64("max-upload-bytes"),
				AuthToken:             c.String("auth-token"),
				AuthMode:              c.String("auth-mode"),
				WaitTimeout:           c.Duration("wait"),
				PollInterval:          c.Duration("poll-interval"),
				MaxActiveJobs:         c.Int("max-active-jobs"),
				WorkerSlots:           c.Int("worker-slots"),
			}
			store, err := artifactstore.New(artifactstore.Config{
				Driver: cfg.ArtifactDriver, Root: cfg.ArtifactRoot,
				S3Endpoint: cfg.S3Endpoint, S3Bucket: cfg.S3Bucket, S3Region: cfg.S3Region,
				S3AccessKey: cfg.S3AccessKey, S3SecretKey: cfg.S3SecretKey,
				S3SessionKey: cfg.S3SessionToken, S3Secure: cfg.S3Secure,
			})
			if err != nil {
				return err
			}
			authenticator, err := serviceauth.New(cfg.AuthMode, cfg.AuthToken, clientset.AuthenticationV1().TokenReviews())
			if err != nil {
				return err
			}
			mgr, err := ctrl.NewManager(restConfig, ctrl.Options{
				Scheme:                  scheme,
				Cache:                   cache.Options{DefaultNamespaces: map[string]cache.Config{cfg.Namespace: {}}},
				Metrics:                 metricsserver.Options{BindAddress: "0"},
				LeaderElection:          c.Bool("leader-elect"),
				LeaderElectionID:        "kova-service.kova.cofy.dev",
				LeaderElectionNamespace: leaderElectionNamespace(c, cfg.Namespace),
			})
			if err != nil {
				return err
			}
			if err := (&buildcontroller.KovaBuildReconciler{
				Client: mgr.GetClient(),
				Scheme: mgr.GetScheme(),
				Kube:   kubeClient,
				Store:  store,
				Cfg:    cfg,
			}).SetupWithManager(mgr); err != nil {
				return err
			}
			go func() {
				if err := httpapi.NewServer(cfg, kubeClient, mgr.GetClient(), mgr.GetAPIReader(), store, authenticator).Start(ctx); err != nil {
					stop()
				}
			}()
			return mgr.Start(ctx)
		},
	}
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

func leaderElectionNamespace(c *cli.Context, fallback string) string {
	if ns := strings.TrimSpace(c.String("leader-election-namespace")); ns != "" {
		return ns
	}
	return fallback
}
