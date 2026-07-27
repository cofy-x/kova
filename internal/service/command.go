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
	"github.com/cofy-x/kova/internal/kube"
	"github.com/cofy-x/kova/internal/runner"
	"github.com/cofy-x/kova/internal/service/buildcontroller"
	"github.com/cofy-x/kova/internal/service/config"
	"github.com/cofy-x/kova/internal/service/httpapi"
	"github.com/cofy-x/kova/internal/service/sourcestore"

	"github.com/urfave/cli/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
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
			&cli.StringFlag{Name: "runner-image", EnvVars: []string{"KOVA_IMAGE"}, Usage: "runner image used for created Pods"},
			&cli.StringFlag{Name: "runner-image-pull-policy", Value: defaults.RunnerImagePullPolicy, Usage: "runner image pull policy"},
			&cli.StringFlag{Name: "runner-image-pull-secret", Value: defaults.ImagePullSecret, Usage: "runner image pull secret name"},
			&cli.StringSliceFlag{Name: "runner-node-selector", Usage: "node selector for runner Pods; repeatable key=value"},
			&cli.StringFlag{Name: "buildkit-addr", Value: defaults.BuildkitAddr, Usage: "BuildKit address passed to runner daemon and build requests"},
			&cli.StringFlag{Name: "source-pvc-claim", Usage: "PVC claim used to store uploaded source archives"},
			&cli.StringFlag{Name: "source-root", Value: sourcestore.DefaultRoot, Usage: "mounted source store root path"},
			&cli.DurationFlag{Name: "job-ttl", Value: 2 * time.Hour, Usage: "duration to retain terminal jobs before cleanup"},
			&cli.StringFlag{Name: "auth-token", EnvVars: []string{"KOVA_SERVICE_AUTH_TOKEN"}, Usage: "optional bearer token for /v1 APIs"},
			&cli.DurationFlag{Name: "wait", Value: 3 * time.Minute, Usage: "timeout for runner Pod readiness"},
			&cli.DurationFlag{Name: "poll-interval", Value: 5 * time.Second, Usage: "build status polling interval"},
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
				BuildkitAddr:          c.String("buildkit-addr"),
				SourcePVCClaim:        strings.TrimSpace(c.String("source-pvc-claim")),
				SourceRoot:            c.String("source-root"),
				SourceMountPath:       c.String("source-root"),
				JobTTL:                c.Duration("job-ttl"),
				AuthToken:             c.String("auth-token"),
				WaitTimeout:           c.Duration("wait"),
				PollInterval:          c.Duration("poll-interval"),
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
				Cfg:    cfg,
			}).SetupWithManager(mgr); err != nil {
				return err
			}
			go func() {
				if err := httpapi.NewServer(cfg, kubeClient, mgr.GetClient(), mgr.GetAPIReader()).Start(ctx); err != nil {
					stop()
				}
			}()
			return mgr.Start(ctx)
		},
	}
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
