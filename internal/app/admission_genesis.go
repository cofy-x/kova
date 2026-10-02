package app

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/cofy-x/kova/internal/service/admissiongenesis"

	"github.com/urfave/cli/v2"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

const admissionGenesisCommandTimeout = 30 * time.Second

func admissionGenesisCLICommand() *cli.Command {
	return &cli.Command{
		Name:  "admission-genesis",
		Usage: "render and verify an externally provisioned Service admission installation",
		Subcommands: []*cli.Command{
			{
				Name:  "render-genesis",
				Usage: "read the explicit original Namespace and render an Initializing Genesis ConfigMap JSON manifest",
				Flags: admissionGenesisIdentityFlags(),
				Action: func(c *cli.Context) error {
					ctx, cancel := context.WithTimeout(c.Context, admissionGenesisCommandTimeout)
					defer cancel()
					spec, reader, err := admissionGenesisInputs(c)
					if err != nil {
						return err
					}
					manifest, err := spec.RenderInitializingGenesis(ctx, reader)
					if err != nil {
						return err
					}
					return encodeAdmissionManifest(c, manifest)
				},
			},
			{
				Name:  "export-receipt-secret",
				Usage: "read the explicitly pinned original Genesis and render an immutable receipt Secret JSON manifest",
				Flags: append(admissionGenesisIdentityFlags(),
					&cli.StringFlag{Name: "genesis-uid", Usage: "expected original Genesis UID (required)"},
					&cli.StringFlag{Name: "service-namespace", Usage: "Namespace of the externally installed receipt Secret (required)"},
					&cli.StringFlag{Name: "secret-name", Usage: "name of the externally installed immutable receipt Secret (required)"},
				),
				Action: func(c *cli.Context) error {
					ctx, cancel := context.WithTimeout(c.Context, admissionGenesisCommandTimeout)
					defer cancel()
					if strings.TrimSpace(c.String("genesis-uid")) == "" ||
						strings.TrimSpace(c.String("service-namespace")) == "" ||
						strings.TrimSpace(c.String("secret-name")) == "" {
						return fmt.Errorf("explicit --genesis-uid, --service-namespace and --secret-name are required")
					}
					spec, reader, err := admissionGenesisInputs(c)
					if err != nil {
						return err
					}
					receipt, err := spec.ReceiptFor(c.String("genesis-uid"))
					if err != nil {
						return err
					}
					manifest, err := receipt.ExportReceiptSecret(ctx, reader,
						c.String("service-namespace"), c.String("secret-name"))
					if err != nil {
						return err
					}
					return encodeAdmissionManifest(c, manifest)
				},
			},
		},
	}
}

func admissionGenesisIdentityFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{Name: "namespace-uid", Usage: "expected original runner Namespace UID (required)"},
		&cli.StringFlag{Name: "generation", Usage: "caller-created 32-character lowercase hex installation generation (required)"},
		&cli.IntFlag{Name: "max-active-jobs", Usage: "exact admission maxActiveJobs (required)"},
		&cli.IntFlag{Name: "max-active-jobs-per-requester", Usage: "exact admission maxActiveJobsPerRequester (required)"},
		&cli.IntFlag{Name: "worker-slots", Usage: "exact admission workerSlots (required)"},
		&cli.IntFlag{Name: "max-queued-jobs", Usage: "exact admission maxQueuedJobs (required)"},
		&cli.IntFlag{Name: "max-queued-jobs-per-requester", Usage: "exact admission maxQueuedJobsPerRequester (required)"},
	}
}

func admissionGenesisInputs(c *cli.Context) (admissiongenesis.InstallationSpec, admissiongenesis.DirectClient, error) {
	var empty admissiongenesis.InstallationSpec
	var noReader admissiongenesis.DirectClient
	if !c.IsSet("namespace") || !c.IsSet("kubeconfig") ||
		strings.TrimSpace(c.String("namespace")) == "" || strings.TrimSpace(c.String("kubeconfig")) == "" {
		return empty, noReader, fmt.Errorf("explicit global --namespace and --kubeconfig are required")
	}
	for _, name := range []string{"namespace-uid", "generation", "max-active-jobs", "max-active-jobs-per-requester",
		"worker-slots", "max-queued-jobs", "max-queued-jobs-per-requester"} {
		if !c.IsSet(name) {
			return empty, noReader, fmt.Errorf("explicit --%s is required", name)
		}
	}
	spec := admissiongenesis.InstallationSpec{
		Namespace: c.String("namespace"), NamespaceUID: c.String("namespace-uid"),
		Generation: c.String("generation"), Limits: admissiongenesis.Limits{
			MaxActiveJobs:             c.Int("max-active-jobs"),
			MaxActiveJobsPerRequester: c.Int("max-active-jobs-per-requester"),
			WorkerSlots:               c.Int("worker-slots"),
			MaxQueuedJobs:             c.Int("max-queued-jobs"),
			MaxQueuedJobsPerRequester: c.Int("max-queued-jobs-per-requester"),
		},
	}
	// Validate the caller's proposed contract before even reading the API.
	if err := spec.Validate(); err != nil {
		return empty, noReader, err
	}
	config, err := clientcmd.BuildConfigFromFlags("", c.String("kubeconfig"))
	if err != nil {
		return empty, noReader, err
	}
	config.Timeout = 10 * time.Second
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return empty, noReader, err
	}
	return spec, admissiongenesis.DirectClient{Client: clientset}, nil
}

func encodeAdmissionManifest(c *cli.Context, manifest any) error {
	encoder := json.NewEncoder(c.App.Writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(manifest)
}
