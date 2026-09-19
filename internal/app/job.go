package app

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/cofy-x/kova/internal/buildcontract"
	"github.com/cofy-x/kova/internal/ctxconfig"
	"github.com/cofy-x/kova/internal/source"
	"github.com/cofy-x/kova/internal/sourcebundle"
	apiv1 "github.com/cofy-x/kova/pkg/api/v1"
	kovaclient "github.com/cofy-x/kova/pkg/client"

	cli "github.com/urfave/cli/v2"
)

func jobCLICommand() *cli.Command {
	return &cli.Command{
		Name:  "job",
		Usage: "submit and manage builds through the Kova service",
		Subcommands: []*cli.Command{
			jobSubmitCLICommand(), jobListCLICommand(), jobGetCLICommand(),
			jobLogsCLICommand(), jobWaitCLICommand(), jobCancelCLICommand(),
			jobResultsCLICommand(),
		},
	}
}

func jobSubmitCLICommand() *cli.Command {
	return &cli.Command{
		Name:      "submit",
		Usage:     "submit an immutable source bundle to the Kova service",
		ArgsUsage: "<oci-or-https-source|context-directory|source.zip>",
		Flags: []cli.Flag{
			&cli.StringSliceFlag{Name: "target", Usage: "output image reference; repeatable for a batch source"},
			&cli.StringSliceFlag{Name: "platform", Usage: "platform paired by position with --target; repeatable"},
			&cli.StringFlag{Name: "source-digest", Usage: "required SHA-256 content digest for a remote source"},
			&cli.StringFlag{Name: "source-repository", Usage: "OCI repository tag used to publish a local directory or zip"},
			&cli.StringSliceFlag{Name: "registry-plain-http", Usage: "registry host using plain HTTP; repeatable and intended for development"},
			&cli.StringFlag{Name: "format", Value: "oci", Usage: "build output format: oci, nydus, or both"},
			&cli.StringSliceFlag{Name: "var", Usage: "build variable in KEY=value form; repeatable"},
			&cli.IntFlag{Name: "concurrency", Value: 1, Usage: "maximum concurrent targets"},
			&cli.IntFlag{Name: "timeout", Value: 300, Usage: "per-target timeout in seconds; 0 disables timeout"},
			&cli.DurationFlag{Name: "oom-cooldown", Value: defaultBuildkitOOMCooldown, Usage: "worker cooldown after an OOM-style failure"},
			&cli.BoolFlag{Name: "fail-fast", Usage: "stop after the first failure"},
			&cli.BoolFlag{Name: "verbose", Usage: "enable verbose runner output"},
			&cli.StringFlag{Name: "idempotency-key", Usage: "stable caller-scoped request key"},
		},
		Action: func(c *cli.Context) error {
			if c.NArg() != 1 {
				return fmt.Errorf("job submit requires exactly one immutable source reference or local source")
			}
			input := strings.TrimSpace(c.Args().First())
			sourceURI, sourceDigest := input, strings.TrimSpace(c.String("source-digest"))
			targets, err := requestedTargetSpecs(c.StringSlice("target"), c.StringSlice("platform"))
			if err != nil {
				return err
			}
			if _, err := os.Stat(input); err == nil {
				if strings.TrimSpace(c.String("source-repository")) == "" {
					return fmt.Errorf("--source-repository is required for a local source")
				}
				info, _ := os.Stat(input)
				if info.IsDir() && len(targets) != 1 {
					return fmt.Errorf("a context directory requires exactly one --target")
				}
				if !info.IsDir() && len(targets) == 0 {
					targets, err = source.BuildArchiveTargets(input)
					if err != nil {
						return err
					}
				}
				targets, err = buildcontract.NormalizeTargetSpecs(targets)
				if err != nil {
					return err
				}
				archive, cleanup, err := sourceArchive(input, targets[0].Target, targets[0].Platform)
				if err != nil {
					return err
				}
				defer cleanup()
				ref, err := sourcebundle.Push(c.Context, archive, c.String("source-repository"), c.StringSlice("registry-plain-http"))
				if err != nil {
					return err
				}
				sourceURI, sourceDigest = ref.URI, ref.Digest
			} else if !os.IsNotExist(err) {
				return err
			}
			if !isLocalPath(input) {
				var err error
				targets, err = buildcontract.NormalizeTargetSpecs(targets)
				if err != nil {
					return err
				}
			}
			if err := buildcontract.ValidateConcurrency(c.Int("concurrency"), len(targets)); err != nil {
				return err
			}
			if err := sourcebundle.Validate(sourceURI, sourceDigest); err != nil {
				return err
			}
			client, err := serviceClientFromContext(c)
			if err != nil {
				return err
			}
			apiTargets := make([]apiv1.TargetSpec, 0, len(targets))
			for _, target := range targets {
				apiTargets = append(apiTargets, apiv1.TargetSpec{Target: target.Target, Platform: apiv1.Platform(target.Platform)})
			}
			job, err := client.CreateBuild(c.Context, apiv1.CreateBuildRequest{
				SourceURI: sourceURI, SourceDigest: sourceDigest, Targets: apiTargets, Format: c.String("format"),
				Concurrency: c.Int("concurrency"), Timeout: c.Int("timeout"),
				OOMCooldown: c.Duration("oom-cooldown").String(), FailFast: c.Bool("fail-fast"),
				Verbose: c.Bool("verbose"), Variables: c.StringSlice("var"),
				IdempotencyKey: c.String("idempotency-key"),
			})
			if err != nil {
				return err
			}
			return writeJSON(c, job)
		},
	}
}

func isLocalPath(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func jobListCLICommand() *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "list visible service jobs",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "output", Aliases: []string{"o"}, Value: "table", Usage: "output format: table or json"},
			&cli.IntFlag{Name: "limit", Value: 100, Usage: "maximum jobs to return, from 1 to 500"},
			&cli.StringFlag{Name: "continue", Usage: "opaque continuation token from a previous response"},
		},
		Action: func(c *cli.Context) error {
			client, err := serviceClientFromContext(c)
			if err != nil {
				return err
			}
			jobs, err := client.ListBuildsPage(c.Context, c.Int("limit"), c.String("continue"))
			if err != nil {
				return err
			}
			if c.String("output") == "json" {
				return writeJSON(c, jobs)
			}
			if c.String("output") != "table" {
				return fmt.Errorf("unsupported output format %q", c.String("output"))
			}
			writer := tabwriter.NewWriter(c.App.Writer, 0, 0, 2, ' ', 0)
			fmt.Fprintln(writer, "ID\tSTATUS\tREQUESTER\tCREATED")
			for _, job := range jobs.Jobs {
				fmt.Fprintf(writer, "%s\t%s\t%s\t%s\n", job.ID, job.Status, job.Requester, job.CreatedAt.Format(time.RFC3339))
			}
			if err := writer.Flush(); err != nil {
				return err
			}
			if jobs.Continue != "" {
				fmt.Fprintf(c.App.ErrWriter, "more jobs are available; continuation token: %s\n", jobs.Continue)
			}
			return nil
		},
	}
}

func jobGetCLICommand() *cli.Command {
	return jobIDCommand("get", "show a service job", func(c *cli.Context, client *kovaclient.Client, id string) error {
		job, err := client.GetBuild(c.Context, id)
		if err != nil {
			return err
		}
		return writeJSON(c, job)
	})
}

func jobLogsCLICommand() *cli.Command {
	command := jobIDCommand("logs", "print service job logs", func(c *cli.Context, client *kovaclient.Client, id string) error {
		raw, err := client.GetLogs(c.Context, id, c.Int64("tail"))
		if err != nil {
			return err
		}
		_, err = c.App.Writer.Write(raw)
		return err
	})
	command.Flags = []cli.Flag{&cli.Int64Flag{Name: "tail", Value: 100, Usage: "number of recent lines"}}
	return command
}

func jobWaitCLICommand() *cli.Command {
	command := jobIDCommand("wait", "wait for a service job to finish", func(c *cli.Context, client *kovaclient.Client, id string) error {
		ctx := c.Context
		cancel := func() {}
		if timeout := c.Duration("timeout"); timeout > 0 {
			ctx, cancel = context.WithTimeout(ctx, timeout)
		}
		defer cancel()
		job, err := client.WaitBuild(ctx, id, c.Duration("interval"))
		if err != nil {
			return err
		}
		return writeJSON(c, job)
	})
	command.Flags = []cli.Flag{
		&cli.DurationFlag{Name: "timeout", Value: 0, Usage: "overall wait timeout; 0 disables timeout"},
		&cli.DurationFlag{Name: "interval", Value: 2 * time.Second, Usage: "poll interval"},
	}
	return command
}

func jobCancelCLICommand() *cli.Command {
	return jobIDCommand("cancel", "cancel a service job", func(c *cli.Context, client *kovaclient.Client, id string) error {
		job, err := client.CancelBuild(c.Context, id)
		if err != nil {
			return err
		}
		return writeJSON(c, job)
	})
}

func jobResultsCLICommand() *cli.Command {
	return jobIDCommand("results", "show typed service job results", func(c *cli.Context, client *kovaclient.Client, id string) error {
		results, err := client.GetResults(c.Context, id)
		if err != nil {
			return err
		}
		return writeJSON(c, results)
	})
}

func jobIDCommand(name, usage string, action func(*cli.Context, *kovaclient.Client, string) error) *cli.Command {
	return &cli.Command{
		Name: name, Usage: usage, ArgsUsage: "<job-id>",
		Action: func(c *cli.Context) error {
			if c.NArg() != 1 || strings.TrimSpace(c.Args().First()) == "" {
				return fmt.Errorf("job %s requires exactly one job ID", name)
			}
			client, err := serviceClientFromContext(c)
			if err != nil {
				return err
			}
			return action(c, client, strings.TrimSpace(c.Args().First()))
		},
	}
}

func serviceClientFromContext(c *cli.Context) (*kovaclient.Client, error) {
	cfg, err := loadCtxConfig(c)
	if err != nil {
		return nil, err
	}
	_, selected, hasCtx := cfg.Resolve(c.String("ctx"))
	if !hasCtx && strings.TrimSpace(c.String("ctx")) != "" {
		return nil, fmt.Errorf("ctx %q does not exist", c.String("ctx"))
	}
	baseURL := strings.TrimSpace(c.String("service-url"))
	if baseURL == "" && hasCtx {
		if selected.EffectiveMode() != ctxconfig.ModeService {
			return nil, fmt.Errorf("selected ctx uses direct mode; choose a service context")
		}
		baseURL = selected.ServiceURL
	}
	if baseURL == "" {
		return nil, fmt.Errorf("service URL is required; set it in a service context or KOVA_SERVICE_URL")
	}
	kubeconfig := strings.TrimSpace(c.String("kubeconfig"))
	if kubeconfig == "" {
		kubeconfig = envValue("KOVA_KUBECONFIG")
	}
	if kubeconfig == "" && hasCtx {
		kubeconfig = selected.Kubeconfig
	}
	caFile := strings.TrimSpace(c.String("service-ca-file"))
	if caFile == "" && hasCtx {
		caFile = selected.ServiceCAFile
	}
	insecure := c.Bool("service-insecure")
	if !c.IsSet("service-insecure") && hasCtx {
		insecure = selected.ServiceInsecure
	}
	return kovaclient.New(kovaclient.Config{
		BaseURL: baseURL, Token: envValue("KOVA_SERVICE_TOKEN"), Kubeconfig: kubeconfig,
		CAFile: caFile, Insecure: insecure,
	})
}

func requestedTargetSpecs(targets, platforms []string) ([]buildcontract.TargetSpec, error) {
	if len(targets) != len(platforms) {
		return nil, fmt.Errorf("each --target requires one --platform in the same position")
	}
	values := make([]buildcontract.TargetSpec, 0, len(targets))
	for index := range targets {
		values = append(values, buildcontract.TargetSpec{Target: targets[index], Platform: platforms[index]})
	}
	if len(values) == 0 {
		return values, nil
	}
	return buildcontract.NormalizeTargetSpecs(values)
}

func writeJSON(c *cli.Context, value any) error {
	encoder := json.NewEncoder(c.App.Writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}
