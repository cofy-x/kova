package app

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/cofy-x/kova/internal/ctxconfig"
	"github.com/cofy-x/kova/internal/serviceclient"
	"github.com/cofy-x/kova/internal/source"

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
		Usage:     "submit a directory or source zip to the Kova service",
		ArgsUsage: "<context-directory|source.zip>",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "target", Usage: "output image target; required for a directory and optional for a batch zip"},
			&cli.StringFlag{Name: "format", Value: "oci", Usage: "build output format: oci, nydus, or both"},
			&cli.StringSliceFlag{Name: "var", Usage: "build variable in KEY=value form; repeatable"},
			&cli.IntFlag{Name: "concurrency", Value: 1, Usage: "maximum concurrent targets"},
			&cli.IntFlag{Name: "timeout", Value: 300, Usage: "per-target timeout in seconds; 0 disables timeout"},
			&cli.IntFlag{Name: "retry", Value: 0, Usage: "retry count"},
			&cli.DurationFlag{Name: "oom-cooldown", Value: defaultBuildkitOOMCooldown, Usage: "worker cooldown after an OOM-style failure"},
			&cli.BoolFlag{Name: "fail-fast", Usage: "stop after the first failure"},
			&cli.BoolFlag{Name: "skip-fail", Usage: "skip targets previously recorded as failed"},
			&cli.BoolFlag{Name: "verbose", Usage: "enable verbose runner output"},
			&cli.StringFlag{Name: "idempotency-key", Usage: "stable caller-scoped request key"},
		},
		Action: func(c *cli.Context) error {
			if c.NArg() != 1 {
				return fmt.Errorf("job submit requires exactly one context directory or source zip")
			}
			archivePath, cleanup, err := prepareServiceArchive(c.Args().First(), c.String("target"))
			if err != nil {
				return err
			}
			defer cleanup()
			client, err := serviceClientFromContext(c)
			if err != nil {
				return err
			}
			job, err := client.CreateBuild(c.Context, serviceclient.CreateBuildOptions{
				ArchivePath: archivePath, Target: c.String("target"), Format: c.String("format"),
				Concurrency: c.Int("concurrency"), Timeout: c.Int("timeout"), Retry: c.Int("retry"),
				OOMCooldown: c.Duration("oom-cooldown"), FailFast: c.Bool("fail-fast"),
				SkipFail: c.Bool("skip-fail"), Verbose: c.Bool("verbose"), Variables: c.StringSlice("var"),
				IdempotencyKey: c.String("idempotency-key"),
			})
			if err != nil {
				return err
			}
			return writeJSON(c, job)
		},
	}
}

func jobListCLICommand() *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "list visible service jobs",
		Flags: []cli.Flag{&cli.StringFlag{Name: "output", Aliases: []string{"o"}, Value: "table", Usage: "output format: table or json"}},
		Action: func(c *cli.Context) error {
			client, err := serviceClientFromContext(c)
			if err != nil {
				return err
			}
			jobs, err := client.List(c.Context)
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
			return writer.Flush()
		},
	}
}

func jobGetCLICommand() *cli.Command {
	return jobIDCommand("get", "show a service job", func(c *cli.Context, client *serviceclient.Client, id string) error {
		job, err := client.Get(c.Context, id)
		if err != nil {
			return err
		}
		return writeJSON(c, job)
	})
}

func jobLogsCLICommand() *cli.Command {
	command := jobIDCommand("logs", "print service job logs", func(c *cli.Context, client *serviceclient.Client, id string) error {
		raw, err := client.Logs(c.Context, id, c.Int64("tail"))
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
	command := jobIDCommand("wait", "wait for a service job to finish", func(c *cli.Context, client *serviceclient.Client, id string) error {
		ctx := c.Context
		cancel := func() {}
		if timeout := c.Duration("timeout"); timeout > 0 {
			ctx, cancel = context.WithTimeout(ctx, timeout)
		}
		defer cancel()
		job, err := client.Wait(ctx, id, c.Duration("interval"))
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
	return jobIDCommand("cancel", "cancel a service job", func(c *cli.Context, client *serviceclient.Client, id string) error {
		job, err := client.Cancel(c.Context, id)
		if err != nil {
			return err
		}
		return writeJSON(c, job)
	})
}

func jobResultsCLICommand() *cli.Command {
	return jobIDCommand("results", "show typed service job results", func(c *cli.Context, client *serviceclient.Client, id string) error {
		results, err := client.Results(c.Context, id)
		if err != nil {
			return err
		}
		return writeJSON(c, results)
	})
}

func jobIDCommand(name, usage string, action func(*cli.Context, *serviceclient.Client, string) error) *cli.Command {
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

func serviceClientFromContext(c *cli.Context) (*serviceclient.Client, error) {
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
	return serviceclient.New(serviceclient.Config{
		BaseURL: baseURL, Token: envValue("KOVA_SERVICE_TOKEN"), Kubeconfig: kubeconfig,
		CAFile: caFile, Insecure: insecure,
	})
}

func prepareServiceArchive(input, target string) (string, func(), error) {
	info, err := os.Stat(input)
	if err != nil {
		return "", func() {}, err
	}
	if !info.IsDir() {
		if strings.TrimSpace(target) != "" {
			if err := source.ValidateSingleBuildArchiveTarget(input, target); err != nil {
				return "", func() {}, err
			}
		} else if _, err := source.BuildArchiveTargets(input); err != nil {
			return "", func() {}, err
		}
		return input, func() {}, nil
	}
	if strings.TrimSpace(target) == "" {
		return "", func() {}, fmt.Errorf("job submit requires --target for a context directory")
	}
	tmp, err := os.CreateTemp("", "kova-service-source-*.zip")
	if err != nil {
		return "", func() {}, err
	}
	path := tmp.Name()
	if err := tmp.Close(); err != nil {
		_ = os.Remove(path)
		return "", func() {}, err
	}
	cleanup := func() { _ = os.Remove(path) }
	if err := source.CreateSingleImageArchive(filepath.Clean(input), target, path); err != nil {
		cleanup()
		return "", func() {}, err
	}
	return path, cleanup, nil
}

func writeJSON(c *cli.Context, value any) error {
	encoder := json.NewEncoder(c.App.Writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}
