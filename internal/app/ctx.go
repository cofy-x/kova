package app

import (
	"encoding/json"
	"fmt"
	"strings"
	"text/tabwriter"

	"github.com/cofy-x/kova/internal/ctxconfig"

	cli "github.com/urfave/cli/v2"
)

func ctxCLICommand() *cli.Command {
	return &cli.Command{
		Name:  "ctx",
		Usage: "manage local Kova contexts",
		Subcommands: []*cli.Command{
			ctxListCLICommand(),
			ctxCurrentCLICommand(),
			ctxUseCLICommand(),
			ctxShowCLICommand(),
			ctxSetCLICommand(),
			ctxDeleteCLICommand(),
		},
	}
}

func ctxListCLICommand() *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "list local Kova contexts",
		Action: func(c *cli.Context) error {
			cfg, err := loadCtxConfig(c)
			if err != nil {
				return err
			}
			writer := tabwriter.NewWriter(c.App.Writer, 0, 0, 2, ' ', 0)
			fmt.Fprintln(writer, "CURRENT\tNAME\tMODE\tENDPOINT")
			for _, name := range cfg.Names() {
				mark := ""
				if name == cfg.Current {
					mark = "*"
				}
				ctx := cfg.Contexts[name]
				endpoint := ctx.ServiceURL
				if ctx.EffectiveMode() == ctxconfig.ModeDirect {
					endpoint = ctx.BuildkitAddr
				}
				fmt.Fprintf(writer, "%s\t%s\t%s\t%s\n", mark, name, ctx.EffectiveMode(), endpoint)
			}
			return writer.Flush()
		},
	}
}

func ctxCurrentCLICommand() *cli.Command {
	return &cli.Command{
		Name:  "current",
		Usage: "print the current local Kova context",
		Action: func(c *cli.Context) error {
			cfg, err := loadCtxConfig(c)
			if err != nil {
				return err
			}
			if strings.TrimSpace(cfg.Current) == "" {
				return fmt.Errorf("no current ctx is set")
			}
			fmt.Fprintln(c.App.Writer, cfg.Current)
			return nil
		},
	}
}

func ctxUseCLICommand() *cli.Command {
	return &cli.Command{
		Name:      "use",
		Usage:     "set the current local Kova context",
		ArgsUsage: "<name>",
		Action: func(c *cli.Context) error {
			name, err := oneCtxNameArg(c)
			if err != nil {
				return err
			}
			cfg, err := loadCtxConfig(c)
			if err != nil {
				return err
			}
			if _, ok := cfg.Contexts[name]; !ok {
				return fmt.Errorf("ctx %q does not exist", name)
			}
			cfg.Current = name
			if err := ctxconfig.Save(c.String("ctx-config"), cfg); err != nil {
				return err
			}
			fmt.Fprintf(c.App.Writer, "current ctx: %s\n", name)
			return nil
		},
	}
}

func ctxShowCLICommand() *cli.Command {
	return &cli.Command{
		Name:      "show",
		Usage:     "show a local Kova context",
		ArgsUsage: "[name]",
		Action: func(c *cli.Context) error {
			cfg, err := loadCtxConfig(c)
			if err != nil {
				return err
			}
			requested := strings.TrimSpace(c.Args().First())
			name, ctx, ok := cfg.Resolve(requested)
			if !ok {
				if requested != "" {
					return fmt.Errorf("ctx %q does not exist", requested)
				}
				return fmt.Errorf("ctx is not set")
			}
			payload := struct {
				Name    string            `json:"name"`
				Current bool              `json:"current"`
				Context ctxconfig.Context `json:"context"`
			}{Name: name, Current: name == cfg.Current, Context: ctx}
			raw, err := json.MarshalIndent(payload, "", "  ")
			if err != nil {
				return err
			}
			fmt.Fprintln(c.App.Writer, string(raw))
			return nil
		},
	}
}

func ctxSetCLICommand() *cli.Command {
	return &cli.Command{
		Name:      "set",
		Usage:     "create or update a local Kova context",
		ArgsUsage: "<name>",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "mode", Usage: "context mode: direct or service"},
			&cli.StringFlag{Name: "kubeconfig", Usage: "path to kubeconfig"},
			&cli.StringFlag{Name: "namespace", Usage: "runner namespace"},
			&cli.StringFlag{Name: "buildkit-addr", Usage: "default BuildKit address"},
			&cli.StringFlag{Name: "image", Usage: "default runner image"},
			&cli.StringFlag{Name: "image-pull-policy", Usage: "default runner image pull policy"},
			&cli.StringFlag{Name: "image-pull-secret", Usage: "default runner image pull secret name"},
			&cli.StringFlag{Name: "service-url", Usage: "Kova service base URL"},
			&cli.StringFlag{Name: "service-ca-file", Usage: "CA bundle for the Kova service"},
			&cli.BoolFlag{Name: "service-insecure", Usage: "skip Kova service TLS verification"},
			&cli.BoolFlag{Name: "use", Usage: "make this context current"},
		},
		Action: func(c *cli.Context) error {
			name, err := oneCtxNameArg(c)
			if err != nil {
				return err
			}
			cfg, err := loadCtxConfig(c)
			if err != nil {
				return err
			}
			if cfg.Contexts == nil {
				cfg.Contexts = map[string]ctxconfig.Context{}
			}
			next := cfg.Contexts[name]
			if c.IsSet("mode") {
				next.Mode = strings.TrimSpace(c.String("mode"))
			}
			if c.IsSet("kubeconfig") {
				next.Kubeconfig = strings.TrimSpace(c.String("kubeconfig"))
			}
			if c.IsSet("namespace") {
				next.Namespace = strings.TrimSpace(c.String("namespace"))
			}
			if c.IsSet("buildkit-addr") {
				next.BuildkitAddr = strings.TrimSpace(c.String("buildkit-addr"))
			}
			if c.IsSet("image") {
				next.RunnerImage = strings.TrimSpace(c.String("image"))
			}
			if c.IsSet("image-pull-policy") {
				next.RunnerImagePullPolicy = strings.TrimSpace(c.String("image-pull-policy"))
			}
			if c.IsSet("image-pull-secret") {
				next.ImagePullSecret = strings.TrimSpace(c.String("image-pull-secret"))
			}
			if c.IsSet("service-url") {
				next.ServiceURL = strings.TrimSpace(c.String("service-url"))
			}
			if c.IsSet("service-ca-file") {
				next.ServiceCAFile = strings.TrimSpace(c.String("service-ca-file"))
			}
			if c.IsSet("service-insecure") {
				next.ServiceInsecure = c.Bool("service-insecure")
			}
			if next.Mode == "" {
				next.Mode = ctxconfig.ModeDirect
			}
			if err := next.Validate(); err != nil {
				return err
			}
			cfg.Contexts[name] = next
			if c.Bool("use") || cfg.Current == "" {
				cfg.Current = name
			}
			if err := ctxconfig.Save(c.String("ctx-config"), cfg); err != nil {
				return err
			}
			fmt.Fprintf(c.App.Writer, "saved ctx: %s\n", name)
			return nil
		},
	}
}

func ctxDeleteCLICommand() *cli.Command {
	return &cli.Command{
		Name:      "delete",
		Aliases:   []string{"rm"},
		Usage:     "delete a local Kova context",
		ArgsUsage: "<name>",
		Action: func(c *cli.Context) error {
			name, err := oneCtxNameArg(c)
			if err != nil {
				return err
			}
			cfg, err := loadCtxConfig(c)
			if err != nil {
				return err
			}
			if _, ok := cfg.Contexts[name]; !ok {
				return fmt.Errorf("ctx %q does not exist", name)
			}
			delete(cfg.Contexts, name)
			if cfg.Current == name {
				cfg.Current = ""
			}
			if err := ctxconfig.Save(c.String("ctx-config"), cfg); err != nil {
				return err
			}
			fmt.Fprintf(c.App.Writer, "deleted ctx: %s\n", name)
			return nil
		},
	}
}

func oneCtxNameArg(c *cli.Context) (string, error) {
	if c.NArg() != 1 {
		return "", fmt.Errorf("ctx name is required")
	}
	return ctxconfig.ValidateName(c.Args().First())
}
