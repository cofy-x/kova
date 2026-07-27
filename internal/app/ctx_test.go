package app

import (
	"bytes"
	"flag"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cofy-x/kova/internal/ctxconfig"

	cli "github.com/urfave/cli/v2"
)

func TestCtxSetUseAndCurrent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	var out bytes.Buffer
	app := NewCLIApp()
	app.Writer = &out
	app.ErrWriter = &out

	err := app.Run([]string{
		"kova",
		"--ctx-config", path,
		"ctx", "set",
		"--kubeconfig", "/tmp/kind.kubeconfig",
		"--namespace", "default",
		"--buildkit-addr", "tcp://kova.kova.svc:9094",
		"--image", "localhost:5001/kova:dev",
		"--image-pull-policy", "IfNotPresent",
		"--image-pull-secret", "",
		"--use",
		"kind",
	})
	if err != nil {
		t.Fatalf("ctx set: %v", err)
	}

	cfg, err := ctxconfig.Load(path)
	if err != nil {
		t.Fatalf("load ctx config: %v", err)
	}
	if cfg.Current != "kind" {
		t.Fatalf("current = %q, want kind", cfg.Current)
	}
	ctx := cfg.Contexts["kind"]
	if ctx.Kubeconfig != "/tmp/kind.kubeconfig" || ctx.Namespace != "default" || ctx.ImagePullSecret != "" {
		t.Fatalf("ctx = %#v", ctx)
	}

	out.Reset()
	err = app.Run([]string{"kova", "--ctx-config", path, "ctx", "current"})
	if err != nil {
		t.Fatalf("ctx current: %v", err)
	}
	if strings.TrimSpace(out.String()) != "kind" {
		t.Fatalf("current output = %q, want kind", out.String())
	}
}

func TestRunnerConfigUsesCurrentCtx(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	err := ctxconfig.Save(path, ctxconfig.Config{
		Current: "remote",
		Contexts: map[string]ctxconfig.Context{
			"remote": {
				Kubeconfig:   "/tmp/remote.kubeconfig",
				Namespace:    "kova",
				BuildkitAddr: "tcp://remote-kova.kova.svc:9094",
			},
		},
	})
	if err != nil {
		t.Fatalf("save ctx config: %v", err)
	}

	set := flag.NewFlagSet("kova", flag.ContinueOnError)
	set.String("ctx-config", path, "")
	set.String("ctx", "", "")
	c := cli.NewContext(NewCLIApp(), set, nil)

	cfg, err := runnerConfigFromContext(c)
	if err != nil {
		t.Fatalf("runnerConfigFromContext: %v", err)
	}
	if cfg.Kubeconfig != "/tmp/remote.kubeconfig" {
		t.Fatalf("kubeconfig = %q", cfg.Kubeconfig)
	}
	if cfg.Namespace != "kova" {
		t.Fatalf("namespace = %q", cfg.Namespace)
	}
	if cfg.BuildkitAddr != "tcp://remote-kova.kova.svc:9094" {
		t.Fatalf("buildkit addr = %q", cfg.BuildkitAddr)
	}
}

func TestRunnerConfigFlagOverridesCtx(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	err := ctxconfig.Save(path, ctxconfig.Config{
		Current:  "kind",
		Contexts: map[string]ctxconfig.Context{"kind": {Namespace: "ctx-ns"}},
	})
	if err != nil {
		t.Fatalf("save ctx config: %v", err)
	}

	set := flag.NewFlagSet("kova", flag.ContinueOnError)
	set.String("ctx-config", path, "")
	set.String("ctx", "", "")
	set.String("namespace", "", "")
	if err := set.Set("namespace", "flag-ns"); err != nil {
		t.Fatalf("set namespace: %v", err)
	}
	c := cli.NewContext(NewCLIApp(), set, nil)

	cfg, err := runnerConfigFromContext(c)
	if err != nil {
		t.Fatalf("runnerConfigFromContext: %v", err)
	}
	if cfg.Namespace != "flag-ns" {
		t.Fatalf("namespace = %q, want flag-ns", cfg.Namespace)
	}
}

func TestRunnerConfigReturnsErrorForMissingExplicitCtx(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := ctxconfig.Save(path, ctxconfig.Config{}); err != nil {
		t.Fatalf("save ctx config: %v", err)
	}

	set := flag.NewFlagSet("kova", flag.ContinueOnError)
	set.String("ctx-config", path, "")
	set.String("ctx", "", "")
	if err := set.Set("ctx", "missing"); err != nil {
		t.Fatalf("set ctx: %v", err)
	}
	c := cli.NewContext(NewCLIApp(), set, nil)

	if _, err := runnerConfigFromContext(c); err == nil {
		t.Fatal("expected missing ctx error")
	}
}

func TestPrepareImageOptionsUseCtxEmptyPullSecret(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	err := ctxconfig.Save(path, ctxconfig.Config{
		Current: "kind",
		Contexts: map[string]ctxconfig.Context{
			"kind": {
				RunnerImage:           "localhost:5001/kova:dev",
				RunnerImagePullPolicy: "IfNotPresent",
				ImagePullSecret:       "",
			},
		},
	})
	if err != nil {
		t.Fatalf("save ctx config: %v", err)
	}

	set := flag.NewFlagSet("kova", flag.ContinueOnError)
	set.String("ctx-config", path, "")
	set.String("ctx", "", "")
	c := cli.NewContext(NewCLIApp(), set, nil)

	ctx, hasCtx, err := runnerCtxFromCLI(c)
	if err != nil {
		t.Fatalf("runnerCtxFromCLI: %v", err)
	}
	image, policy, secret := prepareImageOptionsFromContext(c, ctx, hasCtx)
	if image != "localhost:5001/kova:dev" {
		t.Fatalf("image = %q", image)
	}
	if policy != "IfNotPresent" {
		t.Fatalf("policy = %q", policy)
	}
	if secret != "" {
		t.Fatalf("secret = %q, want empty", secret)
	}
}

func TestPrepareImageOptionsKeepDefaultsWhenCtxImageFieldsEmpty(t *testing.T) {
	t.Setenv("KOVA_IMAGE", "localhost:5001/kova:env")
	t.Setenv("KOVA_IMAGE_PULL_POLICY", "IfNotPresent")
	path := filepath.Join(t.TempDir(), "config.json")
	err := ctxconfig.Save(path, ctxconfig.Config{
		Current:  "kind",
		Contexts: map[string]ctxconfig.Context{"kind": {Kubeconfig: "/tmp/kind.kubeconfig"}},
	})
	if err != nil {
		t.Fatalf("save ctx config: %v", err)
	}

	set := flag.NewFlagSet("kova", flag.ContinueOnError)
	set.String("ctx-config", path, "")
	set.String("ctx", "", "")
	c := cli.NewContext(NewCLIApp(), set, nil)

	ctx, hasCtx, err := runnerCtxFromCLI(c)
	if err != nil {
		t.Fatalf("runnerCtxFromCLI: %v", err)
	}
	image, policy, _ := prepareImageOptionsFromContext(c, ctx, hasCtx)
	if image != "localhost:5001/kova:env" {
		t.Fatalf("image = %q, want env image", image)
	}
	if policy != "IfNotPresent" {
		t.Fatalf("policy = %q, want env policy", policy)
	}
}
