package runner

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/cofy-x/kova/internal/daemonclient"
	kubeapi "github.com/cofy-x/kova/internal/kube"
)

func (c *Client) Preheat(args []string) (err error) {
	ctx, op := runnerOperation("preheat", c.Config)
	defer func() { op.End(err) }()
	if err := c.Config.requireKubeconfig(); err != nil {
		return err
	}
	if err := c.Config.requirePodName(); err != nil {
		return err
	}
	query, err := PreheatQuery(args)
	if err != nil {
		return err
	}
	kube, err := c.kubeClient()
	if err != nil {
		return err
	}
	var stdout, stderr bytes.Buffer
	err = kube.Exec(ctx, c.Config.Namespace, c.Config.PodName, kubeExecOptions(
		nil, &stdout, &stderr,
		daemonclient.TransportCommand("POST", daemonclient.PreheatPath, query, "")...,
	))
	if stdout.Len() > 0 {
		fmt.Fprintln(c.Stdout, string(bytes.TrimSpace(stdout.Bytes())))
	}
	if err != nil {
		if stderr.Len() == 0 && stdout.Len() > 0 {
			stderr.Write(stdout.Bytes())
		}
		return execError("preheat request", stderr.Bytes(), err)
	}
	return nil
}

func (c *Client) List(args []string) error {
	if err := c.Config.requireKubeconfig(); err != nil {
		return err
	}
	wide, err := parseListArgs(args)
	if err != nil {
		return err
	}
	kube, err := c.kubeClient()
	if err != nil {
		return err
	}
	return kube.ListPods(context.Background(), c.Config.Namespace, c.Stdout, wide)
}

func (c *Client) Logs(args []string) error {
	if err := c.Config.requireKubeconfig(); err != nil {
		return err
	}
	if err := c.Config.requirePodName(); err != nil {
		return err
	}
	tailLines, err := parseLogsArgs(args)
	if err != nil {
		return err
	}
	kube, err := c.kubeClient()
	if err != nil {
		return err
	}
	return kube.WritePodLogsTail(context.Background(), c.Config.Namespace, c.Config.PodName, tailLines, c.Stdout)
}

func (c *Client) Exec(args []string) error {
	if err := c.Config.requireKubeconfig(); err != nil {
		return err
	}
	if err := c.Config.requirePodName(); err != nil {
		return err
	}
	if len(args) == 0 {
		return fmt.Errorf("exec requires a command")
	}
	kube, err := c.kubeClient()
	if err != nil {
		return err
	}
	return kube.Exec(context.Background(), c.Config.Namespace, c.Config.PodName, kubeExecOptions(nonTerminalStdin(c.Stdin), c.Stdout, c.Stderr, args...))
}

func (c *Client) Scale(target string) (err error) {
	ctx, op := runnerOperation("scale", c.Config)
	defer func() { op.End(err) }()
	if err := c.Config.requireKubeconfig(); err != nil {
		return err
	}
	if target == "" {
		kube, err := c.kubeClient()
		if err != nil {
			return err
		}
		return kube.ListPodsWithOptions(ctx, kovaWorkerNamespace, c.Stdout, kubeapi.ListPodsOptions{
			Wide:          true,
			LabelSelector: kovaWorkerLabelSelector,
		})
	}
	replicas, err := parseReplicaCount(target)
	if err != nil {
		return err
	}
	kube, err := c.kubeClient()
	if err != nil {
		return err
	}
	if err := kube.ScaleDeployment(ctx, kovaWorkerNamespace, kovaWorkerDeployment, replicas); err != nil {
		return err
	}
	fmt.Fprintf(c.Stdout, "deployment.apps/%s scaled\n", kovaWorkerDeployment)
	return nil
}

func parseReplicaCount(target string) (int32, error) {
	replicas, err := strconv.ParseInt(target, 10, 32)
	if err != nil || replicas < 0 {
		return 0, fmt.Errorf("--target must be a non-negative 32-bit integer")
	}
	return int32(replicas), nil
}

const kovaWorkerLabelSelector = "app.kubernetes.io/name=kova,app.kubernetes.io/instance=kova"

func parseListArgs(args []string) (bool, error) {
	wide := false
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "-o":
			if i+1 >= len(args) {
				return false, fmt.Errorf("-o requires a value")
			}
			if args[i+1] != "wide" {
				return false, fmt.Errorf("list only supports -o wide")
			}
			wide = true
			i++
		case arg == "-o=wide" || arg == "--output=wide":
			wide = true
		case arg == "--output":
			if i+1 >= len(args) {
				return false, fmt.Errorf("--output requires a value")
			}
			if args[i+1] != "wide" {
				return false, fmt.Errorf("list only supports --output wide")
			}
			wide = true
			i++
		default:
			return false, fmt.Errorf("unsupported list argument %q", arg)
		}
	}
	return wide, nil
}

func parseLogsArgs(args []string) (int64, error) {
	tailLines := int64(100)
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--tail":
			if i+1 >= len(args) {
				return 0, fmt.Errorf("--tail requires a value")
			}
			parsed, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil || parsed < 0 {
				return 0, fmt.Errorf("--tail must be a non-negative integer")
			}
			tailLines = parsed
			i++
		case strings.HasPrefix(arg, "--tail="):
			parsed, err := strconv.ParseInt(strings.TrimPrefix(arg, "--tail="), 10, 64)
			if err != nil || parsed < 0 {
				return 0, fmt.Errorf("--tail must be a non-negative integer")
			}
			tailLines = parsed
		default:
			return 0, fmt.Errorf("unsupported logs argument %q", arg)
		}
	}
	return tailLines, nil
}
