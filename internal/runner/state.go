package runner

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cofy-x/kova/internal/daemonclient"
)

func (c *Client) Destroy() (err error) {
	ctx, op := runnerOperation("destroy", c.Config)
	defer func() { op.End(err) }()
	if err := c.Config.requireKubeconfig(); err != nil {
		return err
	}
	if err := c.Config.requirePodName(); err != nil {
		return err
	}
	kube, err := c.kubeClient()
	if err != nil {
		return err
	}
	timeout, err := time.ParseDuration(c.Config.WaitTimeout)
	if err != nil {
		return fmt.Errorf("invalid --wait value %q: %w", c.Config.WaitTimeout, err)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := kube.DeletePod(ctx, c.Config.Namespace, c.Config.PodName); err != nil {
		return fmt.Errorf("delete runner pod: %w", err)
	}
	c.clearMatchingState()
	return nil
}

func (c *Client) Status() error {
	raw, err := c.buildStatusJSON()
	if err != nil {
		return err
	}
	fmt.Fprintln(c.Stdout, string(bytes.TrimSpace(raw)))
	return nil
}

func (c *Client) Wait(timeoutSeconds int, intervalSeconds int) (err error) {
	_, op := runnerOperation("wait", c.Config)
	defer func() { op.End(err) }()
	if err := c.Config.requireKubeconfig(); err != nil {
		return err
	}
	if err := c.Config.requirePodName(); err != nil {
		return err
	}
	if intervalSeconds < 1 {
		return fmt.Errorf("--interval must be greater than or equal to 1")
	}
	start := time.Now()
	for {
		raw, err := c.buildStatusJSON()
		if err != nil {
			return err
		}
		fmt.Fprintln(c.Stdout, string(bytes.TrimSpace(raw)))
		state, err := ParseBuildState(raw)
		if err != nil {
			return fmt.Errorf("failed to parse build status from daemon response")
		}
		done, success, err := WaitDecision(state.Status)
		if err != nil {
			return err
		}
		if done {
			if success {
				return nil
			}
			return fmt.Errorf("build finished with status %s", state.Status)
		}
		if deadlineExceeded(start, timeoutSeconds) {
			return fmt.Errorf("wait timed out after %ds", timeoutSeconds)
		}
		time.Sleep(time.Duration(intervalSeconds) * time.Second)
	}
}

func (c *Client) buildStatusJSON() ([]byte, error) {
	if err := c.Config.requirePodName(); err != nil {
		return nil, err
	}
	kube, err := c.kubeClient()
	if err != nil {
		return nil, err
	}
	var stdout, stderr bytes.Buffer
	err = kube.Exec(context.Background(), c.Config.Namespace, c.Config.PodName, kubeExecOptions(
		nil, &stdout, &stderr,
		daemonclient.TransportCommand("GET", daemonclient.StatusPath, "", "")...,
	))
	if err != nil {
		return nil, execError("build status", stderr.Bytes(), err)
	}
	return stdout.Bytes(), nil
}

func (c *Client) cancelBuildJSON() ([]byte, error) {
	kube, err := c.kubeClient()
	if err != nil {
		return nil, err
	}
	var stdout, stderr bytes.Buffer
	err = kube.Exec(context.Background(), c.Config.Namespace, c.Config.PodName, kubeExecOptions(
		nil, &stdout, &stderr,
		daemonclient.TransportCommand("POST", daemonclient.CancelPath, "", "")...,
	))
	if err != nil {
		return nil, execError("cancel build", stderr.Bytes(), err)
	}
	return stdout.Bytes(), nil
}

func (c *Client) clearMatchingState() {
	raw, err := os.ReadFile(c.Config.StateFile)
	if err != nil {
		return
	}
	content := string(raw)
	if strings.Contains(content, "pod="+c.Config.PodName+"\n") && strings.Contains(content, "namespace="+c.Config.Namespace+"\n") {
		_ = os.Remove(c.Config.StateFile)
	}
}
