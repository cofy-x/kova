package runner

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"time"
)

func (c *Client) Prepare(image string, imagePullPolicy string, imagePullSecret string) (err error) {
	ctx, op := runnerOperation("prepare", c.Config)
	defer func() { op.End(err) }()
	if err := c.Config.requireKubeconfig(); err != nil {
		return err
	}
	if err := c.Config.requirePodName(); err != nil {
		return err
	}
	if err := c.Config.requireBuildkitAddr(); err != nil {
		return err
	}
	if imagePullPolicy == "" {
		imagePullPolicy = c.Config.RunnerImagePullPolicy
	}
	if strings.TrimSpace(image) == "" {
		image = c.Config.RunnerImage
	}
	kube, err := c.kubeClient()
	if err != nil {
		return err
	}
	resolvedImage, err := ResolveRunnerImage(ctx, kube, c.Config.Namespace, imagePullSecret, image)
	if err != nil {
		return err
	}
	exists, err := kube.PodExists(ctx, c.Config.Namespace, c.Config.PodName)
	if err != nil {
		return err
	}
	if exists {
		_ = os.Remove(c.Config.StateFile)
		return fmt.Errorf("pod %s already exists in namespace %s", c.Config.PodName, c.Config.Namespace)
	}

	pod := PreparePod(ManifestOptions{
		PodName:         c.Config.PodName,
		Namespace:       c.Config.Namespace,
		Image:           resolvedImage,
		ImagePullPolicy: imagePullPolicy,
		ImagePullSecret: imagePullSecret,
		BuildkitAddr:    strings.TrimSpace(c.Config.BuildkitAddr),
		PprofServer:     c.Config.DaemonPprofServer,
		Env:             c.Config.DaemonEnv,
	})
	if err := kube.CreatePod(ctx, &pod); err != nil {
		return fmt.Errorf("create runner pod: %w", err)
	}
	waitTimeout, err := time.ParseDuration(c.Config.WaitTimeout)
	if err != nil {
		return fmt.Errorf("invalid --wait value %q: %w", c.Config.WaitTimeout, err)
	}
	if err := kube.WaitPodReady(ctx, c.Config.Namespace, c.Config.PodName, waitTimeout); err != nil {
		return fmt.Errorf("wait runner pod ready: %w", err)
	}
	if err := c.waitDaemonReady(); err != nil {
		_ = kube.WritePodLogsTail(ctx, c.Config.Namespace, c.Config.PodName, 50, c.Stderr)
		return err
	}
	_ = os.WriteFile(c.Config.StateFile, []byte(fmt.Sprintf("pod=%s\nnamespace=%s\n", c.Config.PodName, c.Config.Namespace)), 0o600)
	fmt.Fprintln(c.Stdout, c.Config.PodName)
	return nil
}

func (c *Client) waitDaemonReady() error {
	kube, err := c.kubeClient()
	if err != nil {
		return err
	}
	var lastErr error
	var lastStderr []byte
	for i := 0; i < c.Config.DaemonReadyTimeoutSeconds; i++ {
		var stdout, stderr bytes.Buffer
		if err := kube.Exec(context.Background(), c.Config.Namespace, c.Config.PodName, kubeExecOptions(
			nil, &stdout, &stderr,
			"curl", "-sS", "--max-time", "2",
			"--unix-socket", daemonSocket,
			"http://localhost/api/v1/health",
		)); err == nil {
			return nil
		} else {
			lastErr = err
			lastStderr = bytes.TrimSpace(stderr.Bytes())
		}
		time.Sleep(time.Second)
	}
	if lastErr != nil {
		if len(lastStderr) > 0 {
			return fmt.Errorf("daemon did not become ready within %ds: %w: %s", c.Config.DaemonReadyTimeoutSeconds, lastErr, lastStderr)
		}
		return fmt.Errorf("daemon did not become ready within %ds: %w", c.Config.DaemonReadyTimeoutSeconds, lastErr)
	}
	return fmt.Errorf("daemon did not become ready within %ds", c.Config.DaemonReadyTimeoutSeconds)
}
