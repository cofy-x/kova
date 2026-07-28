package runner

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/cofy-x/kova/internal/daemonclient"
)

func (c *Client) Export(args []string) (err error) {
	ctx, op := runnerOperation("export", c.Config)
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
	query, localResult, err := ExportQuery(args)
	if err != nil {
		return err
	}
	localDir := filepath.Dir(localResult)
	if localDir != "." {
		if err := os.MkdirAll(localDir, 0o755); err != nil {
			return err
		}
	}
	tmp, err := os.CreateTemp(localDir, filepath.Base(localResult)+".tmp.*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	_ = tmp.Close()
	defer os.Remove(tmpPath)

	out, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	cmdErr := kube.Exec(ctx, c.Config.Namespace, c.Config.PodName, kubeExecOptions(
		nil, out, c.Stderr,
		daemonclient.TransportCommand("POST", daemonclient.ExportPath, query, "")...,
	))
	closeErr := out.Close()
	if cmdErr != nil {
		return fmt.Errorf("export request failed: %w", cmdErr)
	}
	if closeErr != nil {
		return closeErr
	}
	if err := os.Rename(tmpPath, localResult); err != nil {
		return err
	}
	fmt.Fprintf(c.Stderr, "Exported to %s\n", localResult)
	return nil
}
