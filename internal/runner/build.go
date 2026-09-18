package runner

import (
	"bytes"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/cofy-x/kova/internal/daemonclient"
)

func (c *Client) Build(args []string) (err error) {
	ctx, op := runnerOperation("build", c.Config)
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
	if stdinIsTerminal(c.Stdin) {
		return fmt.Errorf("build requires a .zip stream on stdin")
	}
	kube, err := c.kubeClient()
	if err != nil {
		return err
	}
	query, err := BuildQuery(args)
	if err != nil {
		return err
	}
	stateRaw, err := c.buildStatusJSON()
	if err != nil {
		return err
	}
	state, _ := ParseBuildState(stateRaw)
	if state.Status == "running" {
		fmt.Fprintln(c.Stderr, "Cancelling existing build before starting a new one")
		if _, err := c.cancelBuildJSON(); err != nil {
			return err
		}
		for {
			stateRaw, err = c.buildStatusJSON()
			if err != nil {
				return err
			}
			state, _ = ParseBuildState(stateRaw)
			if state.Status != "running" {
				break
			}
			time.Sleep(time.Duration(c.Config.WaitBuildIntervalSeconds) * time.Second)
		}
	}
	values := "addrs=" + url.QueryEscape(strings.TrimSpace(c.Config.BuildkitAddr))
	if query != "" {
		values += "&" + query
	}
	var stdout, stderr bytes.Buffer
	err = kube.Exec(ctx, c.Config.Namespace, c.Config.PodName, kubeExecOptions(
		c.Stdin, &stdout, &stderr,
		daemonclient.TransportCommand("POST", daemonclient.BuildPath, values, "")...,
	))
	if stdout.Len() > 0 {
		fmt.Fprintln(c.Stderr, string(bytes.TrimSpace(stdout.Bytes())))
	}
	if err != nil {
		return execError("build request", stderr.Bytes(), err)
	}
	state, err = ParseBuildState(stdout.Bytes())
	if err == nil && (state.Status == "failed" || state.Status == "error") {
		return fmt.Errorf("build request failed: %s", strings.TrimSpace(stdout.String()))
	}
	return nil
}

func stdinIsTerminal(stdin io.Reader) bool {
	file, ok := stdin.(*os.File)
	if !ok {
		return false
	}
	stat, err := file.Stat()
	return err == nil && stat.Mode()&os.ModeCharDevice != 0
}
