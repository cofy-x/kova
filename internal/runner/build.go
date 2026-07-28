package runner

import (
	"bytes"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cofy-x/kova/internal/daemonclient"
	"github.com/cofy-x/kova/internal/source"
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
	args, input, cleanup, err := prepareBuildInput(args)
	if cleanup != nil {
		defer cleanup()
	}
	if err != nil {
		return err
	}
	if input == nil && stdinIsTerminal(c.Stdin) {
		return fmt.Errorf("build requires a .zip stream on stdin")
	}
	if input == nil {
		input = c.Stdin
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
		input, &stdout, &stderr,
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

func prepareBuildInput(args []string) ([]string, io.Reader, func(), error) {
	positional := -1
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if strings.HasPrefix(arg, "--") {
			if buildFlagTakesValue(arg) && !strings.Contains(arg, "=") {
				i++
			}
			continue
		}
		if positional >= 0 {
			return nil, nil, nil, fmt.Errorf("build accepts at most one positional argument")
		}
		positional = i
	}
	if positional < 0 || !isDir(args[positional]) {
		return args, nil, nil, nil
	}

	target := buildTargetArg(args)
	tmpZip, err := os.CreateTemp("", "kova-source-*.zip")
	if err != nil {
		return nil, nil, nil, err
	}
	tmpPath := tmpZip.Name()
	if err := tmpZip.Close(); err != nil {
		os.Remove(tmpPath)
		return nil, nil, nil, err
	}
	if err := source.CreateSingleImageArchive(args[positional], target, tmpPath); err != nil {
		os.Remove(tmpPath)
		return nil, nil, nil, err
	}
	file, err := os.Open(tmpPath)
	if err != nil {
		os.Remove(tmpPath)
		return nil, nil, nil, err
	}
	cleanup := func() {
		file.Close()
		os.Remove(tmpPath)
	}
	trimmed := append([]string{}, args[:positional]...)
	trimmed = append(trimmed, args[positional+1:]...)
	return trimmed, file, cleanup, nil
}

func buildFlagTakesValue(arg string) bool {
	switch arg {
	case "--var", "--target", "--format", "--concurrency", "--oom-cooldown", "--timeout", "--retry":
		return true
	default:
		return false
	}
}

func buildTargetArg(args []string) string {
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--target" && i+1 < len(args):
			return args[i+1]
		case strings.HasPrefix(arg, "--target="):
			return strings.TrimPrefix(arg, "--target=")
		}
	}
	return ""
}

func isDir(path string) bool {
	info, err := os.Stat(filepath.Clean(path))
	return err == nil && info.IsDir()
}

func stdinIsTerminal(stdin io.Reader) bool {
	file, ok := stdin.(*os.File)
	if !ok {
		return false
	}
	stat, err := file.Stat()
	return err == nil && stat.Mode()&os.ModeCharDevice != 0
}
