package daemon

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/cofy-x/kova/internal/batch"
	"github.com/cofy-x/kova/internal/logging"

	"github.com/urfave/cli/v2"
)

const (
	defaultDaemonSocket = "/tmp/kova.sock"
	daemonResultDB      = "/tmp/result.lmdb"
	daemonLogsFile      = "/tmp/logs.jsonl"
	daemonImageDir      = "/tmp/kova-images"
)

type daemonState struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

type serverBackend struct {
	runBuild             func(batch.Options) error
	runExport            func(batch.Options) error
	runPreheat           func(batch.Options) error
	validateBuildArchive func(string) (int, error)
	extractZip           func(string, string) error
}

type daemonServer struct {
	defaultAddrs string
	resultDB     string
	logsFile     string
	backend      serverBackend

	mu          sync.RWMutex
	build       daemonState
	buildCancel context.CancelFunc
	buildDone   chan struct{}
}

func CLICommand() *cli.Command {
	return &cli.Command{
		Name:  "daemon",
		Usage: "start HTTP server on a unix domain socket for build/export/preheat",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "socket", Value: defaultDaemonSocket, Usage: "unix socket path"},
			&cli.StringFlag{Name: "addrs", Usage: "default comma-separated buildkitd addresses"},
			&cli.StringFlag{Name: "auth", Usage: "base64-encoded registry auth JSON; written under the runtime user's home directory"},
		},
		Action: func(c *cli.Context) error {
			if auth := c.String("auth"); auth != "" {
				if err := writeDockerAuth(auth); err != nil {
					return err
				}
			}
			return runDaemon(c.String("socket"), c.String("addrs"))
		},
	}
}

func writeDockerAuth(b64 string) error {
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return fmt.Errorf("decode --auth: %w", err)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	dir := filepath.Join(home, ".docker")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	p := filepath.Join(dir, "config.json")
	if err := os.WriteFile(p, raw, 0o600); err != nil {
		return err
	}
	logging.Infof("Wrote docker config to %s (%d bytes)", p, len(raw))
	return nil
}
