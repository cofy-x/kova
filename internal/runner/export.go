package runner

import (
	"fmt"
	"os"
	"path/filepath"
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

	script := `set -eu
socket=$1
url=$2
body=$(mktemp)
cleanup() { rm -f "$body"; }
trap cleanup EXIT
code=$(curl -sS -o "$body" -w "%{http_code}" -X POST --unix-socket "$socket" "$url")
if [ "$code" -lt 200 ] || [ "$code" -ge 300 ]; then
  cat "$body" >&2
  exit 1
fi
cat "$body"`
	out, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	cmdErr := kube.Exec(ctx, c.Config.Namespace, c.Config.PodName, kubeExecOptions(
		nil, out, c.Stderr,
		"sh", "-lc", script, "sh",
		daemonSocket, "http://localhost/api/v1/export?"+query,
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
