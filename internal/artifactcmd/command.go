package artifactcmd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/cofy-x/kova/internal/artifactstore"

	"github.com/urfave/cli/v2"
)

func CLICommand() *cli.Command {
	return &cli.Command{
		Name:  "artifact",
		Usage: "read build artifacts from configured storage",
		Subcommands: []*cli.Command{
			{
				Name:  "fetch",
				Usage: "fetch and verify an artifact",
				Flags: append(storeFlags(),
					&cli.StringFlag{Name: "uri", Required: true},
					&cli.StringFlag{Name: "output", Required: true},
					&cli.StringFlag{Name: "digest", Required: true, Usage: "expected sha256 digest"},
				),
				Action: fetch,
			},
		},
	}
}

func storeFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{Name: "artifact-root", Value: artifactstore.DefaultRoot, EnvVars: []string{"KOVA_ARTIFACT_ROOT"}},
		&cli.StringFlag{Name: "s3-endpoint", EnvVars: []string{"KOVA_S3_ENDPOINT"}},
		&cli.StringFlag{Name: "s3-bucket", EnvVars: []string{"KOVA_S3_BUCKET"}},
		&cli.StringFlag{Name: "s3-region", EnvVars: []string{"KOVA_S3_REGION"}},
		&cli.StringFlag{Name: "s3-access-key", EnvVars: []string{"KOVA_S3_ACCESS_KEY"}},
		&cli.StringFlag{Name: "s3-secret-key", EnvVars: []string{"KOVA_S3_SECRET_KEY"}},
		&cli.StringFlag{Name: "s3-session-token", EnvVars: []string{"KOVA_S3_SESSION_TOKEN"}},
		&cli.BoolFlag{Name: "s3-secure", Value: true, EnvVars: []string{"KOVA_S3_SECURE"}},
	}
}

func fetch(c *cli.Context) error {
	u, err := artifactstore.ParseURI(c.String("uri"))
	if err != nil {
		return err
	}
	cfg := artifactstore.Config{
		Driver:       map[string]string{"file": artifactstore.DriverFilesystem, "s3": artifactstore.DriverS3}[u.Scheme],
		Root:         c.String("artifact-root"),
		S3Endpoint:   c.String("s3-endpoint"),
		S3Bucket:     c.String("s3-bucket"),
		S3Region:     c.String("s3-region"),
		S3AccessKey:  c.String("s3-access-key"),
		S3SecretKey:  c.String("s3-secret-key"),
		S3SessionKey: c.String("s3-session-token"),
		S3Secure:     c.Bool("s3-secure"),
	}
	store, err := artifactstore.New(cfg)
	if err != nil {
		return err
	}
	return fetchToFile(c.Context, store, c.String("uri"), c.String("output"), c.String("digest"))
}

func fetchToFile(ctx context.Context, store artifactstore.Store, uri, output, expected string) error {
	src, err := store.Open(ctx, uri)
	if err != nil {
		return err
	}
	defer src.Close()
	if err := os.MkdirAll(filepath.Dir(output), 0o750); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(output), ".artifact-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	hash := sha256.New()
	if _, err := io.Copy(io.MultiWriter(tmp, hash), src); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	actual := "sha256:" + hex.EncodeToString(hash.Sum(nil))
	if !strings.EqualFold(actual, strings.TrimSpace(expected)) {
		return fmt.Errorf("artifact digest mismatch: expected %s, got %s", expected, actual)
	}
	if err := os.Rename(tmpPath, output); err != nil {
		return err
	}
	return nil
}
