package batch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/cofy-x/kova/internal/logging"
	"github.com/cofy-x/kova/internal/observability"
	"github.com/cofy-x/kova/internal/scheduler"
	"github.com/cofy-x/kova/internal/source"
	"github.com/cofy-x/kova/internal/store"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1"

	"go.opentelemetry.io/otel/attribute"
)

func executeBuild(ctx context.Context, spec source.Spec, addr *scheduler.Addr, opts Options) store.Entry {
	mode := "nydus"
	if source.FormatIsOCI(spec.Format) {
		mode = "oci"
	}
	ctx, op := observability.StartOperation(ctx, observability.OperationConfig{
		Name: "kova.batch.build.target",
		SpanAttrs: []attribute.KeyValue{
			observability.StringAttr(observability.AttrTarget, spec.Target),
			attribute.String(observability.AttrMode, mode),
			observability.StringAttr(observability.AttrWorkerAddr, addr.Addr),
		},
		MetricAttrs: []attribute.KeyValue{
			attribute.String(observability.AttrMode, mode),
			observability.StringAttr(observability.AttrWorkerAddr, addr.Addr),
		},
		Counter:  observability.Instrument{Name: "kova_build_targets_total", Description: "Build target attempts"},
		Duration: observability.Instrument{Name: "kova_build_target_duration_seconds", Description: "Build target duration"},
	})
	var buildErr error
	defer func() { op.End(buildErr) }()

	startedAt := time.Now()
	nodeIP := addr.NodeIP
	if strings.TrimSpace(nodeIP) == "" {
		nodeIP = scheduler.NodeIPFromAddr(addr.Addr)
	}
	if nodeIP == "" {
		nodeIP = "unknown"
	}

	var buildCtx context.Context
	var buildCancel context.CancelFunc
	if opts.Timeout > 0 {
		buildCtx, buildCancel = context.WithTimeout(ctx, time.Duration(opts.Timeout)*time.Second)
	} else {
		buildCtx, buildCancel = context.WithCancel(ctx)
	}
	defer buildCancel()

	var outputBuf bytes.Buffer
	digest, err := runBuildCommands(buildCtx, spec, addr, opts, &outputBuf)
	buildErr = err
	finishedAt := time.Now()
	elapsed := finishedAt.Sub(startedAt)

	entry := store.Entry{
		StartedAt:      startedAt.Format(time.RFC3339),
		FinishedAt:     finishedAt.Format(time.RFC3339),
		Elapsed:        logging.FormatElapsed(elapsed),
		Target:         spec.Target,
		NodeIP:         nodeIP,
		ManifestDigest: digest,
		Success:        err == nil,
	}

	if err != nil {
		entry.Logs = outputBuf.String()
		entry.Reason = err.Error()
		op.SetResult(observability.ResultError)
		op.SetErrorClass("build_command_failed")

		output := strings.ToLower(outputBuf.String())
		if strings.Contains(output, "connection refused") {
			logging.Infof("Detected OOM-style failure for %s, cooling down %s for %s",
				spec.Target, addr.Addr, addr.Cooldown)
			addr.SetCooldown()
		}
	}

	return entry
}

func runBuildCommands(ctx context.Context, spec source.Spec, addr *scheduler.Addr, opts Options, outputBuf *bytes.Buffer) (string, error) {
	if source.FormatIsOCI(spec.Format) {
		return runBuildctl(ctx, spec, addr, opts, outputBuf)
	}

	ociSpec := spec
	ociSpec.Target = source.StripNydusV3Suffix(spec.Target)
	ociDigest, err := runBuildctl(ctx, ociSpec, addr, opts, outputBuf)
	if err != nil {
		return "", err
	}
	ref, err := name.ParseReference(ociSpec.Target, name.WeakValidation)
	if err != nil {
		return "", fmt.Errorf("parse Nydus source reference: %w", err)
	}
	// Conversion must consume this build's OCI image, even if another job
	// overwrites the intermediate tag before nydusify starts pulling it.
	sourceRef := ref.Context().Name() + "@" + ociDigest
	return runNydusify(ctx, sourceRef, spec.Target, opts, outputBuf)
}

func runNydusify(ctx context.Context, sourceRef, target string, opts Options, outputBuf *bytes.Buffer) (string, error) {
	metadata, err := os.CreateTemp("", "kova-nydusify-metadata-*.json")
	if err != nil {
		return "", fmt.Errorf("create Nydusify metadata file: %w", err)
	}
	metadataPath := metadata.Name()
	defer os.Remove(metadataPath)
	if err := metadata.Close(); err != nil {
		return "", fmt.Errorf("close Nydusify metadata file: %w", err)
	}
	args := append(nydusConvertArgs(sourceRef, target), "--output-json", metadataPath)
	if err := runCommand(ctx, opts.Verbose, outputBuf, "nydusify", args...); err != nil {
		return "", err
	}
	data, err := os.ReadFile(metadataPath)
	if err != nil {
		return "", fmt.Errorf("read Nydusify push metadata: %w", err)
	}
	var pushed struct {
		Digest string `json:"target_manifest_digest"`
	}
	if err := json.Unmarshal(data, &pushed); err != nil {
		return "", fmt.Errorf("parse Nydusify push metadata: %w", err)
	}
	return validatePushedDigest(pushed.Digest, "Nydusify")
}

func runBuildctl(ctx context.Context, spec source.Spec, addr *scheduler.Addr, opts Options, outputBuf *bytes.Buffer) (string, error) {
	metadata, err := os.CreateTemp("", "kova-buildctl-metadata-*.json")
	if err != nil {
		return "", fmt.Errorf("create BuildKit metadata file: %w", err)
	}
	metadataPath := metadata.Name()
	defer os.Remove(metadataPath)
	if err := metadata.Close(); err != nil {
		return "", fmt.Errorf("close BuildKit metadata file: %w", err)
	}
	args := append(buildCommandArgs(spec, addr), "--metadata-file", metadataPath)
	if err := runCommand(ctx, opts.Verbose, outputBuf, "buildctl", args...); err != nil {
		return "", err
	}
	data, err := os.ReadFile(metadataPath)
	if err != nil {
		return "", fmt.Errorf("read BuildKit metadata: %w", err)
	}
	var pushed struct {
		Digest string `json:"containerimage.digest"`
	}
	if err := json.Unmarshal(data, &pushed); err != nil {
		return "", fmt.Errorf("parse BuildKit metadata: %w", err)
	}
	return validatePushedDigest(pushed.Digest, "BuildKit")
}

func validatePushedDigest(value, tool string) (string, error) {
	hash, err := v1.NewHash(value)
	if err != nil || hash.Algorithm != "sha256" {
		return "", fmt.Errorf("%s did not report a valid pushed manifest digest", tool)
	}
	return hash.String(), nil
}

func runCommand(ctx context.Context, verbose bool, outputBuf *bytes.Buffer, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	if verbose {
		cmd.Stdout = io.MultiWriter(os.Stdout, outputBuf)
		cmd.Stderr = io.MultiWriter(os.Stderr, outputBuf)
	} else {
		cmd.Stdout = outputBuf
		cmd.Stderr = outputBuf
	}
	return cmd.Run()
}

func buildCommandArgs(spec source.Spec, addr *scheduler.Addr) []string {
	args := []string{
		"--addr", addr.Addr,
		"build",
		"--frontend=dockerfile.v0",
		"--opt", "platform=" + spec.Platform,
	}

	if spec.Dir != "" {
		args = append(args,
			"--local", "context="+spec.Dir,
			"--local", "dockerfile="+spec.Dir,
		)
	}

	output := fmt.Sprintf("type=image,name=%s,push=true,force-compression=true,oci-mediatypes=true,compression=gzip", spec.Target)
	args = append(args, "--output", output)

	return args
}

func nydusConvertArgs(sourceTarget, nydusTarget string) []string {
	args := []string{
		"convert",
		"--source", sourceTarget,
		"--target", nydusTarget,
		"--fs-version", "5",
		"--nydus-image", "/usr/bin/nydus-image",
	}
	sourcePlainHTTP := registryUsesPlainHTTP(sourceTarget)
	targetPlainHTTP := registryUsesPlainHTTP(nydusTarget)
	if sourcePlainHTTP {
		args = append(args, "--source-insecure")
	}
	if targetPlainHTTP {
		args = append(args, "--target-insecure")
	}
	if sourcePlainHTTP || targetPlainHTTP {
		args = append(args, "--plain-http")
	}
	return args
}

func registryUsesPlainHTTP(target string) bool {
	normalized := strings.TrimPrefix(strings.TrimPrefix(strings.TrimSpace(target), "docker://"), "oci://")
	firstSlash := strings.IndexByte(normalized, '/')
	if firstSlash <= 0 {
		return false
	}
	host := normalized[:firstSlash]
	if idx := strings.IndexByte(host, ':'); idx >= 0 {
		host = host[:idx]
	}
	switch host {
	case "localhost", "127.0.0.1", "host.docker.internal":
		return true
	default:
		return false
	}
}
