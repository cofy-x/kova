package batch

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/cofy-x/kova/internal/buildobservation"
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

	var outputBuf boundedTailBuffer
	digest, observation, err := runObservedBuildCommands(buildCtx, spec, addr, opts, &outputBuf)
	buildErr = err
	finishedAt := time.Now()
	elapsed := finishedAt.Sub(startedAt)

	entry := store.Entry{
		StartedAt:        startedAt.Format(time.RFC3339),
		FinishedAt:       finishedAt.Format(time.RFC3339),
		Elapsed:          logging.FormatElapsed(elapsed),
		Target:           spec.Target,
		NodeIP:           nodeIP,
		ManifestDigest:   digest,
		Success:          err == nil,
		BuildObservation: observation,
	}

	if err != nil {
		output := outputBuf.String()
		entry.Logs = output
		entry.Reason = err.Error()
		op.SetResult(observability.ResultError)
		op.SetErrorClass("build_command_failed")

		if outputBuf.ContainsConnectionRefused() {
			logging.Infof("Detected OOM-style failure for %s, cooling down %s for %s",
				spec.Target, addr.Addr, addr.Cooldown)
			addr.SetCooldown()
		}
	}

	return entry
}

func runBuildCommands(ctx context.Context, spec source.Spec, addr *scheduler.Addr, opts Options, outputBuf io.Writer) (string, error) {
	digest, _, err := runObservedBuildCommands(ctx, spec, addr, opts, outputBuf)
	return digest, err
}

func runObservedBuildCommands(ctx context.Context, spec source.Spec, addr *scheduler.Addr, opts Options, outputBuf io.Writer) (string, *buildobservation.Observation, error) {
	if source.FormatIsOCI(spec.Format) {
		return runObservedBuildctl(ctx, spec, addr, opts, outputBuf)
	}

	ociSpec := spec
	ociSpec.Target = source.StripNydusV3Suffix(spec.Target)
	ociDigest, observation, err := runObservedBuildctl(ctx, ociSpec, addr, opts, outputBuf)
	if err != nil {
		return "", observation, err
	}
	ref, err := name.ParseReference(ociSpec.Target, name.WeakValidation)
	if err != nil {
		return "", observation, fmt.Errorf("parse Nydus source reference: %w", err)
	}
	// Conversion must consume this build's OCI image, even if another job
	// overwrites the intermediate tag before nydusify starts pulling it.
	sourceRef := ref.Context().Name() + "@" + ociDigest
	started := time.Now()
	digest, err := runNydusify(ctx, sourceRef, spec.Target, opts, outputBuf)
	observation.NydusWallNanoseconds = nanosecondsPointer(int64(time.Since(started)))
	observation.NydusAvailability = "observed"
	if err != nil {
		observation.NydusAvailability = "incomplete"
		if observation.Availability == "observed" {
			observation.Availability, observation.Reason = "incomplete", "command_failed"
			if ctx.Err() != nil {
				observation.Reason = "cancelled"
			}
		}
	}
	return digest, buildobservation.Normalize(observation), err
}

func runNydusify(ctx context.Context, sourceRef, target string, opts Options, outputBuf io.Writer) (string, error) {
	metadata, err := os.CreateTemp("", "kova-nydusify-metadata-*.json")
	if err != nil {
		return "", fmt.Errorf("create Nydusify metadata file: %w", err)
	}
	metadataPath := metadata.Name()
	defer os.Remove(metadataPath)
	if err := metadata.Close(); err != nil {
		return "", fmt.Errorf("close Nydusify metadata file: %w", err)
	}
	args := append(nydusConvertArgs(sourceRef, target, opts.RegistryPlainHTTP), "--output-json", metadataPath)
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

func runObservedBuildctl(ctx context.Context, spec source.Spec, addr *scheduler.Addr, opts Options, outputBuf io.Writer) (digest string, observation *buildobservation.Observation, resultErr error) {
	// stdout and the projected stderr now use different exec copy goroutines.
	// Serialize their shared diagnostic sink even for a non-concurrent writer.
	sharedOutput := &lockedCommandWriter{writer: outputBuf}
	projection := newProgressProjection(commandOutput(opts.Verbose, os.Stderr, sharedOutput))
	defer func() { observation = projection.finish(resultErr, ctx.Err() != nil) }()
	metadata, err := os.CreateTemp("", "kova-buildctl-metadata-*.json")
	if err != nil {
		return "", nil, fmt.Errorf("create BuildKit metadata file: %w", err)
	}
	metadataPath := metadata.Name()
	defer os.Remove(metadataPath)
	if err := metadata.Close(); err != nil {
		return "", nil, fmt.Errorf("close BuildKit metadata file: %w", err)
	}
	args := append(buildCommandArgs(spec, addr), "--metadata-file", metadataPath, "--progress=rawjson")
	cmd := exec.CommandContext(ctx, "buildctl", args...)
	cmd.Stdout = commandOutput(opts.Verbose, os.Stdout, sharedOutput)
	cmd.Stderr = projection
	if err := cmd.Run(); err != nil {
		return "", nil, err
	}
	data, err := os.ReadFile(metadataPath)
	if err != nil {
		return "", nil, fmt.Errorf("read BuildKit metadata: %w", err)
	}
	var pushed struct {
		Digest string `json:"containerimage.digest"`
	}
	if err := json.Unmarshal(data, &pushed); err != nil {
		return "", nil, fmt.Errorf("parse BuildKit metadata: %w", err)
	}
	digest, err = validatePushedDigest(pushed.Digest, "BuildKit")
	return digest, nil, err
}

func commandOutput(verbose bool, terminal, output io.Writer) io.Writer {
	if verbose {
		return io.MultiWriter(terminal, output)
	}
	return output
}

type lockedCommandWriter struct {
	mu     sync.Mutex
	writer io.Writer
}

func (w *lockedCommandWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writer.Write(data)
}

func validatePushedDigest(value, tool string) (string, error) {
	hash, err := v1.NewHash(value)
	if err != nil || hash.Algorithm != "sha256" {
		return "", fmt.Errorf("%s did not report a valid pushed manifest digest", tool)
	}
	return hash.String(), nil
}

func runCommand(ctx context.Context, verbose bool, outputBuf io.Writer, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	sharedOutput := &lockedCommandWriter{writer: outputBuf}
	if verbose {
		cmd.Stdout = io.MultiWriter(os.Stdout, sharedOutput)
		cmd.Stderr = io.MultiWriter(os.Stderr, sharedOutput)
	} else {
		cmd.Stdout = sharedOutput
		cmd.Stderr = sharedOutput
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

func nydusConvertArgs(sourceTarget, nydusTarget string, plainHTTPRegistries []string) []string {
	args := []string{
		"convert",
		"--source", sourceTarget,
		"--target", nydusTarget,
		"--fs-version", "5",
		"--nydus-image", "/usr/bin/nydus-image",
	}
	sourcePlainHTTP := registryUsesPlainHTTP(sourceTarget, plainHTTPRegistries)
	targetPlainHTTP := registryUsesPlainHTTP(nydusTarget, plainHTTPRegistries)
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

func registryUsesPlainHTTP(target string, plainHTTPRegistries []string) bool {
	normalized := strings.TrimPrefix(strings.TrimPrefix(strings.TrimSpace(target), "docker://"), "oci://")
	firstSlash := strings.IndexByte(normalized, '/')
	if firstSlash <= 0 {
		return false
	}
	registry := normalized[:firstSlash]
	for _, configured := range plainHTTPRegistries {
		if strings.EqualFold(strings.TrimSpace(configured), registry) {
			return true
		}
	}
	host := registry
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
