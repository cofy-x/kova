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
	"sync"
	"sync/atomic"
	"time"

	"github.com/cofy-x/kova/internal/logging"
	"github.com/cofy-x/kova/internal/observability"
	"github.com/cofy-x/kova/internal/source"
	"github.com/cofy-x/kova/internal/store"

	"go.opentelemetry.io/otel/attribute"
)

func RunPreheat(opts Options) (err error) {
	opCtx := opts.Ctx
	if opCtx == nil {
		opCtx = context.Background()
	}
	ctx, op := observability.StartOperation(opCtx, observability.OperationConfig{
		Name: "kova.batch.preheat",
		SpanAttrs: []attribute.KeyValue{
			attribute.Bool("kova.oci", opts.OCI),
			attribute.Int("kova.concurrency", opts.Concurrency),
		},
		MetricAttrs: []attribute.KeyValue{
			attribute.Bool("kova.oci", opts.OCI),
		},
		Counter:  observability.Instrument{Name: "kova_batch_preheats_total", Description: "Batch preheat operations"},
		Duration: observability.Instrument{Name: "kova_batch_preheat_duration_seconds", Description: "Batch preheat duration"},
	})
	defer func() { op.End(err) }()

	logging.ResetCommandStartTime(time.Now())

	if opts.DragonflySchedulerAddr == "" {
		return fmt.Errorf("--dragonfly-scheduler-addr is required")
	}

	db, err := store.Open(opts.FromResultPath)
	if err != nil {
		return fmt.Errorf("open result database: %w", err)
	}
	defer db.Close()

	entries, err := db.All()
	if err != nil {
		return err
	}

	targets := filterPreheatTargets(entries, opts.Target, opts.OCI)

	if len(targets) == 0 {
		if opts.Target != "" {
			mode := "Nydus"
			if opts.OCI {
				mode = "OCI"
			}
			return fmt.Errorf("preheat target %q was not found as a successful %s build result", opts.Target, mode)
		}
		logging.Infof("No targets to preheat")
		op.SetResult(observability.ResultSkipped)
		return nil
	}

	logging.ResetProgress(len(targets))
	defer logging.ClearProgress()

	logging.Infof("Preheating %d target(s) with concurrency=%d", len(targets), opts.Concurrency)
	registryAuth, err := loadRegistryAuth(opts.DockerConfigPath)
	if err != nil {
		return err
	}

	sem := make(chan struct{}, opts.Concurrency)
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		anyError atomic.Bool
		lastTime time.Time
	)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for _, entry := range targets {
		if anyError.Load() && opts.Failfast {
			break
		}
		if ctx.Err() != nil {
			break
		}

		// Throttle based on --interval.
		if opts.Interval > 0 {
			mu.Lock()
			since := time.Since(lastTime)
			wait := time.Duration(opts.Interval)*time.Second - since
			if wait > 0 {
				mu.Unlock()
				select {
				case <-time.After(wait):
				case <-ctx.Done():
				}
			} else {
				mu.Unlock()
			}
			mu.Lock()
			lastTime = time.Now()
			mu.Unlock()
		}

		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}

		wg.Add(1)
		go func(target string) {
			defer wg.Done()
			defer func() { <-sem }()

			err := executePreheat(ctx, target, opts, registryAuth)
			logging.AdvanceProgress()
			if err != nil {
				logging.Errorf("Preheat failed for %s: %v", target, err)
				anyError.Store(true)
				if opts.Failfast {
					cancel()
				}
			} else {
				logging.Infof("Preheated: %s", target)
			}
		}(entry.Target)
	}

	wg.Wait()
	if anyError.Load() {
		return fmt.Errorf("one or more preheat tasks failed")
	}
	return nil
}

func filterPreheatTargets(entries []store.Entry, requested string, oci bool) []store.Entry {
	targets := make([]store.Entry, 0, len(entries))
	for _, entry := range entries {
		if !entry.Success || !source.TargetMatchesMode(entry.Target, oci) {
			continue
		}
		if requested != "" && entry.Target != requested {
			continue
		}
		targets = append(targets, entry)
	}
	return targets
}

func executePreheat(ctx context.Context, target string, opts Options, registryAuth registryCredentials) (err error) {
	ctx, op := observability.StartOperation(ctx, observability.OperationConfig{
		Name: "kova.batch.preheat.target",
		SpanAttrs: []attribute.KeyValue{
			observability.StringAttr(observability.AttrTarget, target),
		},
		MetricAttrs: []attribute.KeyValue{},
		Counter:     observability.Instrument{Name: "kova_preheat_targets_total", Description: "Preheat target attempts"},
		Duration:    observability.Instrument{Name: "kova_preheat_target_duration_seconds", Description: "Preheat target duration"},
	})
	defer func() { op.End(err) }()

	var pCtx context.Context
	var pCancel context.CancelFunc
	if opts.Timeout > 0 {
		pCtx, pCancel = context.WithTimeout(ctx, time.Duration(opts.Timeout)*time.Second)
	} else {
		pCtx, pCancel = context.WithCancel(ctx)
	}
	defer pCancel()

	preheatURL, err := buildPreheatURL(target)
	if err != nil {
		return err
	}

	username, password := registryAuth.forURL(preheatURL)
	reqJSON, err := json.Marshal(preheatRequestBody(preheatURL, username, password, opts.PreheatInsecureSkipVerify))
	if err != nil {
		return err
	}

	args := []string{
		"-plaintext",
		"-d", "@",
		opts.DragonflySchedulerAddr,
		"scheduler.v2.Scheduler.PreheatImage",
	}

	cmd := exec.CommandContext(pCtx, "grpcurl", args...)
	cmd.Stdin = bytes.NewReader(reqJSON)
	var outputBuf bytes.Buffer
	if opts.Verbose {
		cmd.Stdout = io.MultiWriter(os.Stdout, &outputBuf)
		cmd.Stderr = io.MultiWriter(os.Stderr, &outputBuf)
	} else {
		cmd.Stdout = &outputBuf
		cmd.Stderr = &outputBuf
	}

	err = cmd.Run()
	if err != nil {
		op.SetResult(observability.ResultError)
		op.SetErrorClass("grpcurl_failed")
	}
	return err
}

func preheatRequestBody(preheatURL string, username string, password string, insecureSkipVerify bool) map[string]any {
	return map[string]any{
		"url":                preheatURL,
		"username":           username,
		"password":           password,
		"scope":              "all_seed_peers",
		"insecureSkipVerify": insecureSkipVerify,
	}
}

func buildPreheatURL(target string) (string, error) {
	trimmed := strings.TrimSpace(target)
	if trimmed == "" {
		return "", fmt.Errorf("preheat target is empty")
	}
	if strings.HasPrefix(trimmed, "http://") || strings.HasPrefix(trimmed, "https://") {
		return trimmed, nil
	}

	normalized := strings.TrimPrefix(strings.TrimPrefix(trimmed, "docker://"), "oci://")
	firstSlash := strings.IndexByte(normalized, '/')
	if firstSlash <= 0 || firstSlash == len(normalized)-1 {
		return "", fmt.Errorf("preheat target %q is not a fully qualified image reference", target)
	}

	registry := normalized[:firstSlash]
	remainder := normalized[firstSlash+1:]
	if !strings.Contains(registry, ".") && !strings.Contains(registry, ":") && registry != "localhost" {
		return "", fmt.Errorf("preheat target %q is not a fully qualified image reference", target)
	}

	repo := remainder
	reference := "latest"
	if at := strings.LastIndexByte(remainder, '@'); at >= 0 {
		repo = remainder[:at]
		reference = remainder[at+1:]
	} else {
		lastSlash := strings.LastIndexByte(remainder, '/')
		lastColon := strings.LastIndexByte(remainder, ':')
		if lastColon > lastSlash {
			repo = remainder[:lastColon]
			reference = remainder[lastColon+1:]
		}
	}

	if repo == "" || reference == "" {
		return "", fmt.Errorf("preheat target %q is not a valid image reference", target)
	}

	return fmt.Sprintf("%s://%s/v2/%s/manifests/%s", preheatRegistryScheme(registry), registry, repo, reference), nil
}

func preheatRegistryScheme(registry string) string {
	host := registry
	if idx := strings.IndexByte(host, ':'); idx >= 0 {
		host = host[:idx]
	}
	switch host {
	case "localhost", "127.0.0.1", "host.docker.internal":
		return "http"
	default:
		return "https"
	}
}
