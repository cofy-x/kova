package buildresult

import (
	"context"
	"errors"
	"fmt"
	"sync"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/runnerexec"
	"github.com/cofy-x/kova/internal/source"
	"github.com/google/go-containerregistry/pkg/v1"
)

// CollectReceipts copies exact pushed digests from the runner's bounded export
// into durable status. It never resolves a mutable tag. A transport failure
// leaves its format pending; malformed or missing completed output is final.
func CollectReceipts(ctx context.Context, exporter Exporter, build *kovav1.KovaBuild, results []kovav1.BuildVerificationResult) (string, bool) {
	var transient string
	failed := false
	for _, format := range []string{string(source.BuildFormatOCI), string(source.BuildFormatNydus)} {
		needed := false
		for _, result := range results {
			if result.Format == format && result.State == "pending" && result.PushedDigest == "" {
				needed = true
				break
			}
		}
		if !needed {
			continue
		}
		query := "with-fail=true&summary=true"
		if format == string(source.BuildFormatOCI) {
			query += "&oci=true"
		}
		data, err := exporter.Post(ctx, build, "export", query)
		if err != nil {
			if errors.Is(err, runnerexec.ErrExportTooLarge) {
				for i := range results {
					if results[i].Format == format && results[i].State == "pending" && results[i].PushedDigest == "" {
						results[i].State, results[i].Error = "failed", "runner export exceeded its bounded response size"
						failed = true
					}
				}
				continue
			}
			transient = "runner export unavailable"
			continue
		}
		entries, err := parseEntries(data)
		if err != nil {
			for i := range results {
				if results[i].Format == format && results[i].State == "pending" && results[i].PushedDigest == "" {
					results[i].State, results[i].Error = "failed", "invalid runner export"
					failed = true
				}
			}
			continue
		}
		for i := range results {
			result := &results[i]
			if result.Format != format || result.State != "pending" || result.PushedDigest != "" {
				continue
			}
			entry, ok := entryForTarget(entries, result.Image)
			if !ok {
				result.State, result.Error = "failed", "completed build result is missing"
				failed = true
				continue
			}
			if !entry.Success {
				result.State, result.Error = "failed", "runner reported failed output"
				failed = true
				continue
			}
			hash, err := v1.NewHash(entry.ManifestDigest)
			if err != nil || hash.Algorithm != "sha256" {
				result.State, result.Error = "failed", "runner did not report a valid pushed manifest digest"
				failed = true
				continue
			}
			result.PushedDigest = entry.ManifestDigest
		}
	}
	return transient, failed
}

// VerifyReceipts reads only digest-pinned manifests. Results are independent,
// with at most four registry calls in flight for one controller attempt.
func VerifyReceipts(ctx context.Context, registry RegistryResolver, results []kovav1.BuildVerificationResult, plainHTTPRegistries []string, maxChecks int) (string, bool) {
	if maxChecks <= 0 {
		maxChecks = 16
	}
	indices := make([]int, 0, maxChecks)
	for i, result := range results {
		if result.State == "pending" && result.PushedDigest != "" {
			indices = append(indices, i)
			if len(indices) == maxChecks {
				break
			}
		}
	}
	if len(indices) == 0 {
		return "", false
	}
	workerCount := min(4, len(indices))
	jobs := make(chan int)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var transient string
	failed := false
	for range workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				result := &results[i]
				digest, platform, err := registry.Resolve(ctx, result.Image, result.PushedDigest, plainHTTPRegistries)
				if err != nil {
					if errors.Is(err, ErrDefinitive) {
						result.State, result.Error = "failed", "invalid pushed manifest descriptor"
						mu.Lock()
						failed = true
						mu.Unlock()
					} else {
						mu.Lock()
						transient = "registry manifest verification unavailable"
						mu.Unlock()
					}
					continue
				}
				if digest != result.PushedDigest || platform != result.Platform {
					result.State, result.Error = "failed", fmt.Sprintf("pushed digest/platform %s %s does not match receipt %s %s", digest, platform, result.PushedDigest, result.Platform)
					mu.Lock()
					failed = true
					mu.Unlock()
					continue
				}
				result.State, result.Error = "succeeded", ""
			}
		}()
	}
	for _, i := range indices {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	return transient, failed
}

func VerifyRemoteReceipts(ctx context.Context, results []kovav1.BuildVerificationResult, plainHTTPRegistries []string, maxChecks int) (string, bool) {
	return VerifyReceipts(ctx, remoteRegistryResolver{}, results, plainHTTPRegistries, maxChecks)
}

func VerificationOutputs(results []kovav1.BuildVerificationResult) []kovav1.BuildOutput {
	outputs := make([]kovav1.BuildOutput, 0, len(results))
	for _, result := range results {
		if result.State == "succeeded" {
			outputs = append(outputs, kovav1.BuildOutput{Format: result.Format, Image: result.Image, Platform: result.Platform, ManifestDigest: result.PushedDigest})
		}
	}
	return outputs
}

func VerificationDone(results []kovav1.BuildVerificationResult) (done, failed bool) {
	if len(results) == 0 {
		return false, true
	}
	done = true
	for _, result := range results {
		if result.State == "failed" {
			failed = true
		}
		if result.State == "pending" {
			done = false
		}
	}
	return done, failed
}
