package buildresult

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/buildcontract"
	"github.com/cofy-x/kova/internal/source"
	"github.com/cofy-x/kova/internal/store"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

type Exporter interface {
	Post(context.Context, *kovav1.KovaBuild, string, string) ([]byte, error)
}

type RegistryResolver interface {
	Resolve(context.Context, string, []string) (string, error)
}

type remoteRegistryResolver struct{}

func (remoteRegistryResolver) Resolve(ctx context.Context, target string, plainHTTPRegistries []string) (string, error) {
	ref, err := name.ParseReference(target, referenceOptions(target, plainHTTPRegistries)...)
	if err != nil {
		return "", err
	}
	descriptor, err := remote.Get(ref, remote.WithContext(ctx), remote.WithAuthFromKeychain(authn.DefaultKeychain))
	if err != nil {
		return "", fmt.Errorf("resolve pushed descriptor: %w", err)
	}
	return descriptor.Descriptor.Digest.String(), nil
}

type Result struct {
	Format         string
	Status         string
	Repository     string
	ManifestDigest string
	Error          string
}

func Resolve(ctx context.Context, exporter Exporter, build *kovav1.KovaBuild, plainHTTPRegistries []string) []Result {
	return resolveWithRegistry(ctx, exporter, remoteRegistryResolver{}, build, plainHTTPRegistries)
}

func resolveWithRegistry(ctx context.Context, exporter Exporter, registry RegistryResolver, build *kovav1.KovaBuild, plainHTTPRegistries []string) []Result {
	expected := Pending(build)
	if build.Status.Phase == kovav1.PhaseCancelled {
		return failAll(expected, "cancelled", "build cancelled")
	}
	formats := make(map[string][]store.Entry, 2)
	formatErrors := make(map[string]error, 2)
	for _, result := range expected {
		if _, exists := formats[result.Format]; exists || formatErrors[result.Format] != nil {
			continue
		}
		query := "with-fail=true"
		if result.Format == string(source.BuildFormatOCI) {
			query += "&oci=true"
		}
		data, err := exporter.Post(ctx, build, "export", query)
		if err != nil {
			formatErrors[result.Format] = err
			continue
		}
		entries, err := parseEntries(data)
		if err != nil {
			formatErrors[result.Format] = err
			continue
		}
		formats[result.Format] = entries
	}

	pending := make([]int, 0, len(expected))
	for index := range expected {
		if err := formatErrors[expected[index].Format]; err != nil {
			expected[index].Status, expected[index].Error = "failed", err.Error()
			continue
		}
		entry, ok := entryForTarget(formats[expected[index].Format], expected[index].Repository)
		if !ok {
			expected[index].Status, expected[index].Error = "failed", "build result is missing"
			continue
		}
		if !entry.Success {
			expected[index].Status, expected[index].Error = "failed", entry.Reason
			continue
		}
		pending = append(pending, index)
	}

	limit := int(build.Status.AllocatedConcurrency)
	if limit < 1 {
		limit = build.Spec.Build.Concurrency
	}
	if limit < 1 {
		limit = 1
	}
	if limit > buildcontract.MaxManifestVerificationConcurrency {
		limit = buildcontract.MaxManifestVerificationConcurrency
	}
	if limit > len(pending) {
		limit = len(pending)
	}
	jobs := make(chan int)
	var wg sync.WaitGroup
	for worker := 0; worker < limit; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				digest, err := registry.Resolve(ctx, expected[index].Repository, plainHTTPRegistries)
				if err != nil {
					expected[index].Status, expected[index].Error = "failed", err.Error()
					continue
				}
				expected[index].Status = "succeeded"
				expected[index].ManifestDigest = digest
			}
		}()
	}
	for _, index := range pending {
		jobs <- index
	}
	close(jobs)
	wg.Wait()
	return expected
}

func referenceOptions(target string, plainHTTPRegistries []string) []name.Option {
	options := []name.Option{name.WeakValidation}
	normalized := strings.TrimPrefix(strings.TrimPrefix(strings.TrimSpace(target), "docker://"), "oci://")
	registry, _, found := strings.Cut(normalized, "/")
	if !found {
		return options
	}
	for _, configured := range plainHTTPRegistries {
		if strings.EqualFold(strings.TrimSpace(configured), registry) {
			return append(options, name.Insecure)
		}
	}
	host, _, _ := strings.Cut(registry, ":")
	switch host {
	case "localhost", "127.0.0.1", "host.docker.internal":
		return append(options, name.Insecure)
	default:
		return options
	}
}

func entryForTarget(entries []store.Entry, target string) (store.Entry, bool) {
	for _, entry := range entries {
		if entry.Target == target {
			return entry, true
		}
	}
	return store.Entry{}, false
}

func Pending(build *kovav1.KovaBuild) []Result {
	formats, err := source.ParseBuildFormats(build.Spec.Build.Format)
	if err != nil {
		return nil
	}
	targets, err := buildcontract.NormalizeTargets(build.Spec.Targets)
	if err != nil {
		return nil
	}
	results := make([]Result, 0, len(formats)*len(targets))
	for _, target := range targets {
		for _, format := range formats {
			results = append(results, Result{Format: string(format), Status: "pending", Repository: source.NormalizeTargetForFormat(target, format)})
		}
	}
	return results
}

func AllSucceeded(results []Result) bool {
	if len(results) == 0 {
		return false
	}
	for _, result := range results {
		if result.Status != "succeeded" || result.ManifestDigest == "" {
			return false
		}
	}
	return true
}

func parseEntries(data []byte) ([]store.Entry, error) {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	var entries []store.Entry
	for scanner.Scan() {
		if strings.TrimSpace(scanner.Text()) == "" {
			continue
		}
		var entry store.Entry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return nil, fmt.Errorf("parse exported result: %w", err)
		}
		entries = append(entries, entry)
	}
	return entries, scanner.Err()
}

func failAll(results []Result, status, message string) []Result {
	for index := range results {
		results[index].Status, results[index].Error = status, message
	}
	return results
}

func Outputs(results []Result) []kovav1.BuildOutput {
	outputs := make([]kovav1.BuildOutput, 0, len(results))
	for _, result := range results {
		if result.Status != "succeeded" || result.ManifestDigest == "" {
			continue
		}
		outputs = append(outputs, kovav1.BuildOutput{
			Format: result.Format, Image: result.Repository, ManifestDigest: result.ManifestDigest,
		})
	}
	return outputs
}
