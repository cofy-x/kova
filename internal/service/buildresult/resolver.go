package buildresult

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/source"
	"github.com/cofy-x/kova/internal/store"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

type Exporter interface {
	Post(context.Context, *kovav1.KovaBuild, string, string) ([]byte, error)
}

func Resolve(ctx context.Context, exporter Exporter, build *kovav1.KovaBuild, plainHTTPRegistries []string) []kovav1.BuildResult {
	expected := Pending(build)
	if build.Status.Phase == kovav1.PhaseCancelled {
		return failAll(expected, "cancelled", "build cancelled")
	}
	for index := range expected {
		query := "with-fail=true"
		if expected[index].Format == string(source.BuildFormatOCI) {
			query += "&oci=true"
		}
		data, err := exporter.Post(ctx, build, "export", query)
		if err != nil {
			expected[index].Status, expected[index].Error = "failed", err.Error()
			continue
		}
		entries, err := parseEntries(data)
		if err != nil {
			expected[index].Status, expected[index].Error = "failed", err.Error()
			continue
		}
		entry, ok := entryForTarget(entries, expected[index].Repository)
		if !ok {
			expected[index].Status, expected[index].Error = "failed", "build result is missing"
			continue
		}
		if !entry.Success {
			expected[index].Status, expected[index].Error = "failed", entry.Reason
			continue
		}
		ref, parseErr := name.ParseReference(entry.Target, referenceOptions(entry.Target, plainHTTPRegistries)...)
		if parseErr != nil {
			expected[index].Status, expected[index].Error = "failed", parseErr.Error()
			continue
		}
		descriptor, getErr := remote.Get(ref, remote.WithContext(ctx), remote.WithAuthFromKeychain(authn.DefaultKeychain))
		if getErr != nil {
			expected[index].Status, expected[index].Error = "failed", fmt.Sprintf("resolve pushed descriptor: %v", getErr)
			continue
		}
		expected[index].Status = "succeeded"
		expected[index].ManifestDigest = descriptor.Descriptor.Digest.String()
		expected[index].MediaType = string(descriptor.Descriptor.MediaType)
		expected[index].Size = descriptor.Descriptor.Size
	}
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

func Pending(build *kovav1.KovaBuild) []kovav1.BuildResult {
	formats, err := source.ParseBuildFormats(build.Spec.Build.Format)
	if err != nil {
		return nil
	}
	results := make([]kovav1.BuildResult, 0, len(formats))
	for _, format := range formats {
		results = append(results, kovav1.BuildResult{Format: string(format), Status: "pending", Repository: source.NormalizeTargetForFormat(build.Spec.Build.Target, format)})
	}
	return results
}

func AllSucceeded(results []kovav1.BuildResult) bool {
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

func failAll(results []kovav1.BuildResult, status, message string) []kovav1.BuildResult {
	for index := range results {
		results[index].Status, results[index].Error = status, message
	}
	return results
}
