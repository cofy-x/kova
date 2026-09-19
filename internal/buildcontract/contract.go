package buildcontract

import (
	"fmt"
	"sort"
	"strings"

	apiv1 "github.com/cofy-x/kova/pkg/api/v1"

	"github.com/google/go-containerregistry/pkg/name"
)

const (
	MaxLogicalTargets                  = apiv1.MaxLogicalTargets
	MaxConcreteOutputs                 = apiv1.MaxConcreteOutputs
	MaxTargetLength                    = apiv1.MaxTargetLength
	MaxBuildConcurrency                = apiv1.MaxBuildConcurrency
	MaxManifestVerificationConcurrency = 8
	DefaultControllerConcurrency       = 4
	MaxControllerConcurrency           = 32
)

// NormalizeTarget accepts only explicit, tagged image destinations.
// Digest references are immutable identifiers, not push destinations.
func NormalizeTarget(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", fmt.Errorf("target must not be empty")
	}
	if len(value) > MaxTargetLength {
		return "", fmt.Errorf("target exceeds %d bytes", MaxTargetLength)
	}
	if value != raw || strings.ContainsAny(value, "\t\r\n ") {
		return "", fmt.Errorf("target must not contain surrounding or embedded whitespace")
	}
	if strings.Contains(value, "://") {
		return "", fmt.Errorf("target must be an image reference without a URI scheme")
	}
	if strings.Contains(value, "@") {
		return "", fmt.Errorf("target must be a tagged push destination, not a digest reference")
	}
	if strings.LastIndexByte(value, ':') <= strings.LastIndexByte(value, '/') {
		return "", fmt.Errorf("target must include an explicit tag")
	}
	tag, err := name.NewTag(value)
	if err != nil {
		return "", fmt.Errorf("invalid tagged image target %q: %w", value, err)
	}
	normalized := tag.Name()
	if _, err := name.NewTag(normalized, name.StrictValidation); err != nil {
		return "", fmt.Errorf("invalid tagged image target %q: %w", value, err)
	}
	return normalized, nil
}

func NormalizeTargets(values []string) ([]string, error) {
	if len(values) < 1 || len(values) > MaxLogicalTargets {
		return nil, fmt.Errorf("targets must contain between 1 and %d image references", MaxLogicalTargets)
	}
	normalized := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		target, err := NormalizeTarget(value)
		if err != nil {
			return nil, err
		}
		if _, exists := seen[target]; exists {
			return nil, fmt.Errorf("target %q is duplicated", target)
		}
		seen[target] = struct{}{}
		normalized = append(normalized, target)
	}
	sort.Strings(normalized)
	return normalized, nil
}

func ValidateConcurrency(concurrency, logicalTargets int) error {
	if concurrency < 1 || concurrency > MaxBuildConcurrency {
		return fmt.Errorf("concurrency must be between 1 and %d", MaxBuildConcurrency)
	}
	if concurrency > logicalTargets {
		return fmt.Errorf("concurrency %d exceeds logical target count %d", concurrency, logicalTargets)
	}
	return nil
}

func EqualTargetSets(left, right []string) bool {
	leftNormalized, leftErr := NormalizeTargets(left)
	rightNormalized, rightErr := NormalizeTargets(right)
	if leftErr != nil || rightErr != nil || len(leftNormalized) != len(rightNormalized) {
		return false
	}
	for index := range leftNormalized {
		if leftNormalized[index] != rightNormalized[index] {
			return false
		}
	}
	return true
}
