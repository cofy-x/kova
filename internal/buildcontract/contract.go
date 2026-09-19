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

const (
	PlatformLinuxAMD64 = string(apiv1.PlatformLinuxAMD64)
	PlatformLinuxARM64 = string(apiv1.PlatformLinuxARM64)
	// NydusV3TargetSuffix is reserved for the concrete Nydus output derived
	// from a logical target. Logical targets using it would make the mapping
	// from logical targets to concrete outputs ambiguous.
	NydusV3TargetSuffix = "_nydus_v3"
)

type TargetSpec struct {
	Target   string `json:"target"`
	Platform string `json:"platform"`
}

func NormalizePlatform(raw string) (string, error) {
	value := strings.TrimSpace(strings.ToLower(raw))
	if value != raw {
		return "", fmt.Errorf("platform must be canonical and contain no surrounding whitespace")
	}
	switch value {
	case PlatformLinuxAMD64, PlatformLinuxARM64:
		return value, nil
	default:
		return "", fmt.Errorf("unsupported platform %q; supported platforms are %s and %s", raw, PlatformLinuxAMD64, PlatformLinuxARM64)
	}
}

func ParsePlatformAddresses(values []string) (map[string]string, error) {
	result := make(map[string]string, len(values))
	for _, raw := range values {
		platform, address, ok := strings.Cut(strings.TrimSpace(raw), "=")
		if !ok || strings.TrimSpace(address) == "" {
			return nil, fmt.Errorf("platform address %q must use platform=buildkit-address", raw)
		}
		normalized, err := NormalizePlatform(platform)
		if err != nil {
			return nil, err
		}
		if _, exists := result[normalized]; exists {
			return nil, fmt.Errorf("platform %q has more than one address pool", normalized)
		}
		result[normalized] = strings.TrimSpace(address)
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("at least one platform address is required")
	}
	return result, nil
}

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

// NormalizeLogicalTarget validates a caller-owned logical destination. Kova
// derives concrete format destinations from this value, so callers cannot use
// the suffix reserved for a derived Nydus output.
func NormalizeLogicalTarget(raw string) (string, error) {
	target, err := NormalizeTarget(raw)
	if err != nil {
		return "", err
	}
	if strings.HasSuffix(target, NydusV3TargetSuffix) {
		return "", fmt.Errorf("target tag suffix %q is reserved for Kova Nydus outputs", NydusV3TargetSuffix)
	}
	return target, nil
}

func NormalizeTargets(values []string) ([]string, error) {
	if len(values) < 1 || len(values) > MaxLogicalTargets {
		return nil, fmt.Errorf("targets must contain between 1 and %d image references", MaxLogicalTargets)
	}
	normalized := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		target, err := NormalizeLogicalTarget(value)
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

func NormalizeTargetSpecs(values []TargetSpec) ([]TargetSpec, error) {
	if len(values) < 1 || len(values) > MaxLogicalTargets {
		return nil, fmt.Errorf("targets must contain between 1 and %d entries", MaxLogicalTargets)
	}
	normalized := make([]TargetSpec, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		target, err := NormalizeLogicalTarget(value.Target)
		if err != nil {
			return nil, err
		}
		platform, err := NormalizePlatform(value.Platform)
		if err != nil {
			return nil, err
		}
		if _, exists := seen[target]; exists {
			return nil, fmt.Errorf("target %q is duplicated", target)
		}
		seen[target] = struct{}{}
		normalized = append(normalized, TargetSpec{Target: target, Platform: platform})
	}
	sort.Slice(normalized, func(i, j int) bool {
		if normalized[i].Target != normalized[j].Target {
			return normalized[i].Target < normalized[j].Target
		}
		return normalized[i].Platform < normalized[j].Platform
	})
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

func EqualTargetSpecSets(left, right []TargetSpec) bool {
	leftNormalized, leftErr := NormalizeTargetSpecs(left)
	rightNormalized, rightErr := NormalizeTargetSpecs(right)
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
