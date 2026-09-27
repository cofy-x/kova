package source

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/cofy-x/kova/internal/buildcontract"
)

const NydusV3TargetSuffix = buildcontract.NydusV3TargetSuffix

type ImageMetadata struct {
	Target   string `json:"target"`
	Platform string `json:"platform"`
}

type BuildFormat string

const (
	BuildFormatNydus BuildFormat = "nydus"
	BuildFormatOCI   BuildFormat = "oci"
)

type Spec struct {
	Dir      string
	Target   string
	Platform string
	Format   BuildFormat
}

func LoadBuildSpecs(imageDirs, target, platform string, oci bool, buildVars map[string]string) ([]Spec, func(), error) {
	format := BuildFormatNydus
	if oci {
		format = BuildFormatOCI
	}
	return LoadBuildSpecsForFormats(imageDirs, target, platform, []BuildFormat{format}, buildVars)
}

func LoadBuildSpecsForFormats(imageDirs, target, platform string, buildFormats []BuildFormat, buildVars map[string]string) ([]Spec, func(), error) {
	return loadBuildSpecsForFormats(imageDirs, target, platform, buildFormats, buildVars, false)
}

// LoadBuildSpecsForFormatsInPlace is for a private, job-local extracted source
// tree. It applies build variables in that tree without copying its payload a
// second time. Callers own the tree and must remove it after the build.
func LoadBuildSpecsForFormatsInPlace(imageDirs, target, platform string, buildFormats []BuildFormat, buildVars map[string]string) ([]Spec, func(), error) {
	return loadBuildSpecsForFormats(imageDirs, target, platform, buildFormats, buildVars, true)
}

func loadBuildSpecsForFormats(imageDirs, target, platform string, buildFormats []BuildFormat, buildVars map[string]string, inPlace bool) ([]Spec, func(), error) {
	if imageDirs == "" && target == "" {
		return nil, nil, fmt.Errorf("either --image-dirs or a positional [target] is required")
	}
	if len(buildFormats) == 0 {
		return nil, nil, fmt.Errorf("at least one build format is required")
	}

	if imageDirs == "" {
		normalizedPlatform, err := buildcontract.NormalizePlatform(platform)
		if err != nil {
			return nil, nil, err
		}
		specs := make([]Spec, 0, len(buildFormats))
		for _, format := range buildFormats {
			specs = append(specs, Spec{
				Target:   NormalizeTargetForFormat(target, format),
				Platform: normalizedPlatform,
				Format:   format,
			})
		}
		sort.Slice(specs, func(i, j int) bool {
			return specs[i].Target < specs[j].Target
		})
		return specs, nil, nil
	}

	targetFilters := make(map[string]struct{}, len(buildFormats))
	if strings.TrimSpace(target) != "" {
		normalizedPlatform, platformErr := buildcontract.NormalizePlatform(platform)
		if platformErr != nil {
			return nil, nil, platformErr
		}
		platform = normalizedPlatform
		for _, format := range buildFormats {
			targetFilters[NormalizeTargetForFormat(target, format)] = struct{}{}
		}
	}
	var preparedRoot string
	var cleanup func()
	if inPlace {
		preparedRoot = imageDirs
		if err := prepareBuildImageDirsInPlace(imageDirs, buildVars); err != nil {
			return nil, nil, err
		}
	} else {
		var err error
		preparedRoot, err = prepareBuildImageDirs(imageDirs, buildVars)
		if err != nil {
			return nil, nil, err
		}
		cleanup = func() {
			_ = os.RemoveAll(preparedRoot)
		}
	}
	fail := func(err error) ([]Spec, func(), error) {
		if cleanup != nil {
			cleanup()
		}
		return nil, nil, err
	}

	entries, err := os.ReadDir(preparedRoot)
	if err != nil {
		return fail(fmt.Errorf("read image-dirs: %w", err))
	}
	if len(entries) == 0 {
		return fail(fmt.Errorf("image-dirs %s does not contain any image directories", imageDirs))
	}

	var specs []Spec
	for _, e := range entries {
		if !e.IsDir() {
			return fail(fmt.Errorf("image-dirs %s must contain only directories, found %s", imageDirs, e.Name()))
		}
		dir := filepath.Join(preparedRoot, e.Name())
		metaPath := filepath.Join(dir, "metadata.json")

		meta, err := loadImageMetadata(metaPath)
		if err != nil {
			return fail(err)
		}
		if meta.Target == "" {
			return fail(fmt.Errorf("%s has empty target in metadata.json", dir))
		}

		for _, format := range buildFormats {
			normalizedTarget := NormalizeTargetForFormat(meta.Target, format)
			if len(targetFilters) > 0 {
				if _, ok := targetFilters[normalizedTarget]; !ok {
					continue
				}
				if meta.Platform != platform {
					return fail(fmt.Errorf("target %q declares platform %s, not requested platform %s", meta.Target, meta.Platform, platform))
				}
			}

			specs = append(specs, Spec{
				Dir:      dir,
				Target:   normalizedTarget,
				Platform: meta.Platform,
				Format:   format,
			})
		}
	}

	sort.Slice(specs, func(i, j int) bool {
		return specs[i].Target < specs[j].Target
	})
	return specs, cleanup, nil
}

func loadImageMetadata(metaPath string) (ImageMetadata, error) {
	raw, err := readBoundedBuildFile(metaPath)
	if err != nil {
		return ImageMetadata{}, fmt.Errorf("read %s: %w", metaPath, err)
	}
	var meta ImageMetadata
	if err := json.Unmarshal(raw, &meta); err != nil {
		return ImageMetadata{}, fmt.Errorf("invalid %s: %w", metaPath, err)
	}
	meta.Target, err = buildcontract.NormalizeLogicalTarget(meta.Target)
	if err != nil {
		return ImageMetadata{}, fmt.Errorf("invalid target in %s: %w", metaPath, err)
	}
	meta.Platform, err = buildcontract.NormalizePlatform(meta.Platform)
	if err != nil {
		return ImageMetadata{}, fmt.Errorf("invalid platform in %s: %w", metaPath, err)
	}
	return meta, nil
}

func HasNydusV3Suffix(target string) bool {
	return strings.HasSuffix(strings.TrimSpace(target), NydusV3TargetSuffix)
}

func EnsureNydusV3Suffix(target string) string {
	trimmed := strings.TrimSpace(target)
	if trimmed == "" || HasNydusV3Suffix(trimmed) {
		return trimmed
	}
	return trimmed + NydusV3TargetSuffix
}

func StripNydusV3Suffix(target string) string {
	return strings.TrimSuffix(strings.TrimSpace(target), NydusV3TargetSuffix)
}

func NormalizeTargetForMode(target string, oci bool) string {
	if oci {
		return StripNydusV3Suffix(target)
	}
	return EnsureNydusV3Suffix(target)
}

func NormalizeTargetForFormat(target string, format BuildFormat) string {
	switch format {
	case BuildFormatOCI:
		return StripNydusV3Suffix(target)
	default:
		return EnsureNydusV3Suffix(target)
	}
}

func ParseBuildFormats(value string) ([]BuildFormat, error) {
	switch NormalizeBuildFormatValue(value) {
	case "", string(BuildFormatNydus):
		return []BuildFormat{BuildFormatNydus}, nil
	case string(BuildFormatOCI):
		return []BuildFormat{BuildFormatOCI}, nil
	case "both":
		return []BuildFormat{BuildFormatNydus, BuildFormatOCI}, nil
	default:
		return nil, fmt.Errorf("build format must be one of nydus, oci, or both")
	}
}

func NormalizeBuildFormatValue(value string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))
	if normalized == "" {
		return string(BuildFormatNydus)
	}
	return normalized
}

func FormatIsOCI(format BuildFormat) bool {
	return format == BuildFormatOCI
}

func TargetMatchesMode(target string, oci bool) bool {
	if oci {
		return !HasNydusV3Suffix(target)
	}
	return HasNydusV3Suffix(target)
}
