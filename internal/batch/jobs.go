package batch

import (
	"sort"

	"github.com/cofy-x/kova/internal/source"
)

type buildJob struct {
	key      string
	platform string
	specs    []source.Spec
}

func groupBuildSpecs(specs []source.Spec) []buildJob {
	if len(specs) == 0 {
		return nil
	}

	grouped := make(map[string][]source.Spec, len(specs))
	for _, spec := range specs {
		key := source.StripNydusV3Suffix(spec.Target)
		grouped[key] = append(grouped[key], spec)
	}

	keys := make([]string, 0, len(grouped))
	for key := range grouped {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	jobs := make([]buildJob, 0, len(keys))
	for _, key := range keys {
		jobSpecs := append([]source.Spec(nil), grouped[key]...)
		sort.SliceStable(jobSpecs, func(i, j int) bool {
			if jobSpecs[i].Format != jobSpecs[j].Format {
				return !source.FormatIsOCI(jobSpecs[i].Format)
			}
			return jobSpecs[i].Target < jobSpecs[j].Target
		})
		jobs = append(jobs, buildJob{key: key, platform: jobSpecs[0].Platform, specs: jobSpecs})
	}
	return jobs
}
