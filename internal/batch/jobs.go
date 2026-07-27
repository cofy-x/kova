package batch

import (
	"sort"

	"github.com/cofy-x/kova/internal/source"
	"github.com/cofy-x/kova/internal/store"
)

type buildJob struct {
	key   string
	specs []source.Spec
}

type buildResultReader interface {
	Get(target string) (store.Entry, bool, error)
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
		jobs = append(jobs, buildJob{key: key, specs: jobSpecs})
	}
	return jobs
}

func filterBuildJobs(jobs []buildJob, results buildResultReader, skipFail bool) ([]buildJob, map[string]store.OutcomeState, int, int, error) {
	existingOutcomes := make(map[string]store.OutcomeState, len(jobs))
	filtered := make([]buildJob, 0, len(jobs))
	var skippedSucceeded, skippedFailed int

	for _, job := range jobs {
		pendingSpecs := make([]source.Spec, 0, len(job.specs))
		allSucceeded := len(job.specs) > 0
		anyFailed := false

		for _, spec := range job.specs {
			entry, found, err := results.Get(spec.Target)
			if err != nil {
				return nil, nil, 0, 0, err
			}
			if !found {
				allSucceeded = false
				pendingSpecs = append(pendingSpecs, spec)
				continue
			}
			if entry.Success {
				continue
			}

			allSucceeded = false
			anyFailed = true
			if !skipFail {
				pendingSpecs = append(pendingSpecs, spec)
			}
		}

		if allSucceeded {
			existingOutcomes[job.key] = store.OutcomeSucceeded
			skippedSucceeded++
			continue
		}
		if anyFailed && skipFail {
			existingOutcomes[job.key] = store.OutcomeFailed
			skippedFailed++
			continue
		}

		filtered = append(filtered, buildJob{key: job.key, specs: pendingSpecs})
	}

	return filtered, existingOutcomes, skippedSucceeded, skippedFailed, nil
}
