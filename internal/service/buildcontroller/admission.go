package buildcontroller

import (
	"context"
	"sort"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

type admissionDecision struct {
	Admitted   bool
	Allocation int
	Message    string
}

func (r *KovaBuildReconciler) admission(ctx context.Context, build *kovav1.KovaBuild) (admissionDecision, error) {
	var builds kovav1.KovaBuildList
	if err := r.List(ctx, &builds, client.InNamespace(build.Namespace)); err != nil {
		return admissionDecision{}, err
	}
	active := 0
	usedSlots := 0
	activeByRequester := map[string]int{}
	queued := make([]*kovav1.KovaBuild, 0, len(builds.Items))
	for i := range builds.Items {
		item := &builds.Items[i]
		switch item.Status.Phase {
		case kovav1.PhaseStarting, kovav1.PhaseRunning:
			active++
			activeByRequester[requesterKey(item)]++
			usedSlots += allocatedConcurrency(item)
		case "", kovav1.PhaseQueued:
			queued = append(queued, item)
		}
	}
	jobCapacity := len(queued)
	if r.Cfg.MaxActiveJobs > 0 {
		jobCapacity = r.Cfg.MaxActiveJobs - active
		if jobCapacity <= 0 {
			return admissionDecision{Message: "waiting for an active job slot"}, nil
		}
	}
	slotCapacity := 0
	boundedSlots := r.Cfg.WorkerSlots > 0
	if boundedSlots {
		slotCapacity = r.Cfg.WorkerSlots - usedSlots
		if slotCapacity <= 0 {
			return admissionDecision{Message: "waiting for worker capacity"}, nil
		}
	}
	for _, candidate := range fairQueue(queued) {
		requester := requesterKey(candidate)
		if r.Cfg.MaxActiveJobsPerRequester > 0 && activeByRequester[requester] >= r.Cfg.MaxActiveJobsPerRequester {
			continue
		}
		allocation := requestedConcurrency(candidate)
		if boundedSlots && allocation > slotCapacity {
			allocation = slotCapacity
		}
		if allocation <= 0 || jobCapacity <= 0 {
			break
		}
		if candidate.Name == build.Name {
			return admissionDecision{Admitted: true, Allocation: allocation}, nil
		}
		activeByRequester[requester]++
		jobCapacity--
		if boundedSlots {
			slotCapacity -= allocation
		}
	}
	return admissionDecision{Message: "waiting for fair-share capacity"}, nil
}

func fairQueue(builds []*kovav1.KovaBuild) []*kovav1.KovaBuild {
	groups := map[string][]*kovav1.KovaBuild{}
	for _, build := range builds {
		key := requesterKey(build)
		groups[key] = append(groups[key], build)
	}
	for _, group := range groups {
		sort.Slice(group, func(i, j int) bool { return buildLess(group[i], group[j]) })
	}
	requesters := make([]string, 0, len(groups))
	for requester := range groups {
		requesters = append(requesters, requester)
	}
	sort.Slice(requesters, func(i, j int) bool {
		left, right := groups[requesters[i]][0], groups[requesters[j]][0]
		if buildLess(left, right) {
			return true
		}
		if buildLess(right, left) {
			return false
		}
		return requesters[i] < requesters[j]
	})
	ordered := make([]*kovav1.KovaBuild, 0, len(builds))
	for round := 0; len(ordered) < len(builds); round++ {
		for _, requester := range requesters {
			if round < len(groups[requester]) {
				ordered = append(ordered, groups[requester][round])
			}
		}
	}
	return ordered
}

func buildLess(left, right *kovav1.KovaBuild) bool {
	if left.CreationTimestamp.Equal(&right.CreationTimestamp) {
		return left.Name < right.Name
	}
	return left.CreationTimestamp.Before(&right.CreationTimestamp)
}

func requesterKey(build *kovav1.KovaBuild) string {
	if build.Spec.Requester.Username != "" {
		return build.Spec.Requester.Username
	}
	return "unknown/" + build.Name
}

func requestedConcurrency(build *kovav1.KovaBuild) int {
	if build.Spec.Build.Concurrency > 0 {
		return build.Spec.Build.Concurrency
	}
	return 1
}

func allocatedConcurrency(build *kovav1.KovaBuild) int {
	if build.Status.AllocatedConcurrency > 0 {
		return int(build.Status.AllocatedConcurrency)
	}
	return requestedConcurrency(build)
}
