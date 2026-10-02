package buildcontroller

import (
	"context"
	"fmt"
	"sort"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/queueadmission"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type admissionDecision struct {
	Admitted   bool
	Allocation int
	Message    string
}

// admission commits a durable reservation before its caller may create a Pod.
// The ConfigMap resourceVersion serializes grants across reconciles and leaders.
func (r *KovaBuildReconciler) admission(ctx context.Context, build *kovav1.KovaBuild) (admissionDecision, error) {
	for retry := 0; retry < maxReservationCASAttempts; retry++ {
		if err := ctx.Err(); err != nil {
			return admissionDecision{}, err
		}
		cm, reservations, err := r.readReservations(ctx, build.Namespace)
		if err != nil {
			return admissionDecision{}, err
		}
		if err := r.ensureLiveAdmissionBuild(ctx, build); err != nil {
			return admissionDecision{}, err
		}
		key := reservationKey(build)
		// A new grant is impossible while a durable limit is exhausted. Check
		// the queue intent before waiting, but defer the namespace-wide CR and
		// Pod scans until capacity may actually be granted. A concurrent release
		// can only delay this build until its next reconcile, never overbook it.
		if _, existing := reservations.Active[key]; !existing {
			if err := r.queueStoreForNamespace(build.Namespace).VerifyForBuild(ctx, build); err != nil {
				return admissionDecision{}, err
			}
			if blocked, saturated := saturatedAdmission(build, reservations.Active, r.Cfg.MaxActiveJobs, r.Cfg.MaxActiveJobsPerRequester, r.Cfg.WorkerSlots); saturated {
				return blocked, nil
			}
		}
		var builds kovav1.KovaBuildList
		if err := r.reader().List(ctx, &builds, client.InNamespace(build.Namespace)); err != nil {
			return admissionDecision{}, err
		}
		if err := r.ensureNoUnreservedRunner(ctx, build.Namespace, reservations, builds.Items); err != nil {
			return admissionDecision{}, err
		}
		if existing, ok := reservations.Active[key]; ok {
			if existing.BuildName != build.Name || existing.Requester != requesterKey(build) {
				return admissionDecision{}, fmt.Errorf("admission reservation for %s/%s does not match KovaBuild identity", build.Namespace, build.Name)
			}
			if existing.Closing {
				return admissionDecision{}, fmt.Errorf("%w: %s/%s grant is closing", errAdmissionClosed, build.Namespace, build.Name)
			}
			return admissionDecision{Admitted: true, Allocation: existing.Slots}, nil
		}
		decision := decideAdmission(build, builds.Items, reservations.Active, r.Cfg.MaxActiveJobs, r.Cfg.MaxActiveJobsPerRequester, r.Cfg.WorkerSlots, reservations.LastGrantedRequesterHash)
		if !decision.Admitted {
			return decision, nil
		}
		reservations.Active[key] = activeReservation{BuildName: build.Name, Requester: requesterKey(build), Slots: decision.Allocation}
		// The next requester must be chosen from the last committed grant, not
		// from a process-local cursor that disappears on leader handoff. The
		// cursor and grant share one resourceVersion CAS.
		reservations.LastGrantedRequesterHash = queueadmission.HashRequester(requesterKey(build))
		if err := r.writeReservations(ctx, cm, reservations); err != nil {
			if apierrors.IsConflict(err) || apierrors.IsAlreadyExists(err) {
				if err := waitReservationCAS(ctx, retry); err != nil {
					return admissionDecision{}, err
				}
				continue
			}
			return admissionDecision{}, err
		}
		return decision, nil
	}
	return admissionDecision{}, fmt.Errorf("active admission ledger is busy granting %s/%s", build.Namespace, build.Name)
}

func saturatedAdmission(build *kovav1.KovaBuild, active map[string]activeReservation, maxJobs, maxRequesterJobs, workerSlots int) (admissionDecision, bool) {
	if maxJobs > 0 && len(active) >= maxJobs {
		return admissionDecision{Message: "waiting for an active job slot"}, true
	}
	usedSlots := 0
	activeForRequester := 0
	requester := requesterKey(build)
	for _, reservation := range active {
		usedSlots += reservation.Slots
		if reservation.Requester == requester {
			activeForRequester++
		}
	}
	if workerSlots > 0 && usedSlots >= workerSlots {
		return admissionDecision{Message: "waiting for worker capacity"}, true
	}
	if maxRequesterJobs > 0 && activeForRequester >= maxRequesterJobs {
		return admissionDecision{Message: "waiting for fair-share capacity"}, true
	}
	return admissionDecision{}, false
}

func decideAdmission(build *kovav1.KovaBuild, builds []kovav1.KovaBuild, active map[string]activeReservation, maxJobs, maxRequesterJobs, workerSlots int, lastGrantedRequesterHash string) admissionDecision {
	activeByRequester := map[string]int{}
	usedSlots := 0
	for _, reservation := range active {
		activeByRequester[reservation.Requester]++
		usedSlots += reservation.Slots
	}
	queued := queuedAdmissionCandidates(builds, active)
	jobCapacity := len(queued)
	if maxJobs > 0 {
		jobCapacity = maxJobs - len(active)
		if jobCapacity <= 0 {
			return admissionDecision{Message: "waiting for an active job slot"}
		}
	}
	slotCapacity := 0
	boundedSlots := workerSlots > 0
	if boundedSlots {
		slotCapacity = workerSlots - usedSlots
		if slotCapacity <= 0 {
			return admissionDecision{Message: "waiting for worker capacity"}
		}
	}
	for _, candidate := range fairQueue(queued, activeByRequester, lastGrantedRequesterHash) {
		requester := requesterKey(candidate)
		if maxRequesterJobs > 0 && activeByRequester[requester] >= maxRequesterJobs {
			continue
		}
		allocation := requestedConcurrency(candidate)
		if boundedSlots && allocation > slotCapacity {
			allocation = slotCapacity
		}
		if allocation <= 0 || jobCapacity <= 0 {
			break
		}
		if reservationKey(candidate) == reservationKey(build) {
			return admissionDecision{Admitted: true, Allocation: allocation}
		}
		activeByRequester[requester]++
		jobCapacity--
		if boundedSlots {
			slotCapacity -= allocation
		}
	}
	return admissionDecision{Message: "waiting for fair-share capacity"}
}

func queuedAdmissionCandidates(builds []kovav1.KovaBuild, active map[string]activeReservation) []*kovav1.KovaBuild {
	queued := make([]*kovav1.KovaBuild, 0, len(builds))
	for i := range builds {
		item := &builds[i]
		// Deleting and cancellation-requested builds can remain Queued while
		// cleanup is blocked. They will never receive a real grant, so they
		// must not consume one in the virtual fair-share allocation either.
		if item.Labels["kova.cofy.dev/recovery-tombstone"] == "" &&
			item.DeletionTimestamp.IsZero() && !cancellationRequested(item) &&
			(item.Status.Phase == "" || item.Status.Phase == kovav1.PhaseQueued) && active[reservationKey(item)].Slots == 0 {
			queued = append(queued, item)
		}
	}
	return queued
}

func fairQueue(builds []*kovav1.KovaBuild, activeByRequester map[string]int, lastGrantedRequesterHash string) []*kovav1.KovaBuild {
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
		if activeByRequester[requesters[i]] != activeByRequester[requesters[j]] {
			return activeByRequester[requesters[i]] < activeByRequester[requesters[j]]
		}
		left, right := groups[requesters[i]][0], groups[requesters[j]][0]
		if buildLess(left, right) {
			return true
		}
		if buildLess(right, left) {
			return false
		}
		return requesters[i] < requesters[j]
	})
	// Prefer requesters with fewer current grants, then their oldest queued
	// jobs. Within the cursor's equal-grant tier, rotate after its last durable
	// grant. Without this rotation, a single released slot always goes back to
	// the oldest requester's backlog. Never rotate a more-served requester in
	// front of a less-served one after concurrent out-of-order grants.
	for i, requester := range requesters {
		if queueadmission.HashRequester(requester) == lastGrantedRequesterHash {
			start, end := i, i+1
			for start > 0 && activeByRequester[requesters[start-1]] == activeByRequester[requester] {
				start--
			}
			for end < len(requesters) && activeByRequester[requesters[end]] == activeByRequester[requester] {
				end++
			}
			rotated := append([]string{}, requesters[i+1:end]...)
			rotated = append(rotated, requesters[start:i+1]...)
			copy(requesters[start:end], rotated)
			break
		}
	}
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
