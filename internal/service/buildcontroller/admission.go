package buildcontroller

import (
	"context"
	"fmt"
	"sort"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"

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
		if err := r.queueStoreForNamespace(build.Namespace).VerifyForBuild(ctx, build); err != nil {
			return admissionDecision{}, err
		}
		decision := decideAdmission(build, builds.Items, reservations.Active, r.Cfg.MaxActiveJobs, r.Cfg.MaxActiveJobsPerRequester, r.Cfg.WorkerSlots)
		if !decision.Admitted {
			return decision, nil
		}
		reservations.Active[key] = activeReservation{BuildName: build.Name, Requester: requesterKey(build), Slots: decision.Allocation}
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

func decideAdmission(build *kovav1.KovaBuild, builds []kovav1.KovaBuild, active map[string]activeReservation, maxJobs, maxRequesterJobs, workerSlots int) admissionDecision {
	activeByRequester := map[string]int{}
	usedSlots := 0
	for _, reservation := range active {
		activeByRequester[reservation.Requester]++
		usedSlots += reservation.Slots
	}
	queued := make([]*kovav1.KovaBuild, 0, len(builds))
	for i := range builds {
		item := &builds[i]
		if (item.Status.Phase == "" || item.Status.Phase == kovav1.PhaseQueued) && active[reservationKey(item)].Slots == 0 {
			queued = append(queued, item)
		}
	}
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
	for _, candidate := range fairQueue(queued) {
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
