package buildcontroller

import (
	"context"
	"errors"
	"testing"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/config"
	"github.com/cofy-x/kova/internal/service/queueadmission"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Return the original active snapshot, then commit the competing lifecycle
// writes before the pump decodes it. This makes the handoff interleaving
// deterministic without clocks, concurrent goroutines, or a real cluster.
type pumpHandoffReader struct {
	client.Reader
	afterSnapshot func()
	afterBuildGet func()
}

func (r *pumpHandoffReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if err := r.Reader.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	if _, isConfigMap := obj.(*corev1.ConfigMap); isConfigMap && key.Name == reservationConfigMap && r.afterSnapshot != nil {
		afterSnapshot := r.afterSnapshot
		r.afterSnapshot = nil
		afterSnapshot()
	}
	if _, isBuild := obj.(*kovav1.KovaBuild); isBuild && r.afterBuildGet != nil {
		afterBuildGet := r.afterBuildGet
		r.afterBuildGet = nil
		afterBuildGet()
	}
	return nil
}

func preparePumpQueueIntent(t *testing.T, base client.Client, cfg config.Config, build *kovav1.KovaBuild) queueadmission.Store {
	t.Helper()
	ctx := context.Background()
	queue := queueadmission.Store{Client: base, Reader: base, Namespace: "jobs",
		GlobalLimit: cfg.MaxQueuedJobs, RequesterLimit: cfg.MaxQueuedJobsPerRequester}
	intent, fresh, err := queue.Reserve(ctx, build)
	if err != nil || !fresh {
		t.Fatalf("queue intent fresh=%v err=%v", fresh, err)
	}
	build.Annotations = map[string]string{queueadmission.IntentAnnotation: intent.Nonce}
	if err := base.Update(ctx, build); err != nil {
		t.Fatal(err)
	}
	build.Status.Phase = kovav1.PhaseQueued
	if err := base.Status().Update(ctx, build); err != nil {
		t.Fatal(err)
	}
	return queue
}

func TestAdmissionPumpRecoversConcurrentGrantBeforeStarting(t *testing.T) {
	ctx := context.Background()
	cfg := admissionConfig()
	cfg.MaxActiveJobs, cfg.WorkerSlots, cfg.MaxActiveJobsPerRequester = 2, 2, 1
	first := queuedBuild("alice-first", "alice", 1, 1)
	aliceBacklog := queuedBuild("alice-backlog", "alice", 2, 1)
	bob := queuedBuild("bob", "bob", 3, 1)
	p, r, base := pumpFixture(t, cfg, first, aliceBacklog, bob)
	queue := preparePumpQueueIntent(t, base, cfg, first)
	counter := &pumpReadCounter{Reader: base}
	p.Reader = &pumpHandoffReader{Reader: counter, afterSnapshot: func() {
		decision, err := r.admission(ctx, first)
		if err != nil || !decision.Admitted {
			t.Fatalf("concurrent grant=%#v err=%v", decision, err)
		}
		if err := queue.ReleaseForBuild(ctx, first); err != nil {
			t.Fatal(err)
		}
	}}
	result, err := p.Reconcile(ctx, pumpRequest(reservationConfigMap))
	if err != nil || result.Requeue || result.RequeueAfter != admissionHandoffRetry {
		t.Fatalf("legal handoff entered failure backoff: result=%#v err=%v", result, err)
	}
	if got := wakeName(t, p); got != first.Name {
		t.Fatalf("handoff wake=%q, want grant holder %q", got, first.Name)
	}
	assertNoWake(t, p)
	// The recovery path adds just one ledger/CR reread, never a full-table
	// retry loop. It cannot itself grant a slot or advance the fairness cursor.
	if counter.activeGets != 2 || counter.queueGets != 3 || counter.buildGets != 2 || counter.buildLists != 1 || counter.podLists != 1 {
		t.Fatalf("unbounded handoff reads: active=%d queue=%d build=%d buildLists=%d podLists=%d",
			counter.activeGets, counter.queueGets, counter.buildGets, counter.buildLists, counter.podLists)
	}
	_, state, err := r.readReservations(ctx, "jobs")
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Active) != 1 || state.Active[reservationKey(first)].BuildName != first.Name ||
		state.LastGrantedRequesterHash != queueadmission.HashRequester("alice") {
		t.Fatalf("pump changed durable grants or fairness cursor: %#v", state)
	}
	var stored kovav1.KovaBuild
	if err := base.Get(ctx, client.ObjectKeyFromObject(first), &stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Phase != kovav1.PhaseQueued {
		t.Fatalf("fixture did not preserve pre-Starting handoff: %q", stored.Status.Phase)
	}
	// The single healthy retry must select from the new ledger, not grant
	// around Alice or prefer her older backlog while her requester slot is full.
	result, err = p.Reconcile(ctx, pumpRequest(reservationConfigMap))
	if err != nil || result.RequeueAfter != 0 || result.Requeue {
		t.Fatalf("fresh-state retry=%#v err=%v", result, err)
	}
	if got := wakeName(t, p); got != bob.Name {
		t.Fatalf("fresh-state fair wake=%q, want %q", got, bob.Name)
	}
	assertNoWake(t, p)
}

func TestAdmissionPumpConcurrentGrantStillFailsClosed(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*reservationState, *kovav1.KovaBuild)
	}{
		{name: "missing grant", mutate: func(state *reservationState, build *kovav1.KovaBuild) {
			delete(state.Active, reservationKey(build))
		}},
		{name: "wrong UID", mutate: func(state *reservationState, build *kovav1.KovaBuild) {
			entry := state.Active[reservationKey(build)]
			delete(state.Active, reservationKey(build))
			state.Active["different-uid"] = entry
		}},
		{name: "wrong name", mutate: func(state *reservationState, build *kovav1.KovaBuild) {
			entry := state.Active[reservationKey(build)]
			entry.BuildName = "different-name"
			state.Active[reservationKey(build)] = entry
		}},
		{name: "wrong requester", mutate: func(state *reservationState, build *kovav1.KovaBuild) {
			entry := state.Active[reservationKey(build)]
			entry.Requester = "different-requester"
			state.Active[reservationKey(build)] = entry
		}},
		{name: "closing grant", mutate: func(state *reservationState, build *kovav1.KovaBuild) {
			entry := state.Active[reservationKey(build)]
			entry.Closing = true
			state.Active[reservationKey(build)] = entry
		}},
		{name: "oversized allocation", mutate: func(state *reservationState, build *kovav1.KovaBuild) {
			entry := state.Active[reservationKey(build)]
			entry.Slots = requestedConcurrency(build) + 1
			state.Active[reservationKey(build)] = entry
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			cfg := admissionConfig()
			bad := queuedBuild("bad", "alice", 1, 1)
			good := queuedBuild("good", "bob", 2, 1)
			p, r, base := pumpFixture(t, cfg, bad, good)
			queue := preparePumpQueueIntent(t, base, cfg, bad)
			p.Reader = &pumpHandoffReader{Reader: base, afterSnapshot: func() {
				cm, state, err := r.readReservations(ctx, "jobs")
				if err != nil {
					t.Fatal(err)
				}
				state.Active[reservationKey(bad)] = activeReservation{BuildName: bad.Name, Requester: requesterKey(bad), Slots: 1}
				test.mutate(&state, bad)
				if err := r.writeReservations(ctx, cm, state); err != nil {
					t.Fatal(err)
				}
				if err := queue.ReleaseForBuild(ctx, bad); err != nil {
					t.Fatal(err)
				}
			}}
			result, err := p.Reconcile(ctx, pumpRequest(reservationConfigMap))
			if !errors.Is(err, queueadmission.ErrDrift) || result.RequeueAfter != 0 {
				t.Fatalf("unproved handoff allowed: result=%#v err=%v", result, err)
			}
			if got := wakeName(t, p); got != bad.Name {
				t.Fatalf("drift recovery wake=%q, want %q", got, bad.Name)
			}
			assertNoWake(t, p)
		})
	}
}

func TestAdmissionPumpConcurrentGrantDoesNotHideLedgerLossOrLimitDrift(t *testing.T) {
	for _, test := range []string{"queue ledger loss", "active limit drift", "changed queue intent", "replacement CR UID"} {
		t.Run(test, func(t *testing.T) {
			ctx := context.Background()
			cfg := admissionConfig()
			build := queuedBuild("first", "alice", 1, 1)
			p, r, base := pumpFixture(t, cfg, build)
			queue := preparePumpQueueIntent(t, base, cfg, build)
			handoffReader := &pumpHandoffReader{Reader: base, afterSnapshot: func() {
				decision, err := r.admission(ctx, build)
				if err != nil || !decision.Admitted {
					t.Fatalf("concurrent grant=%#v err=%v", decision, err)
				}
				if err := queue.ReleaseForBuild(ctx, build); err != nil {
					t.Fatal(err)
				}
				if test == "active limit drift" {
					cm, state, err := r.readReservations(ctx, "jobs")
					if err != nil {
						t.Fatal(err)
					}
					state.WorkerSlots++
					if err := r.writeReservations(ctx, cm, state); err != nil {
						t.Fatal(err)
					}
				}
			}, afterBuildGet: func() {
				switch test {
				case "queue ledger loss":
					if err := base.Delete(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: queueadmission.ConfigMapName, Namespace: "jobs"}}); err != nil {
						t.Fatal(err)
					}
				case "changed queue intent":
					changed := build.DeepCopy()
					changed.Spec.Source.Digest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
					if _, fresh, err := queue.Reserve(ctx, changed); err != nil || !fresh {
						t.Fatalf("replacement queue intent fresh=%v err=%v", fresh, err)
					}
				case "replacement CR UID":
					var current kovav1.KovaBuild
					if err := base.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
						t.Fatal(err)
					}
					current.UID = types.UID("replacement-uid")
					if err := base.Update(ctx, &current); err != nil {
						t.Fatal(err)
					}
				}
			}}
			p.Reader = handoffReader
			result, err := p.Reconcile(ctx, pumpRequest(reservationConfigMap))
			if err == nil || result.RequeueAfter != 0 {
				t.Fatalf("ledger/identity drift was masked: result=%#v err=%v", result, err)
			}
			if test == "queue ledger loss" && !apierrors.IsNotFound(err) {
				t.Fatalf("queue ledger loss error=%v", err)
			}
		})
	}
}
