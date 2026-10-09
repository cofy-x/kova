package recoverydisposal

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestDeletionGenerationTransitionIsNarrowAndPreservesBody(t *testing.T) {
	for _, role := range []string{"original-build", "original-pod"} {
		for _, scenario := range []string{"first-delete", "already-marked-by-namespace-gc", "before-delete-drift", "plus-two", "spec-drift", "status-drift", "already-deleting-generation-drift"} {
			t.Run(role+"/"+scenario, func(t *testing.T) {
				f := newExecutorFixture(t)
				key := executorTargetKey(f, role)
				obj := f.api.objects[key]
				if obj.GetGeneration() != 1 {
					t.Fatal("fixture must represent a real positive-generation object")
				}
				if scenario == "first-delete" {
					// Fake API now follows first-Delete generation 1 -> 2.
				} else if scenario == "before-delete-drift" {
					obj.SetGeneration(2)
				} else {
					obj.SetDeletionTimestamp(&metav1.Time{Time: time.Now().UTC()})
					obj.SetGeneration(2)
					switch scenario {
					case "plus-two":
						obj.SetGeneration(3)
					case "spec-drift":
						obj.Object["spec"] = map[string]any{"changed": "must-not-normalize"}
					case "status-drift":
						obj.Object["status"] = map[string]any{"phase": "Succeeded"}
					case "already-deleting-generation-drift":
						// Requalify an already-deleting gen2 archive. A later gen3
						// is not another legitimate first deletion transition.
						for i, target := range f.plan.payload.Targets {
							if target.Role == role {
								archived := obj.DeepCopy()
								archived.SetResourceVersion(target.ArchiveResourceVersion)
								raw, _ := json.Marshal(archived.Object)
								target.ArchiveBytes = int64(len(raw))
								target.ArchiveDigest = digestBytes(raw)
								var err error
								target.QualificationDigest, err = DigestExecutionQualification(target, raw)
								if err != nil {
									t.Fatal(err)
								}
								f.plan.payload.Targets[i] = target
								f.archives[i] = raw
							}
						}
						obj.SetGeneration(3)
					}
				}
				report, err := Execute(context.Background(), f.api, f.plan, f.grant, f.archives)
				valid := scenario == "first-delete" || scenario == "already-marked-by-namespace-gc"
				if valid && err != nil {
					t.Fatalf("legitimate first deletion refused: %+v %v", report, err)
				}
				if !valid && (err == nil || len(f.api.writes) != 0) {
					t.Fatalf("generation/body drift did not block before writes: %+v %v", report, err)
				}
				if scenario == "already-marked-by-namespace-gc" {
					for _, write := range f.api.writes {
						if write == "DELETE:"+key {
							t.Fatal("already deleting object was deleted again")
						}
					}
				}
			})
		}
	}
}

func TestBackgroundBuiltInFinalizersRefuseCompletePlanBeforeAnyWrite(t *testing.T) {
	for _, finalizer := range []string{metav1.FinalizerOrphanDependents, metav1.FinalizerDeleteDependents} {
		for _, where := range []string{"archive", "current"} {
			t.Run(finalizer+"/"+where, func(t *testing.T) {
				f := newExecutorFixture(t)
				key := executorTargetKey(f, "original-build")
				if where == "current" {
					obj := f.api.objects[key]
					obj.SetFinalizers(append(obj.GetFinalizers(), finalizer))
				} else {
					for i, target := range f.plan.payload.Targets {
						if target.Role == "original-build" {
							var obj unstructured.Unstructured
							if json.Unmarshal(f.archives[i], &obj.Object) != nil {
								t.Fatal("archive decode")
							}
							obj.SetFinalizers(append(obj.GetFinalizers(), finalizer))
							raw, _ := json.Marshal(obj.Object)
							f.archives[i] = raw
							f.plan.payload.Targets[i].ArchiveBytes = int64(len(raw))
							f.plan.payload.Targets[i].ArchiveDigest = digestBytes(raw)
						}
					}
				}
				report, err := Execute(context.Background(), f.api, f.plan, f.grant, f.archives)
				if err == nil || len(f.api.writes) != 0 {
					t.Fatalf("Background finalizer side effect not refused: %+v %v", report, err)
				}
				if where == "archive" && len(f.api.calls) != 0 {
					t.Fatal("unqualified archive reached API")
				}
				for _, call := range f.api.calls {
					if strings.HasPrefix(call, "DELETE:") || strings.HasPrefix(call, "PATCH:") {
						t.Fatal("mutating preflight")
					}
				}
			})
		}
	}
}
