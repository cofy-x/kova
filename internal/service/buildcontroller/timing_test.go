package buildcontroller

import (
	"testing"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestRunnerStartupSegmentsSplitSourceFromScheduling(t *testing.T) {
	base := time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC)
	at := func(seconds int) metav1.Time { return metav1.NewTime(base.Add(time.Duration(seconds) * time.Second)) }
	build := &kovav1.KovaBuild{ObjectMeta: metav1.ObjectMeta{CreationTimestamp: at(0)}}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{CreationTimestamp: at(3)},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodScheduled, Status: corev1.ConditionTrue, LastTransitionTime: at(5)},
				{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: at(19)},
			},
			InitContainerStatuses: []corev1.ContainerStatus{{Name: "source-fetch", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{StartedAt: at(9), FinishedAt: at(16)}}}},
		},
	}
	got := runnerStartupSegments(build, pod)
	want := []startupSegment{{"queue_to_pod", 3 * time.Second}, {"pod_schedule", 2 * time.Second}, {"source_wait", 4 * time.Second}, {"source_fetch", 7 * time.Second}, {"runner_ready", 3 * time.Second}, {"pod_startup", 16 * time.Second}}
	if len(got) != len(want) {
		t.Fatalf("segments=%v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("segment %d=%v, want %v", i, got[i], want[i])
		}
	}
	// Failed init / missing transitions are not successful source timings.
	pod.Status.InitContainerStatuses[0].State.Terminated.ExitCode = 1
	pod.Status.Conditions[0].LastTransitionTime = at(2) // clock skew
	pod.Status.Conditions[1].Status = corev1.ConditionFalse
	got = runnerStartupSegments(build, pod)
	if len(got) != 1 || got[0].stage != "queue_to_pod" {
		t.Fatalf("invalid timestamps fabricated segments: %v", got)
	}
}

func TestRunnerStartupSegmentsOmitUnknownTimestamps(t *testing.T) {
	if got := runnerStartupSegments(&kovav1.KovaBuild{}, &corev1.Pod{}); len(got) != 0 {
		t.Fatalf("unknown timestamps must remain unknown: %v", got)
	}
}
