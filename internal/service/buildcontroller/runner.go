package buildcontroller

import (
	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/runner"

	corev1 "k8s.io/api/core/v1"
)

func sourcePath(build *kovav1.KovaBuild) string {
	return runner.MaterializedSourcePath
}

func buildPodName(name string) string {
	return "kova-job-" + name
}

func podReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}
