package httpapi

import (
	"testing"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/config"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBuildJobReportsAdmissionRecoveryWithoutChangingOutcome(t *testing.T) {
	build := &kovav1.KovaBuild{Status: kovav1.KovaBuildStatus{
		Phase: kovav1.PhaseSucceeded,
		Conditions: []metav1.Condition{{
			Type: "AdmissionRecoveryRequired", Status: metav1.ConditionTrue,
			Reason: "PodCreateOutcomeUnknown", Message: "capacity remains reserved",
		}},
	}}
	job := buildJobFromCR(build, config.Config{})
	if string(job.Status) != "succeeded" || !job.RecoveryRequired {
		t.Fatalf("job = %#v", job)
	}
}
