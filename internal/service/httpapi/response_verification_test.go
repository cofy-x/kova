package httpapi

import (
	"testing"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/config"
	apiv1 "github.com/cofy-x/kova/pkg/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBuildJobReportsDurableVerificationProgress(t *testing.T) {
	now := metav1.Now()
	deadline := metav1.NewTime(now.Add(5 * time.Minute))
	build := &kovav1.KovaBuild{Status: kovav1.KovaBuildStatus{
		Phase: kovav1.PhaseVerifying, VerificationStartedAt: &now, VerificationDeadlineAt: &deadline,
		VerificationNextAttemptAt: &now, VerificationAttempts: 3, VerificationLastError: "temporary registry error",
		VerificationResults: []kovav1.BuildVerificationResult{
			{State: "succeeded"}, {State: "pending"}, {State: "failed"},
		},
	}}
	job := buildJobFromCR(build, config.Config{})
	if job.Status != apiv1.JobStatusVerifying || job.VerificationStartedAt == nil || job.VerificationDeadlineAt == nil ||
		job.VerificationNextAttemptAt == nil || job.VerificationAttempts != 3 || job.VerificationLastError != "temporary registry error" ||
		job.VerificationPending != 1 || job.VerificationSucceeded != 1 || job.VerificationFailed != 1 {
		t.Fatalf("job = %#v", job)
	}
}
