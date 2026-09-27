package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/config"
	apiv1 "github.com/cofy-x/kova/pkg/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
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

func TestFailedVerifyingProjectsAsVerifyingAndCannotBeCancelled(t *testing.T) {
	now := metav1.Now()
	deadline := metav1.NewTime(now.Add(time.Minute))
	build := &kovav1.KovaBuild{
		ObjectMeta: metav1.ObjectMeta{Name: "failed-receipts", Namespace: "jobs", Labels: map[string]string{requesterLabel: requesterID("test-user")}},
		Spec:       kovav1.KovaBuildSpec{Requester: kovav1.KovaBuildRequester{Username: "test-user"}},
		Status: kovav1.KovaBuildStatus{
			Phase: kovav1.PhaseFailedVerifying, Reason: "BuildFailed", Message: "runner failed after push",
			VerificationStartedAt: &now, VerificationDeadlineAt: &deadline, VerificationNextAttemptAt: &now,
			VerificationResults: []kovav1.BuildVerificationResult{{State: "pending"}},
		},
	}
	job := buildJobFromCR(build, config.Config{})
	if job.Status != apiv1.JobStatusVerifying || job.VerificationPending != 1 || job.VerificationDeadlineAt == nil ||
		job.VerificationNextAttemptAt == nil || job.FailureCode != "" || job.FinishedAt != nil {
		t.Fatalf("public failed-verifying projection=%#v", job)
	}
	srv := newTestServer(t, &fakeKube{})
	if err := srv.client.Create(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/builds/failed-receipts/cancel", nil)
	request.Header.Set("Authorization", "Bearer token")
	response := httptest.NewRecorder()
	srv.routes().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("cancel after runner failure status=%d body=%s", response.Code, response.Body.String())
	}
	var observed apiv1.BuildJob
	if err := json.Unmarshal(response.Body.Bytes(), &observed); err != nil {
		t.Fatal(err)
	}
	if observed.Status != apiv1.JobStatusVerifying || observed.CancellationRequested {
		t.Fatalf("cancel after runner failure job=%#v", observed)
	}
	var stored kovav1.KovaBuild
	if err := srv.client.Get(context.Background(), client.ObjectKeyFromObject(build), &stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Phase != kovav1.PhaseFailedVerifying || stored.Annotations[kovav1.CancellationRequestedAnnotation] != "" {
		t.Fatalf("cancel rewrote failed-runner receipt state: %#v", stored)
	}
}
