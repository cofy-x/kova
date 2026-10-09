package admissioncontract

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestReceiptRejectsPreExecutionIdentityVersions(t *testing.T) {
	spec := InstallationSpec{Namespace: "jobs", NamespaceUID: "original-jobs", ReceiptNamespace: "receipts",
		ReceiptNamespaceUID: "original-receipts", WorkerPoolID: "original-pool",
		RunnerImage: "example.com/kova/runner@sha256:" + strings.Repeat("a", 64), Generation: strings.Repeat("a", 32),
		Limits: Limits{MaxActiveJobs: 20, MaxActiveJobsPerRequester: 4, WorkerSlots: 20,
			MaxQueuedJobs: 1000, MaxQueuedJobsPerRequester: 100}}
	receipt, err := spec.ReceiptFor("original-genesis")
	if err != nil {
		t.Fatal(err)
	}
	for _, oldVersion := range []int{1, 2} {
		old := receipt
		old.Contract.Version = oldVersion
		raw, err := json.Marshal(old)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ParseReceipt(raw); err == nil {
			t.Errorf("accepted old contract version %d", oldVersion)
		}
	}
}

func TestRunnerManifestDigestRequiresImmutableCanonicalReference(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	for _, value := range []string{"example.com/kova/runner@" + digest, "localhost:5002/kova@" + digest} {
		got, err := RunnerManifestDigest(value)
		if err != nil || got != digest {
			t.Fatalf("qualified runner %q: %q, %v", value, got, err)
		}
	}
	for _, value := range []string{
		"", "example.com/kova/runner:latest", "example.com/kova/runner:v1", digest,
		"runner@" + digest, "example.com/kova/runner@sha256:" + strings.Repeat("A", 64),
		"example.com/kova/runner@sha256:abc", "example.com/kova/runner@sha512:" + strings.Repeat("a", 128),
		" example.com/kova/runner@" + digest,
	} {
		if _, err := RunnerManifestDigest(value); err == nil {
			t.Errorf("accepted noncanonical or mutable runner image %q", value)
		}
	}
}

func TestExternallyAssignedWorkerPoolIdentity(t *testing.T) {
	for _, value := range []string{"pool-epoch-17", "urn:worker-pool:region/installation-1", strings.Repeat("x", 128)} {
		if !ValidWorkerPoolID(value) {
			t.Errorf("rejected worker capacity identity %q", value)
		}
	}
	for _, value := range []string{"", "pool name", "pool\n", "pool\x00", "池", strings.Repeat("x", 129)} {
		if ValidWorkerPoolID(value) {
			t.Errorf("accepted invalid worker capacity identity %q", value)
		}
	}
}
