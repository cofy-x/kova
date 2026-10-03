package buildcontroller

import (
	"testing"

	"github.com/cofy-x/kova/internal/service/config"
	corev1 "k8s.io/api/core/v1"
)

func TestGenesisActiveTemplateUsesExistingBoundedSchema(t *testing.T) {
	cfg := config.Config{MaxActiveJobs: 128, MaxActiveJobsPerRequester: 8, WorkerSlots: 65535}
	data, err := GenesisEmptyAdmissionData(cfg)
	if err != nil {
		t.Fatal(err)
	}
	cm := &corev1.ConfigMap{Data: map[string]string{AdmissionLedgerDataKey: data}}
	if err := ValidateAdmissionLedgerForGenesis(cm, cfg); err != nil {
		t.Fatal(err)
	}
	drift := cfg
	drift.MaxActiveJobs--
	if err := ValidateAdmissionLedgerForGenesis(cm, drift); err == nil {
		t.Fatal("receipt capacity drift qualified")
	}
	cm.Data["extra"] = "value"
	if err := ValidateAdmissionLedgerForGenesis(cm, cfg); err == nil {
		t.Fatal("extra data key qualified")
	}
}
