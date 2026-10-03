package queueadmission

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestGenesisQueueTemplateUsesExistingBoundedSchema(t *testing.T) {
	data, err := GenesisEmptyQueueData(1000, 100)
	if err != nil {
		t.Fatal(err)
	}
	cm := &corev1.ConfigMap{Data: map[string]string{LedgerDataKey: data}}
	if err := ValidateQueueLedgerForGenesis(cm, 1000, 100); err != nil {
		t.Fatal(err)
	}
	if err := ValidateQueueLedgerForGenesis(cm, 999, 100); err == nil {
		t.Fatal("receipt capacity drift qualified")
	}
	cm.Data["extra"] = "value"
	if err := ValidateQueueLedgerForGenesis(cm, 1000, 100); err == nil {
		t.Fatal("extra data key qualified")
	}
}
