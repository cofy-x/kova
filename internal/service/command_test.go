package service

import "testing"

func TestParseNodeSelector(t *testing.T) {
	got, err := parseNodeSelector([]string{"kova.cofy.io/source-node=true", "topology.kubernetes.io/zone=cn-hongkong-b"})
	if err != nil {
		t.Fatal(err)
	}
	if got["kova.cofy.io/source-node"] != "true" || got["topology.kubernetes.io/zone"] != "cn-hongkong-b" {
		t.Fatalf("selectors = %#v", got)
	}
}

func TestParseNodeSelectorRejectsInvalidAndDuplicateValues(t *testing.T) {
	for _, values := range [][]string{
		{"missing-value"},
		{"=true"},
		{"not a key=true"},
		{"kova.cofy.io/source-node=true", "kova.cofy.io/source-node=false"},
	} {
		if _, err := parseNodeSelector(values); err == nil {
			t.Fatalf("parseNodeSelector(%#v) succeeded", values)
		}
	}
}

func TestParseNodeSelectorAllowsEmptyLabelValue(t *testing.T) {
	got, err := parseNodeSelector([]string{"kova.cofy.io/source-node="})
	if err != nil {
		t.Fatal(err)
	}
	if value, exists := got["kova.cofy.io/source-node"]; !exists || value != "" {
		t.Fatalf("selectors = %#v", got)
	}
}
