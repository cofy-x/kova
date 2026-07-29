package service

import "testing"

func TestParseNodeSelector(t *testing.T) {
	got, err := parseNodeSelector([]string{"kova.cofy.io/source-node=true", "topology.kubernetes.io/zone=zone-b"})
	if err != nil {
		t.Fatal(err)
	}
	if got["kova.cofy.io/source-node"] != "true" || got["topology.kubernetes.io/zone"] != "zone-b" {
		t.Fatalf("selectors = %#v", got)
	}
}

func TestParseRegistryHosts(t *testing.T) {
	hosts, err := parseRegistryHosts([]string{" Registry.Example:5000 ", "registry.example:5000"})
	if err != nil {
		t.Fatal(err)
	}
	if len(hosts) != 1 || hosts[0] != "registry.example:5000" {
		t.Fatalf("hosts = %#v", hosts)
	}
	if _, err := parseRegistryHosts([]string{"http://registry.example:5000"}); err == nil {
		t.Fatal("expected scheme to be rejected")
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

func TestRunnerObservabilityEnvUsesRunnerServiceName(t *testing.T) {
	t.Setenv("KOVA_OTEL_ENABLED", "true")
	t.Setenv("OTEL_SERVICE_NAME", "kova-controller")
	t.Setenv("KOVA_RUNNER_OTEL_SERVICE_NAME", "kova-runner")
	env := runnerObservabilityEnv()
	if env["KOVA_OTEL_ENABLED"] != "true" || env["OTEL_SERVICE_NAME"] != "kova-runner" {
		t.Fatalf("runner env = %#v", env)
	}
}
