package v1alpha1

import (
	"os"
	"regexp"
	"testing"

	"sigs.k8s.io/yaml"
)

func TestGeneratedCRDMatchesBoundedBuildContract(t *testing.T) {
	raw, err := os.ReadFile("../../../../charts/kova/crds/kova.cofy.dev_kovabuilds.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := yaml.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	versions := document["spec"].(map[string]any)["versions"].([]any)
	schema := versions[0].(map[string]any)["schema"].(map[string]any)["openAPIV3Schema"].(map[string]any)
	properties := schema["properties"].(map[string]any)
	spec := properties["spec"].(map[string]any)["properties"].(map[string]any)
	status := properties["status"].(map[string]any)["properties"].(map[string]any)
	targets := spec["targets"].(map[string]any)
	outputs := status["outputs"].(map[string]any)
	build := spec["build"].(map[string]any)["properties"].(map[string]any)
	if int(targets["maxItems"].(float64)) != MaxLogicalTargets {
		t.Fatalf("targets.maxItems = %v", targets["maxItems"])
	}
	if _, exists := targets["uniqueItems"]; exists {
		t.Fatal("targets.uniqueItems is forbidden by current Kubernetes CRD validation")
	}
	validations := schema["x-kubernetes-validations"].([]any)
	hasTargetUniqueness := false
	hasReservedSuffixValidation := false
	for _, validation := range validations {
		rule := validation.(map[string]any)["rule"].(string)
		if rule == "self.spec.targets.all(target, self.spec.targets.filter(candidate, candidate.target == target.target).size() == 1)" {
			hasTargetUniqueness = true
		}
		if rule == "self.spec.targets.all(target, !target.target.endsWith('_nydus_v3'))" {
			hasReservedSuffixValidation = true
		}
	}
	if !hasTargetUniqueness {
		t.Fatal("CRD is missing bounded target uniqueness validation")
	}
	if !hasReservedSuffixValidation {
		t.Fatal("CRD is missing reserved Nydus output suffix validation")
	}
	items := targets["items"].(map[string]any)["properties"].(map[string]any)
	target := items["target"].(map[string]any)
	if int(target["maxLength"].(float64)) != 512 || target["pattern"] == "" {
		t.Fatalf("target item schema = %#v", items)
	}
	pattern := regexp.MustCompile(target["pattern"].(string))
	for _, target := range []string{"ubuntu:dev", "registry.example.com/team/image:dev", "registry.example.com:5000/team/image:dev"} {
		if !pattern.MatchString(target) {
			t.Fatalf("target pattern rejects valid tagged reference %q", target)
		}
	}
	platform := items["platform"].(map[string]any)
	if values := platform["enum"].([]any); len(values) != 2 || values[0] != "linux/amd64" || values[1] != "linux/arm64" {
		t.Fatalf("platform schema = %#v", platform)
	}
	for _, target := range []string{"registry.example.com/team/image", "registry.example.com/team/image@sha256:deadbeef", " registry.example.com/team/image:dev"} {
		if pattern.MatchString(target) {
			t.Fatalf("target pattern accepts invalid reference %q", target)
		}
	}
	if int(outputs["maxItems"].(float64)) != MaxConcreteOutputs {
		t.Fatalf("outputs.maxItems = %v", outputs["maxItems"])
	}
	outputPlatform := outputs["items"].(map[string]any)["properties"].(map[string]any)["platform"].(map[string]any)
	if values := outputPlatform["enum"].([]any); len(values) != 2 || values[0] != "linux/amd64" || values[1] != "linux/arm64" {
		t.Fatalf("output platform schema = %#v", outputPlatform)
	}
	if int(build["concurrency"].(map[string]any)["maximum"].(float64)) != MaxBuildConcurrency {
		t.Fatalf("concurrency schema = %#v", build["concurrency"])
	}
}
