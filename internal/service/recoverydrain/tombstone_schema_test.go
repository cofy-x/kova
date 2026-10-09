package recoverydrain

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/cofy-x/kova/internal/service/recoverypermit"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
)

// The fake Kubernetes client does not apply CRD OpenAPI/CEL validation. Test
// the shipped OpenAPI shape and CEL-critical field; an isolated real API gate
// is still required before declaring actual API acceptance.
func TestBuildTombstoneFitsShippedCRDShape(t *testing.T) {
	path := filepath.Join("..", "..", "..", "charts", "kova", "crds", "kova.cofy.dev_kovabuilds.yaml")
	rawCRD, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var crd apiextensionsv1.CustomResourceDefinition
	if err := yaml.Unmarshal(rawCRD, &crd); err != nil {
		t.Fatal(err)
	}
	if len(crd.Spec.Versions) != 1 || crd.Spec.Versions[0].Name != "v1alpha1" ||
		crd.Spec.Versions[0].Schema == nil || crd.Spec.Versions[0].Schema.OpenAPIV3Schema == nil {
		t.Fatal("unexpected shipped KovaBuild CRD version/schema")
	}
	expected := recoverypermit.Expectation{IncidentID: "incident-one",
		Epoch: recoverypermit.EpochIdentity{Namespace: "runner-old", NamespaceUID: "namespace-uid"}}
	raw, err := json.Marshal(newBuildTombstone(expected, "build-one"))
	if err != nil {
		t.Fatal(err)
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		t.Fatal(err)
	}
	rootSchema := crd.Spec.Versions[0].Schema.OpenAPIV3Schema
	specSchema := rootSchema.Properties["spec"]
	if !slices.Contains(rootSchema.Required, "spec") ||
		!slices.Contains(specSchema.Required, "requester") ||
		!slices.Contains(specSchema.Required, "source") ||
		!slices.Contains(specSchema.Required, "targets") {
		t.Fatal("shipped CRD required fields changed; re-review tombstone")
	}
	spec := obj["spec"].(map[string]any)
	buildSpec := spec["build"].(map[string]any)
	concurrencySchema := specSchema.Properties["build"].Properties["concurrency"]
	if buildSpec["concurrency"] != float64(1) {
		t.Fatalf("CEL-critical concurrency missing: %#v", buildSpec)
	}
	if concurrencySchema.Minimum == nil || *concurrencySchema.Minimum > 1 ||
		concurrencySchema.Maximum == nil || *concurrencySchema.Maximum < 1 {
		t.Fatal("tombstone concurrency does not fit shipped schema")
	}
	targets := spec["targets"].([]any)
	targetSchema := specSchema.Properties["targets"]
	if len(targets) != 1 || targetSchema.MinItems == nil || *targetSchema.MinItems > 1 ||
		targetSchema.Items == nil || targetSchema.Items.Schema == nil {
		t.Fatal("tombstone target shape does not fit shipped schema")
	}
	target := targets[0].(map[string]any)
	itemSchema := targetSchema.Items.Schema
	if !slices.Contains(itemSchema.Required, "target") || !slices.Contains(itemSchema.Required, "platform") ||
		!regexp.MustCompile(itemSchema.Properties["target"].Pattern).MatchString(target["target"].(string)) ||
		target["platform"] != "linux/amd64" {
		t.Fatal("tombstone target does not fit shipped schema")
	}
	source := spec["source"].(map[string]any)
	sourceSchema := specSchema.Properties["source"]
	if !slices.Contains(sourceSchema.Required, "uri") || !slices.Contains(sourceSchema.Required, "digest") ||
		!regexp.MustCompile(sourceSchema.Properties["uri"].Pattern).MatchString(source["uri"].(string)) ||
		!regexp.MustCompile(sourceSchema.Properties["digest"].Pattern).MatchString(source["digest"].(string)) {
		t.Fatal("tombstone source does not fit shipped schema")
	}
	var concurrencyRule bool
	for _, rule := range crd.Spec.Versions[0].Schema.OpenAPIV3Schema.XValidations {
		if strings.Contains(rule.Rule, "self.spec.build.concurrency") && strings.Contains(rule.Rule, "size(self.spec.targets)") {
			concurrencyRule = true
		}
	}
	if !concurrencyRule {
		t.Fatal("shipped CRD concurrency rule changed; re-review inert tombstone spec")
	}
}
