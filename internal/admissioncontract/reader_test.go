package admissioncontract

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestDirectReaderUsesOnlyNamedGets(t *testing.T) {
	client := fake.NewClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "jobs-57"}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "jobs-57", Name: GenesisName}},
	)
	reader := NewDirectReader(client)
	if _, err := reader.GetNamespace(context.Background(), "jobs-57"); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.GetConfigMap(context.Background(), "jobs-57", GenesisName); err != nil {
		t.Fatal(err)
	}
	actions := client.Actions()
	if len(actions) != 2 || actions[0].GetVerb() != "get" || actions[0].GetResource().Resource != "namespaces" ||
		actions[1].GetVerb() != "get" || actions[1].GetResource().Resource != "configmaps" ||
		actions[1].GetNamespace() != "jobs-57" {
		t.Fatalf("installer reader used an unexpected API action: %#v", actions)
	}
	for _, action := range actions {
		get, ok := action.(interface{ GetName() string })
		if !ok || get.GetName() == "" {
			t.Fatalf("installer reader did not perform a named GET: %#v", action)
		}
	}
}
