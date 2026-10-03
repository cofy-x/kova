package admissioncontract

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Reader is the complete API surface available to declarative installer
// rendering. It cannot create or patch a Namespace, Genesis, ledger, or Secret.
type Reader interface {
	GetNamespace(context.Context, string) (*corev1.Namespace, error)
	GetConfigMap(context.Context, string, string) (*corev1.ConfigMap, error)
}

// DirectReader wraps typed, named, uncached GETs. The clientset is private so
// CLI code cannot obtain its mutation methods through this helper.
type DirectReader struct{ client kubernetes.Interface }

func NewDirectReader(client kubernetes.Interface) DirectReader { return DirectReader{client: client} }

func (r DirectReader) GetNamespace(ctx context.Context, name string) (*corev1.Namespace, error) {
	if r.client == nil {
		return nil, fmt.Errorf("admission direct reader is missing")
	}
	return r.client.CoreV1().Namespaces().Get(ctx, name, metav1.GetOptions{})
}

func (r DirectReader) GetConfigMap(ctx context.Context, namespace, name string) (*corev1.ConfigMap, error) {
	if r.client == nil {
		return nil, fmt.Errorf("admission direct reader is missing")
	}
	return r.client.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
}
