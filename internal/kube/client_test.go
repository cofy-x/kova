package kube

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestGetSecretData(t *testing.T) {
	kube := &Client{clientset: fake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "registry", Namespace: "jobs"},
		Data: map[string][]byte{
			"kova-image": []byte("registry.local/kova:dev"),
		},
	})}

	value, err := kube.GetSecretData(context.Background(), "jobs", "registry", "kova-image")
	if err != nil {
		t.Fatal(err)
	}
	if value != "registry.local/kova:dev" {
		t.Fatalf("secret value = %q", value)
	}
}

func TestPodExists(t *testing.T) {
	kube := &Client{clientset: fake.NewSimpleClientset(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: "jobs"},
	})}

	exists, err := kube.PodExists(context.Background(), "jobs", "runner")
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("expected runner pod to exist")
	}
	exists, err = kube.PodExists(context.Background(), "jobs", "missing")
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("missing pod should not exist")
	}
}

func TestWaitPodReady(t *testing.T) {
	kube := &Client{clientset: fake.NewSimpleClientset(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: "jobs"},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	})}

	if err := kube.WaitPodReady(context.Background(), "jobs", "runner", time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestScaleDeployment(t *testing.T) {
	replicas := int32(1)
	kube := &Client{clientset: fake.NewSimpleClientset(&appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "kova", Namespace: "kova"},
		Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
	})}

	if err := kube.ScaleDeployment(context.Background(), "kova", "kova", 3); err != nil {
		t.Fatal(err)
	}
	deployment, err := kube.clientset.AppsV1().Deployments("kova").Get(context.Background(), "kova", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas != 3 {
		t.Fatalf("replicas = %#v", deployment.Spec.Replicas)
	}
}

func TestListPods(t *testing.T) {
	kube := &Client{clientset: fake.NewSimpleClientset(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: "jobs", CreationTimestamp: metav1.Now()},
		Spec:       corev1.PodSpec{NodeName: "kind-worker", Containers: []corev1.Container{{Name: "runner"}}},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.244.0.10",
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "runner", Ready: true, RestartCount: 1},
			},
		},
	})}

	var out bytes.Buffer
	if err := kube.ListPods(context.Background(), "jobs", &out, true); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	for _, want := range []string{"NAME", "runner", "1/1", "Running", "10.244.0.10", "kind-worker"} {
		if !strings.Contains(text, want) {
			t.Fatalf("expected list output to contain %q:\n%s", want, text)
		}
	}
}

func TestListPodsWithOptionsFiltersByLabelSelector(t *testing.T) {
	kube := &Client{clientset: fake.NewSimpleClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "kova-worker",
				Namespace:         "kova",
				CreationTimestamp: metav1.Now(),
				Labels: map[string]string{
					"app.kubernetes.io/name":     "kova",
					"app.kubernetes.io/instance": "kova",
				},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "otel-collector",
				Namespace:         "kova",
				CreationTimestamp: metav1.Now(),
				Labels:            map[string]string{"app.kubernetes.io/name": "otel-collector"},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		},
	)}

	var out bytes.Buffer
	err := kube.ListPodsWithOptions(context.Background(), "kova", &out, ListPodsOptions{
		Wide:          true,
		LabelSelector: "app.kubernetes.io/name=kova,app.kubernetes.io/instance=kova",
	})
	if err != nil {
		t.Fatal(err)
	}
	text := out.String()
	if !strings.Contains(text, "kova-worker") {
		t.Fatalf("expected worker pod in list output:\n%s", text)
	}
	if strings.Contains(text, "otel-collector") {
		t.Fatalf("did not expect otel pod in list output:\n%s", text)
	}
}
