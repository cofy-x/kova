package kube

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
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

func TestDeletePodWithUIDRejectsEmptyUIDWithoutRequest(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	kube := &Client{clientset: clientset}
	if err := kube.DeletePodWithUID(context.Background(), "jobs", "runner", ""); err == nil {
		t.Fatal("expected empty UID to fail closed")
	}
	if got := len(clientset.Actions()); got != 0 {
		t.Fatalf("Kubernetes requests after empty UID = %d", got)
	}
}

func TestDeletePodWithUIDUsesPreconditionAndHandlesNotFound(t *testing.T) {
	uid := types.UID("original")
	for _, present := range []bool{true, false} {
		t.Run(map[bool]string{true: "present", false: "already-gone"}[present], func(t *testing.T) {
			clientset := fake.NewSimpleClientset()
			if present {
				if err := clientset.Tracker().Create(corev1.SchemeGroupVersion.WithResource("pods"),
					&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: "jobs", UID: uid}}, "jobs"); err != nil {
					t.Fatal(err)
				}
			}
			kube := &Client{clientset: clientset}
			if err := kube.DeletePodWithUID(context.Background(), "jobs", "runner", uid); err != nil {
				t.Fatal(err)
			}
			if len(clientset.Actions()) < 2 {
				t.Fatalf("expected DELETE and confirming GET, actions=%#v", clientset.Actions())
			}
			deleteAction, ok := clientset.Actions()[0].(ktesting.DeleteAction)
			if !ok {
				t.Fatalf("first action = %T, want DeleteAction", clientset.Actions()[0])
			}
			options := deleteAction.GetDeleteOptions()
			if options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != uid {
				t.Fatalf("DELETE preconditions = %#v, want UID %s", options.Preconditions, uid)
			}
		})
	}
}

func TestDeletePodWithUIDConflictsWhenNameReplacedBeforeDelete(t *testing.T) {
	uid := types.UID("original")
	replacementUID := types.UID("replacement")
	clientset := fake.NewSimpleClientset(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: "jobs", UID: uid}})
	clientset.PrependReactor("delete", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		options := action.(ktesting.DeleteAction).GetDeleteOptions()
		if options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != uid {
			t.Fatalf("DELETE was not pinned to original UID: %#v", options.Preconditions)
		}
		if err := clientset.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("pods"), "jobs", "runner"); err != nil {
			t.Fatal(err)
		}
		if err := clientset.Tracker().Create(corev1.SchemeGroupVersion.WithResource("pods"),
			&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: "jobs", UID: replacementUID}}, "jobs"); err != nil {
			t.Fatal(err)
		}
		return true, nil, apierrors.NewConflict(corev1.Resource("pods"), "runner", errors.New("UID precondition failed"))
	})
	kube := &Client{clientset: clientset}
	if err := kube.DeletePodWithUID(context.Background(), "jobs", "runner", uid); !apierrors.IsConflict(err) {
		t.Fatalf("replacement before DELETE error = %v, want Conflict", err)
	}
	pod, err := clientset.CoreV1().Pods("jobs").Get(context.Background(), "runner", metav1.GetOptions{})
	if err != nil || pod.UID != replacementUID {
		t.Fatalf("replacement Pod lost: pod=%#v err=%v", pod, err)
	}
}

func TestDeletePodWithUIDConflictsWhenNameReplacedDuringWait(t *testing.T) {
	uid := types.UID("original")
	replacementUID := types.UID("replacement")
	clientset := fake.NewSimpleClientset(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: "jobs", UID: uid}})
	clientset.PrependReactor("delete", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		if err := clientset.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("pods"), "jobs", "runner"); err != nil {
			t.Fatal(err)
		}
		if err := clientset.Tracker().Create(corev1.SchemeGroupVersion.WithResource("pods"),
			&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: "jobs", UID: replacementUID}}, "jobs"); err != nil {
			t.Fatal(err)
		}
		return true, nil, nil
	})
	kube := &Client{clientset: clientset}
	if err := kube.DeletePodWithUID(context.Background(), "jobs", "runner", uid); !apierrors.IsConflict(err) {
		t.Fatalf("replacement while waiting error = %v, want Conflict", err)
	}
	pod, err := clientset.CoreV1().Pods("jobs").Get(context.Background(), "runner", metav1.GetOptions{})
	if err != nil || pod.UID != replacementUID {
		t.Fatalf("replacement Pod lost: pod=%#v err=%v", pod, err)
	}
}

func TestDeletePodWithUIDWaitsForOriginalUIDToDisappear(t *testing.T) {
	uid := types.UID("original")
	clientset := fake.NewSimpleClientset(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: "jobs", UID: uid}})
	gets := 0
	clientset.PrependReactor("get", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		gets++
		if gets == 1 {
			return true, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: "jobs", UID: uid}}, nil
		}
		return true, nil, apierrors.NewNotFound(corev1.Resource("pods"), "runner")
	})
	kube := &Client{clientset: clientset}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := kube.DeletePodWithUID(ctx, "jobs", "runner", uid); err != nil {
		t.Fatal(err)
	}
	if gets != 2 {
		t.Fatalf("confirmation GETs = %d, want retry then NotFound", gets)
	}
}

func TestDeletePodWithUIDRetriesAfterConfirmationReadError(t *testing.T) {
	uid := types.UID("original")
	clientset := fake.NewSimpleClientset(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: "jobs", UID: uid}})
	failOnce := true
	clientset.PrependReactor("get", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		if failOnce {
			failOnce = false
			return true, nil, errors.New("temporary API read failure")
		}
		return false, nil, nil
	})
	kube := &Client{clientset: clientset}
	if err := kube.DeletePodWithUID(context.Background(), "jobs", "runner", uid); err == nil {
		t.Fatal("confirmation read failure should cause a retry")
	}
	if err := kube.DeletePodWithUID(context.Background(), "jobs", "runner", uid); err != nil {
		t.Fatalf("retry after original Pod was deleted: %v", err)
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
