package kube

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	realAPIRunAnnotation     = "kova.cofy.dev/real-api-run"
	realAPIAttemptAnnotation = "kova.cofy.dev/real-api-attempt"
	realAPIPodName           = "runner"
)

// TestDeletePodWithUIDRealAPI is deliberately opt-in and mutates only a new,
// explicitly named namespace in an identity-pinned Kind cluster. Run it with:
//
// KOVA_REAL_API_KUBECONFIG=/absolute/path/to/kubeconfig \
// KOVA_REAL_API_CONTEXT=kind-cluster-name \
// KOVA_REAL_API_KUBE_SYSTEM_UID=expected-kube-system-uid \
// KOVA_REAL_API_NAMESPACE=kova-rc-uid-unique-suffix \
// go test ./internal/kube -run '^TestDeletePodWithUIDRealAPI$' -count=1 -v
func TestDeletePodWithUIDRealAPI(t *testing.T) {
	kubeconfig := strings.TrimSpace(os.Getenv("KOVA_REAL_API_KUBECONFIG"))
	if kubeconfig == "" {
		t.Skip("set KOVA_REAL_API_KUBECONFIG to opt in to the real API-server test")
	}
	contextName := requiredRealAPIEnv(t, "KOVA_REAL_API_CONTEXT")
	expectedSystemUID := types.UID(requiredRealAPIEnv(t, "KOVA_REAL_API_KUBE_SYSTEM_UID"))
	namespaceName := requiredRealAPIEnv(t, "KOVA_REAL_API_NAMESPACE")
	if !filepath.IsAbs(kubeconfig) {
		t.Fatal("KOVA_REAL_API_KUBECONFIG must be an absolute path")
	}
	if info, err := os.Stat(kubeconfig); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("KOVA_REAL_API_KUBECONFIG must name an existing regular file: %v", err)
	}
	if !strings.HasPrefix(contextName, "kind-") {
		t.Fatalf("refusing non-Kind context %q", contextName)
	}
	if !strings.HasPrefix(namespaceName, "kova-rc-uid-") || len(validation.IsDNS1123Label(namespaceName)) != 0 {
		t.Fatalf("KOVA_REAL_API_NAMESPACE must be a DNS label beginning with kova-rc-uid-: %q", namespaceName)
	}
	config, err := clientcmd.LoadFromFile(kubeconfig)
	if err != nil {
		t.Fatalf("load explicit kubeconfig: %v", err)
	}
	if config.CurrentContext != contextName || config.Contexts[contextName] == nil || config.Contexts[contextName].Cluster != contextName {
		t.Fatalf("explicit kubeconfig does not select the expected Kind context %q", contextName)
	}
	kubeClient, err := NewClient(kubeconfig)
	if err != nil {
		t.Fatalf("create client from explicit kubeconfig: %v", err)
	}
	var api API = kubeClient
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	systemRV, err := verifyRealAPICluster(ctx, kubeClient, expectedSystemUID)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("verified context=%s kube-system UID=%s RV=%s", contextName, expectedSystemUID, systemRV)
	if _, err := kubeClient.clientset.CoreV1().Namespaces().Get(ctx, namespaceName, metav1.GetOptions{}); err == nil {
		t.Fatalf("namespace %s already exists; refusing to adopt it", namespaceName)
	} else if !apierrors.IsNotFound(err) {
		t.Fatalf("cannot prove namespace %s is absent: %v", namespaceName, err)
	}

	var runBytes [12]byte
	if _, err := rand.Read(runBytes[:]); err != nil {
		t.Fatalf("generate test ownership marker: %v", err)
	}
	runID := hex.EncodeToString(runBytes[:])
	if _, err := verifyRealAPICluster(ctx, kubeClient, expectedSystemUID); err != nil {
		t.Fatal(err)
	}
	namespace, err := kubeClient.clientset.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:        namespaceName,
			Annotations: map[string]string{realAPIRunAnnotation: runID},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("namespace Create outcome is not confirmed for %s (run %s): %v; leave any resulting namespace for inspection", namespaceName, runID, err)
	}
	if namespace.UID == "" || namespace.ResourceVersion == "" {
		t.Fatalf("namespace %s Create returned no UID/RV; leave it for inspection", namespaceName)
	}
	t.Logf("created namespace %s UID=%s RV=%s run=%s", namespace.Name, namespace.UID, namespace.ResourceVersion, runID)

	// A failed assertion preserves the namespace and any Pods as evidence.
	// Successful cleanup is allowed only after every object identity is known.
	completed := false
	t.Cleanup(func() {
		if !completed {
			t.Logf("preserving namespace %s UID=%s after failed/uncertain test", namespaceName, namespace.UID)
			return
		}
		cleanupRealAPINamespace(t, kubeClient, namespaceName, namespace.UID, runID, expectedSystemUID)
	})
	createPod := func(attempt string) *corev1.Pod {
		t.Helper()
		if _, err := verifyRealAPICluster(ctx, kubeClient, expectedSystemUID); err != nil {
			t.Fatal(err)
		}
		pod, err := kubeClient.clientset.CoreV1().Pods(namespaceName).Create(ctx, realAPIPendingPod(namespaceName, string(namespace.UID), runID, attempt), metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("Pod %s/%s %s Create outcome is not confirmed: %v; leave namespace for inspection", namespaceName, realAPIPodName, attempt, err)
		}
		if pod.UID == "" || pod.ResourceVersion == "" {
			t.Fatalf("Pod %s/%s %s Create returned no UID/RV; leave namespace for inspection", namespaceName, realAPIPodName, attempt)
		}
		t.Logf("created %s Pod %s/%s UID=%s RV=%s", attempt, namespaceName, pod.Name, pod.UID, pod.ResourceVersion)
		return pod
	}
	assertPodUID := func(want types.UID) {
		t.Helper()
		pod, err := kubeClient.clientset.CoreV1().Pods(namespaceName).Get(ctx, realAPIPodName, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("confirm Pod %s/%s UID %s: %v", namespaceName, realAPIPodName, want, err)
		}
		t.Logf("observed Pod %s/%s UID=%s RV=%s", namespaceName, pod.Name, pod.UID, pod.ResourceVersion)
		if pod.UID != want || pod.Annotations[realAPIRunAnnotation] != runID {
			t.Fatalf("Pod identity changed: UID=%s run=%q, want UID=%s run=%q", pod.UID, pod.Annotations[realAPIRunAnnotation], want, runID)
		}
	}
	deletePod := func(uid types.UID) error {
		t.Helper()
		if _, err := verifyRealAPICluster(ctx, kubeClient, expectedSystemUID); err != nil {
			t.Fatal(err)
		}
		return api.DeletePodWithUID(ctx, namespaceName, realAPIPodName, uid)
	}

	original := createPod("original")
	wrongUID := types.UID("00000000-0000-0000-0000-000000000000")
	if wrongUID == original.UID {
		wrongUID = "ffffffff-ffff-ffff-ffff-ffffffffffff"
	}
	if err := deletePod(wrongUID); !apierrors.IsConflict(err) {
		t.Fatalf("wrong UID deletion returned %v, want Conflict; leave namespace for inspection", err)
	}
	assertPodUID(original.UID)
	if err := deletePod(original.UID); err != nil {
		t.Fatalf("delete original UID %s: %v; leave namespace for inspection", original.UID, err)
	}

	replacement := createPod("replacement")
	if replacement.UID == original.UID {
		t.Fatalf("replacement reused original UID %s; leave namespace for inspection", original.UID)
	}
	if err := deletePod(original.UID); !apierrors.IsConflict(err) {
		t.Fatalf("stale UID deletion returned %v, want Conflict; leave replacement for inspection", err)
	}
	assertPodUID(replacement.UID)
	if err := deletePod(replacement.UID); err != nil {
		t.Fatalf("delete replacement UID %s: %v; leave namespace for inspection", replacement.UID, err)
	}
	if pod, err := kubeClient.clientset.CoreV1().Pods(namespaceName).Get(ctx, realAPIPodName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("replacement Pod not confirmed absent: pod=%v err=%v; leave namespace for inspection", pod, err)
	}
	completed = true
}

func requiredRealAPIEnv(t *testing.T, name string) string {
	t.Helper()
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		t.Fatalf("%s is required when KOVA_REAL_API_KUBECONFIG is set", name)
	}
	return value
}

func verifyRealAPICluster(ctx context.Context, kubeClient *Client, expectedUID types.UID) (string, error) {
	current, err := kubeClient.clientset.CoreV1().Namespaces().Get(ctx, "kube-system", metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("verify kube-system UID before mutation: %w", err)
	}
	if current.UID != expectedUID {
		return "", fmt.Errorf("refusing cluster with kube-system UID %s; expected %s", current.UID, expectedUID)
	}
	return current.ResourceVersion, nil
}

func realAPIPendingPod(namespace, namespaceUID, runID, attempt string) *corev1.Pod {
	automount := false
	grace := int64(0)
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      realAPIPodName,
			Annotations: map[string]string{
				realAPIRunAnnotation:     runID,
				realAPIAttemptAnnotation: attempt,
			},
		},
		Spec: corev1.PodSpec{
			AutomountServiceAccountToken:  &automount,
			TerminationGracePeriodSeconds: &grace,
			RestartPolicy:                 corev1.RestartPolicyNever,
			SchedulingGates:               []corev1.PodSchedulingGate{{Name: "kova.cofy.dev/real-api-test"}},
			NodeSelector:                  map[string]string{"kova.cofy.dev/never-schedule": namespaceUID},
			Containers: []corev1.Container{{
				Name:            "inert",
				Image:           "kova.invalid/uid-delete-never-run:integration",
				ImagePullPolicy: corev1.PullNever,
			}},
		},
	}
}

func cleanupRealAPINamespace(t *testing.T, kubeClient *Client, name string, uid types.UID, runID string, expectedSystemUID types.UID) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if _, err := verifyRealAPICluster(ctx, kubeClient, expectedSystemUID); err != nil {
		t.Errorf("leaving namespace %s UID=%s: %v", name, uid, err)
		return
	}
	namespace, err := kubeClient.clientset.CoreV1().Namespaces().Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Errorf("namespace %s UID=%s cleanup identity read failed: %v", name, uid, err)
		return
	}
	if namespace.UID != uid || namespace.Annotations[realAPIRunAnnotation] != runID {
		t.Errorf("leaving namespace %s: UID/run changed to %s/%q, expected %s/%q", name, namespace.UID, namespace.Annotations[realAPIRunAnnotation], uid, runID)
		return
	}
	pods, err := kubeClient.clientset.CoreV1().Pods(name).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Errorf("leaving namespace %s UID=%s: Pod list failed: %v", name, uid, err)
		return
	}
	if len(pods.Items) != 0 {
		t.Errorf("leaving namespace %s UID=%s: %d Pod(s) remain after exact-UID deletes", name, uid, len(pods.Items))
		return
	}
	if err := kubeClient.clientset.CoreV1().Namespaces().Delete(ctx, name, metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &uid},
	}); err != nil {
		t.Errorf("namespace %s UID=%s preconditioned delete failed: %v", name, uid, err)
		return
	}
	for {
		_, err := kubeClient.clientset.CoreV1().Namespaces().Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			t.Logf("deleted namespace %s UID=%s", name, uid)
			return
		}
		if err != nil {
			t.Errorf("namespace %s UID=%s deletion confirmation failed: %v", name, uid, err)
			return
		}
		select {
		case <-ctx.Done():
			t.Errorf("namespace %s UID=%s deletion not confirmed: %v", name, uid, ctx.Err())
			return
		case <-time.After(time.Second):
		}
	}
}
