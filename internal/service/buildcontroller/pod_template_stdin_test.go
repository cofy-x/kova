package buildcontroller

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/cofy-x/kova/internal/service/recoveryreceipt"
	"github.com/cofy-x/kova/internal/version"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kjson "sigs.k8s.io/json"
)

const podTemplateStdinMaximum = 128 << 10

// This offline test-binary entrypoint is never a live/API gate. It reuses the
// product's typed Go projection instead of trusting a Pod annotation or
// duplicating corev1.PodSpec's encoding rules in the Python harness.
func podTemplateStdinDigest(raw []byte, commit string) ([]byte, error) {
	var pod corev1.Pod
	strict, err := kjson.UnmarshalStrict(raw, &pod, kjson.DisallowDuplicateFields, kjson.DisallowUnknownFields)
	if len(raw) == 0 || len(raw) > podTemplateStdinMaximum || err != nil || len(strict) != 0 ||
		!regexp.MustCompile(`^[0-9a-f]{40}$`).MatchString(commit) || pod.APIVersion != "v1" || pod.Kind != "Pod" ||
		!regexp.MustCompile(`^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$`).MatchString(string(pod.UID)) ||
		pod.DeletionTimestamp != nil || len(pod.OwnerReferences) != 1 ||
		pod.OwnerReferences[0].APIVersion != "kova.cofy.dev/v1alpha1" || pod.OwnerReferences[0].Kind != "KovaBuild" ||
		pod.OwnerReferences[0].Controller == nil || !*pod.OwnerReferences[0].Controller {
		return nil, errors.New("POD_TEMPLATE_INPUT_INVALID")
	}
	digest, err := recoveryreceipt.CanonicalPodTemplateDigest(&pod)
	if err != nil {
		return nil, errors.New("POD_TEMPLATE_DIGEST_UNAVAILABLE")
	}
	inputSHA := sha256.Sum256(raw)
	return json.Marshal(struct {
		CandidateCommit   string `json:"candidate_commit"`
		InputSHA256       string `json:"input_sha256"`
		PodUID            string `json:"pod_uid"`
		PodTemplateDigest string `json:"pod_template_digest"`
	}{commit, hex.EncodeToString(inputSHA[:]), string(pod.UID), digest})
}

func TestE2EPodTemplateDigestStdin(t *testing.T) {
	mode := os.Getenv("KOVA_E2E_POD_TEMPLATE_STDIN")
	if mode != "1" && mode != "identity" {
		t.Skip("offline stdin digest entrypoint is explicitly opt-in")
	}
	if !regexp.MustCompile(`^[0-9a-f]{40}$`).MatchString(version.Commit) {
		t.Fatal("POD_TEMPLATE_CANDIDATE_UNPINNED")
	}
	if mode == "identity" {
		result, err := json.Marshal(struct {
			CandidateCommit string `json:"candidate_commit"`
			OS              string `json:"os"`
			Architecture    string `json:"architecture"`
		}{version.Commit, runtime.GOOS, runtime.GOARCH})
		if err != nil {
			t.Fatal("POD_TEMPLATE_IDENTITY_UNAVAILABLE")
		}
		fmt.Printf("KOVA_E2E_POD_TEMPLATE_IDENTITY %s\n", result)
		return
	}
	raw, err := io.ReadAll(io.LimitReader(os.Stdin, podTemplateStdinMaximum+1))
	if err != nil {
		t.Fatal("POD_TEMPLATE_INPUT_UNAVAILABLE")
	}
	result, err := podTemplateStdinDigest(raw, version.Commit)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("KOVA_E2E_POD_TEMPLATE %s\n", result)
}

func TestPodTemplateStdinDigestUsesProductProjection(t *testing.T) {
	yes := true
	pod := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "kova-job-build", Namespace: "runner", UID: types.UID("00000000-0000-4000-8000-000000000001"),
			Labels:          map[string]string{"test": "original"},
			Annotations:     map[string]string{"kova.cofy.dev/create-attempt": strings.Repeat("a", 32)},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "kova.cofy.dev/v1alpha1", Kind: "KovaBuild", Name: "build", UID: "build-original", Controller: &yes}},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "runner", Image: "example.invalid/runner@sha256:" + strings.Repeat("a", 64)}}, RestartPolicy: corev1.RestartPolicyNever},
	}
	want, err := recoveryreceipt.CanonicalPodTemplateDigest(pod)
	if err != nil {
		t.Fatal(err)
	}
	pod.Annotations[recoveryreceipt.PodTemplateDigestAnnotation] = want
	commit := strings.Repeat("b", 40)
	check := func(value *corev1.Pod) string {
		t.Helper()
		raw, marshalErr := json.Marshal(value)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		result, digestErr := podTemplateStdinDigest(raw, commit)
		if digestErr != nil {
			t.Fatal(digestErr)
		}
		var got map[string]string
		if json.Unmarshal(result, &got) != nil || len(got) != 4 || got["candidate_commit"] != commit || got["pod_uid"] != string(pod.UID) {
			t.Fatal("native response did not preserve exact original input identity")
		}
		sum := sha256.Sum256(raw)
		if got["input_sha256"] != hex.EncodeToString(sum[:]) {
			t.Fatal("native response did not bind exact input bytes")
		}
		return got["pod_template_digest"]
	}
	if check(pod) != want {
		t.Fatal("native stdin helper differs from product projection")
	}
	defaulted := pod.DeepCopy()
	defaulted.Spec.DNSPolicy = corev1.DNSClusterFirst
	defaulted.Spec.ServiceAccountName = "default"
	defaulted.Spec.SchedulerName = "default-scheduler"
	if check(defaulted) != want {
		t.Fatal("known Kubernetes defaults changed native projection")
	}
	for _, mutate := range []func(*corev1.Pod){
		func(p *corev1.Pod) { p.Labels["injected"] = "true" },
		func(p *corev1.Pod) { p.Spec.HostNetwork = true },
		func(p *corev1.Pod) {
			p.Spec.Containers = append(p.Spec.Containers, corev1.Container{Name: "injected", Image: "foreign"})
		},
	} {
		changed := pod.DeepCopy()
		mutate(changed)
		if check(changed) == want {
			t.Fatal("changed Pod label/spec falsely retained original stamped digest")
		}
	}
	raw, _ := json.Marshal(pod)
	for _, invalid := range [][]byte{
		nil, []byte(`null`), append(raw, []byte(` {}`)...),
		[]byte(strings.Replace(string(raw), `"spec":{`, `"spec":{"unknown":true,`, 1)),
		[]byte(strings.Replace(string(raw), `"name":"runner"`, `"name":"runner","name":"duplicate"`, 1)),
		[]byte(strings.Replace(string(raw), `"containers":`, `"Containers":`, 1)),
		[]byte(strings.Repeat(" ", podTemplateStdinMaximum+1)),
	} {
		if result, digestErr := podTemplateStdinDigest(invalid, commit); digestErr == nil || result != nil {
			t.Fatal("malformed, duplicate, foreign or unbounded input produced a digest")
		}
	}
	if result, digestErr := podTemplateStdinDigest(raw, commit[:12]); digestErr == nil || result != nil {
		t.Fatal("short source alias produced a digest")
	}
}
