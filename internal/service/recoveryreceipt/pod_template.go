package recoveryreceipt

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PodTemplateDigestAnnotation is a witness to the pre-Create Pod template,
// not its authority. Callers must recompute CanonicalPodTemplateDigest from a
// direct Pod GET before trusting that annotation or a PodCreate receipt.
const PodTemplateDigestAnnotation = "kova.cofy.dev/pod-template-digest"

const podTemplateProjectionVersion = "kova-pod-template-v1"

type podTemplateProjection struct {
	Version         string                  `json:"version"`
	Namespace       string                  `json:"namespace"`
	Name            string                  `json:"name"`
	Labels          map[string]string       `json:"labels"`
	Annotations     map[string]string       `json:"annotations"`
	OwnerReferences []metav1.OwnerReference `json:"ownerReferences"`
	Spec            corev1.PodSpec          `json:"spec"`
}

// CanonicalPodTemplateDigest binds the entire application-controlled Pod
// identity and Spec, including container images, source-fetch command,
// security context, mounts, volumes, resources, scheduling and networking.
// It excludes API-assigned object metadata/status, the digest's own annotation,
// and the scheduler-assigned nodeName. The limited normalizations below cover
// Kubernetes defaults on fields the Service may leave empty. An injected
// container, volume, mount, env, imagePullSecret, host access, or other Spec
// mutation changes the digest and fails closed.
func CanonicalPodTemplateDigest(pod *corev1.Pod) (string, error) {
	if pod == nil || pod.Namespace == "" || pod.Name == "" || len(pod.OwnerReferences) != 1 {
		return "", fmt.Errorf("Pod template has no exact name or sole controller owner")
	}
	copy := pod.DeepCopy()
	delete(copy.Annotations, PodTemplateDigestAnnotation)
	if len(copy.Annotations) == 0 {
		copy.Annotations = nil
	}
	if len(copy.Labels) == 0 {
		copy.Labels = nil
	}
	canonicalPodSpec(&copy.Spec)
	projection := podTemplateProjection{
		Version: podTemplateProjectionVersion, Namespace: copy.Namespace, Name: copy.Name,
		Labels: copy.Labels, Annotations: copy.Annotations,
		OwnerReferences: copy.OwnerReferences, Spec: copy.Spec,
	}
	raw, err := json.Marshal(projection)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func canonicalPodSpec(spec *corev1.PodSpec) {
	// This value is assigned by the scheduler after Create. Its selection is
	// constrained by the still-bound selector, affinity and tolerations.
	spec.NodeName = ""
	if spec.ServiceAccountName == "default" {
		spec.ServiceAccountName = ""
	}
	if spec.DeprecatedServiceAccount == "default" {
		spec.DeprecatedServiceAccount = ""
	}
	if spec.DNSPolicy == corev1.DNSClusterFirst {
		spec.DNSPolicy = ""
	}
	if spec.SchedulerName == "default-scheduler" {
		spec.SchedulerName = ""
	}
	if spec.TerminationGracePeriodSeconds != nil && *spec.TerminationGracePeriodSeconds == corev1.DefaultTerminationGracePeriodSeconds {
		spec.TerminationGracePeriodSeconds = nil
	}
	if spec.EnableServiceLinks != nil && *spec.EnableServiceLinks {
		spec.EnableServiceLinks = nil
	}
	if spec.HostUsers != nil && *spec.HostUsers {
		spec.HostUsers = nil
	}
	if spec.Priority != nil && *spec.Priority == 0 {
		spec.Priority = nil
	}
	if spec.PreemptionPolicy != nil && *spec.PreemptionPolicy == corev1.PreemptLowerPriority {
		spec.PreemptionPolicy = nil
	}
	// The DefaultTolerationSeconds admission plugin may append these two
	// standard NoExecute tolerations when they were absent at Create. Only
	// their exact 300-second defaults are normalized; additional or modified
	// tolerations remain bound by the receipt.
	kept := spec.Tolerations[:0]
	for _, toleration := range spec.Tolerations {
		if !standardNoExecuteToleration(toleration) {
			kept = append(kept, toleration)
		}
	}
	spec.Tolerations = kept
	if len(spec.Tolerations) == 0 {
		spec.Tolerations = nil
	}
	for i := range spec.Containers {
		canonicalContainer(&spec.Containers[i])
	}
	for i := range spec.InitContainers {
		canonicalContainer(&spec.InitContainers[i])
	}
	for i := range spec.Volumes {
		if secret := spec.Volumes[i].Secret; secret != nil && secret.DefaultMode != nil && *secret.DefaultMode == 0644 {
			secret.DefaultMode = nil
		}
		if configMap := spec.Volumes[i].ConfigMap; configMap != nil && configMap.DefaultMode != nil && *configMap.DefaultMode == 0644 {
			configMap.DefaultMode = nil
		}
	}
}

func standardNoExecuteToleration(value corev1.Toleration) bool {
	if value.Key != "node.kubernetes.io/not-ready" && value.Key != "node.kubernetes.io/unreachable" {
		return false
	}
	return value.Operator == corev1.TolerationOpExists && value.Effect == corev1.TaintEffectNoExecute &&
		value.Value == "" && value.TolerationSeconds != nil && *value.TolerationSeconds == 300
}

func canonicalContainer(container *corev1.Container) {
	if container.TerminationMessagePath == corev1.TerminationMessagePathDefault {
		container.TerminationMessagePath = ""
	}
	if container.TerminationMessagePolicy == corev1.TerminationMessageReadFile {
		container.TerminationMessagePolicy = ""
	}
	for _, probe := range []*corev1.Probe{container.LivenessProbe, container.ReadinessProbe, container.StartupProbe} {
		if probe != nil && probe.SuccessThreshold == 1 {
			probe.SuccessThreshold = 0
		}
	}
}
