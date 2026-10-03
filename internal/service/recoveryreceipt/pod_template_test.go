package recoveryreceipt

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func canonicalPodFixture() *corev1.Pod {
	yes := true
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "jobs", Name: "kova-job-build", UID: "pod-original", ResourceVersion: "7",
		Labels:      map[string]string{"kova.cofy.dev/build-id": "build"},
		Annotations: map[string]string{"kova.cofy.dev/create-attempt": strings.Repeat("a", 32)},
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "kova.cofy.dev/v1alpha1", Kind: "KovaBuild",
			Name: "build", UID: types.UID("build-original"), Controller: &yes}},
	}, Spec: corev1.PodSpec{
		RestartPolicy: corev1.RestartPolicyNever,
		Containers: []corev1.Container{{Name: "runner", Image: "example.com/kova/runner@sha256:" + strings.Repeat("b", 64),
			ImagePullPolicy: corev1.PullIfNotPresent, Command: []string{"kovad", "daemon"},
			ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"test", "-S", "/tmp/kova.sock"}}}},
			VolumeMounts:   []corev1.VolumeMount{{Name: "docker-config", MountPath: "/home/kova/.docker", ReadOnly: true}}}},
		InitContainers: []corev1.Container{{Name: "source-fetch", Image: "example.com/kova/runner@sha256:" + strings.Repeat("b", 64),
			Command:                  []string{"kovad", "source", "fetch", "--uri", "https://example.com/source.zip", "--digest", "sha256:" + strings.Repeat("c", 64)},
			TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError}},
		Volumes: []corev1.Volume{{Name: "docker-config", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "pull-secret"}}}},
	}}
}

func TestCanonicalPodTemplateNormalizesOnlyKnownAPIDefaults(t *testing.T) {
	proposed := canonicalPodFixture()
	want, err := CanonicalPodTemplateDigest(proposed)
	if err != nil {
		t.Fatal(err)
	}
	observed := proposed.DeepCopy()
	observed.Annotations[PodTemplateDigestAnnotation] = want
	observed.UID, observed.ResourceVersion = "server-assigned-uid", "99"
	observed.Spec.NodeName = "worker-1"
	observed.Spec.ServiceAccountName = "default"
	observed.Spec.DNSPolicy = corev1.DNSClusterFirst
	observed.Spec.SchedulerName = "default-scheduler"
	grace := int64(corev1.DefaultTerminationGracePeriodSeconds)
	observed.Spec.TerminationGracePeriodSeconds = &grace
	yes := true
	observed.Spec.EnableServiceLinks, observed.Spec.HostUsers = &yes, &yes
	priority := int32(0)
	observed.Spec.Priority = &priority
	preempt := corev1.PreemptLowerPriority
	observed.Spec.PreemptionPolicy = &preempt
	seconds := int64(300)
	observed.Spec.Tolerations = []corev1.Toleration{
		{Key: "node.kubernetes.io/not-ready", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: &seconds},
		{Key: "node.kubernetes.io/unreachable", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: &seconds},
	}
	for i := range observed.Spec.Containers {
		observed.Spec.Containers[i].TerminationMessagePath = corev1.TerminationMessagePathDefault
		observed.Spec.Containers[i].TerminationMessagePolicy = corev1.TerminationMessageReadFile
		if observed.Spec.Containers[i].ReadinessProbe != nil {
			observed.Spec.Containers[i].ReadinessProbe.SuccessThreshold = 1
		}
	}
	observed.Spec.InitContainers[0].TerminationMessagePath = corev1.TerminationMessagePathDefault
	mode := int32(0644)
	observed.Spec.Volumes[0].Secret.DefaultMode = &mode
	got, err := CanonicalPodTemplateDigest(observed)
	if err != nil || got != want {
		t.Fatalf("documented API defaults changed template digest: got=%s want=%s err=%v", got, want, err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*corev1.Pod)
	}{
		{"inherited image pull secret", func(p *corev1.Pod) { p.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "injected"}} }},
		{"changed source URI", func(p *corev1.Pod) { p.Spec.InitContainers[0].Command[4] = "https://other.example/source.zip" }},
		{"changed image", func(p *corev1.Pod) { p.Spec.Containers[0].Image = "example.com/evil:latest" }},
		{"changed security", func(p *corev1.Pod) { p.Spec.HostNetwork = true }},
		{"extra volume", func(p *corev1.Pod) { p.Spec.Volumes = append(p.Spec.Volumes, corev1.Volume{Name: "extra"}) }},
		{"extra toleration", func(p *corev1.Pod) {
			p.Spec.Tolerations = append(p.Spec.Tolerations, corev1.Toleration{Key: "dedicated", Operator: corev1.TolerationOpExists})
		}},
		{"changed default toleration", func(p *corev1.Pod) { value := int64(600); p.Spec.Tolerations[0].TolerationSeconds = &value }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := observed.DeepCopy()
			tc.mutate(changed)
			digest, err := CanonicalPodTemplateDigest(changed)
			if err == nil && digest == want {
				t.Fatal("execution-relevant Pod mutation retained digest")
			}
		})
	}
}
