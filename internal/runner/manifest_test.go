package runner

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"
)

func TestRenderPrepareManifestWithoutSecret(t *testing.T) {
	manifest, err := RenderPrepareManifest(ManifestOptions{
		PodName:         "e2e",
		Namespace:       "default",
		Image:           "localhost:5001/kova:dev",
		ImagePullPolicy: "IfNotPresent",
		BuildkitAddr:    "tcp://buildkit.example.svc:9094",
	})
	if err != nil {
		t.Fatal(err)
	}
	pod := decodePodManifest(t, manifest)
	assertBasePreparePod(t, pod)

	mustContain(t, manifest, "name: e2e")
	mustContain(t, manifest, "namespace: default")
	mustContain(t, manifest, "image: localhost:5001/kova:dev")
	mustContain(t, manifest, "imagePullPolicy: IfNotPresent")
	mustContain(t, manifest, "- kovad")
	mustContain(t, manifest, "- daemon")
	mustContain(t, manifest, "tcp://buildkit.example.svc:9094")
	mustNotContain(t, manifest, "imagePullSecrets:")
	mustNotContain(t, manifest, "volumeMounts:")
}

func TestRenderPrepareManifestWithSecretAndPprof(t *testing.T) {
	manifest, err := RenderPrepareManifest(ManifestOptions{
		PodName:         "runner",
		Namespace:       "jobs",
		Image:           "registry.local/kova:dev",
		ImagePullPolicy: "Always",
		ImagePullSecret: "registry-secret",
		BuildkitAddr:    defaultBuildkitServiceAddr,
		PprofServer:     "0.0.0.0:5241",
	})
	if err != nil {
		t.Fatal(err)
	}
	pod := decodePodManifest(t, manifest)
	assertBasePreparePod(t, pod)

	mustContain(t, manifest, "imagePullSecrets:")
	mustContain(t, manifest, "- name: registry-secret")
	mustContain(t, manifest, "mountPath: /home/kova/.docker")
	mustContain(t, manifest, "secretName: registry-secret")
	mustContain(t, manifest, "KOVA_PPROF_SERVER")
	if len(pod.Spec.Containers[0].Env) != 1 || pod.Spec.Containers[0].Env[0].Value != "0.0.0.0:5241" {
		t.Fatalf("unexpected pprof env: %#v", pod.Spec.Containers[0].Env)
	}
	if got := pod.Spec.ImagePullSecrets; len(got) != 1 || got[0].Name != "registry-secret" {
		t.Fatalf("unexpected imagePullSecrets: %#v", got)
	}
	if got := pod.Spec.Volumes; len(got) != 1 || got[0].Secret == nil || got[0].Secret.SecretName != "registry-secret" {
		t.Fatalf("unexpected docker config volume: %#v", got)
	}
}

func TestPreparePodUsesKubernetesTypes(t *testing.T) {
	pod := PreparePod(ManifestOptions{
		PodName:         "typed",
		Namespace:       "jobs",
		Image:           "registry.local/kova:dev",
		ImagePullPolicy: string(corev1.PullIfNotPresent),
		BuildkitAddr:    "tcp://buildkit.example.svc:9094",
	})

	assertBasePreparePod(t, pod)
	if pod.Name != "typed" || pod.Namespace != "jobs" {
		t.Fatalf("unexpected pod identity: %s/%s", pod.Namespace, pod.Name)
	}
	if got := pod.Spec.Containers[0].ImagePullPolicy; got != corev1.PullIfNotPresent {
		t.Fatalf("imagePullPolicy = %q", got)
	}
}

func TestPreparePodCopiesNodeSelector(t *testing.T) {
	selector := map[string]string{"kova.cofy.io/source-node": "true"}
	pod := PreparePod(ManifestOptions{
		PodName: "scheduled", Namespace: "jobs", Image: "registry.local/kova:dev",
		NodeSelector: selector,
	})
	selector["kova.cofy.io/source-node"] = "mutated"
	if got := pod.Spec.NodeSelector["kova.cofy.io/source-node"]; got != "true" {
		t.Fatalf("node selector = %q, want true", got)
	}
}

func TestRenderPrepareManifestWithObservabilityEnv(t *testing.T) {
	manifest, err := RenderPrepareManifest(ManifestOptions{
		PodName:         "runner",
		Namespace:       "jobs",
		Image:           "registry.local/kova:dev",
		ImagePullPolicy: string(corev1.PullIfNotPresent),
		BuildkitAddr:    "tcp://buildkit.example.svc:9094",
		Env: map[string]string{
			"KOVA_OTEL_ENABLED":           "true",
			"OTEL_EXPORTER_OTLP_ENDPOINT": "otel-collector.kova.svc:4317",
			"OTEL_SERVICE_NAME":           "kova-runner",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	pod := decodePodManifest(t, manifest)
	env := pod.Spec.Containers[0].Env
	if len(env) != 3 {
		t.Fatalf("env len = %d, want 3: %#v", len(env), env)
	}
	mustContain(t, manifest, "KOVA_OTEL_ENABLED")
	mustContain(t, manifest, "OTEL_EXPORTER_OTLP_ENDPOINT")
	mustContain(t, manifest, "kova-runner")
}

func mustContain(t *testing.T, value, want string) {
	t.Helper()
	if !strings.Contains(value, want) {
		t.Fatalf("expected manifest to contain %q\n%s", want, value)
	}
}

func decodePodManifest(t *testing.T, manifest string) corev1.Pod {
	t.Helper()
	var pod corev1.Pod
	if err := yaml.Unmarshal([]byte(manifest), &pod); err != nil {
		t.Fatalf("manifest is not valid yaml: %v\n%s", err, manifest)
	}
	return pod
}

func assertBasePreparePod(t *testing.T, pod corev1.Pod) {
	t.Helper()
	if pod.APIVersion != "v1" || pod.Kind != "Pod" {
		t.Fatalf("unexpected manifest header: %#v", pod)
	}
	if len(pod.Spec.Containers) != 1 {
		t.Fatalf("expected one container, got %#v", pod.Spec.Containers)
	}
	if pod.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Fatalf("restartPolicy = %q", pod.Spec.RestartPolicy)
	}
	if got := pod.Spec.Containers[0].Name; got != "runner" {
		t.Fatalf("container name = %q", got)
	}
	wantCommand := []string{"kovad", "daemon", "--addrs"}
	if got := pod.Spec.Containers[0].Command; len(got) != 4 || got[0] != wantCommand[0] || got[1] != wantCommand[1] || got[2] != wantCommand[2] {
		t.Fatalf("unexpected command: %#v", got)
	}
	probe := pod.Spec.Containers[0].ReadinessProbe
	if probe == nil || probe.Exec == nil || len(probe.Exec.Command) != 3 || probe.Exec.Command[2] != "/tmp/kova.sock" {
		t.Fatalf("unexpected readiness probe: %#v", probe)
	}
}

func mustNotContain(t *testing.T, value, unwanted string) {
	t.Helper()
	if strings.Contains(value, unwanted) {
		t.Fatalf("expected manifest not to contain %q\n%s", unwanted, value)
	}
}
