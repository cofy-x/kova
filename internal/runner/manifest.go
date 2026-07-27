package runner

import (
	"maps"
	"sort"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

type ManifestOptions struct {
	PodName         string
	Namespace       string
	Image           string
	ImagePullPolicy string
	ImagePullSecret string
	BuildkitAddr    string
	PprofServer     string
	Env             map[string]string
	Labels          map[string]string
	Annotations     map[string]string
	NodeSelector    map[string]string
	SourcePVCClaim  string
	SourceMountPath string
	SourceReadOnly  bool
}

func RenderPrepareManifest(opts ManifestOptions) (string, error) {
	pod := PreparePod(opts)
	raw, err := yaml.Marshal(pod)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func PreparePod(opts ManifestOptions) corev1.Pod {
	pod := corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      opts.PodName,
			Namespace: opts.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name": "kova-runner",
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{
				{
					Name:            "runner",
					Image:           opts.Image,
					ImagePullPolicy: corev1.PullPolicy(opts.ImagePullPolicy),
					Command: []string{
						"kovad",
						"daemon",
						"--addrs",
						opts.BuildkitAddr,
					},
				},
			},
		},
	}
	maps.Copy(pod.Labels, opts.Labels)
	if len(opts.NodeSelector) > 0 {
		pod.Spec.NodeSelector = maps.Clone(opts.NodeSelector)
	}

	if len(opts.Annotations) > 0 {
		pod.Annotations = map[string]string{}
		for key, value := range opts.Annotations {
			pod.Annotations[key] = value
		}
	}
	if opts.PprofServer != "" {
		pod.Spec.Containers[0].Env = append(pod.Spec.Containers[0].Env, corev1.EnvVar{Name: "KOVA_PPROF_SERVER", Value: opts.PprofServer})
	}
	for _, name := range sortedEnvNames(opts.Env) {
		pod.Spec.Containers[0].Env = append(pod.Spec.Containers[0].Env, corev1.EnvVar{Name: name, Value: opts.Env[name]})
	}
	if opts.SourcePVCClaim != "" && opts.SourceMountPath != "" {
		pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{
			Name:      "kova-source-store",
			MountPath: opts.SourceMountPath,
			ReadOnly:  opts.SourceReadOnly,
		})
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
			Name: "kova-source-store",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: opts.SourcePVCClaim,
					ReadOnly:  opts.SourceReadOnly,
				},
			},
		})
	}
	if opts.ImagePullSecret != "" {
		pod.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: opts.ImagePullSecret}}
		pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts,
			corev1.VolumeMount{Name: "docker-config", MountPath: "/root/.docker", ReadOnly: true},
		)
		pod.Spec.Volumes = append(pod.Spec.Volumes,
			corev1.Volume{
				Name: "docker-config",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName: opts.ImagePullSecret,
						Items: []corev1.KeyToPath{
							{Key: ".dockerconfigjson", Path: "config.json"},
						},
					},
				},
			},
		)
	}
	return pod
}

func sortedEnvNames(values map[string]string) []string {
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
