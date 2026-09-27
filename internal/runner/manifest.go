package runner

import (
	"maps"
	"sort"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

type ManifestOptions struct {
	PodName               string
	Namespace             string
	Image                 string
	ImagePullPolicy       string
	ImagePullSecret       string
	BuildkitAddr          string
	PprofServer           string
	Env                   map[string]string
	Labels                map[string]string
	Annotations           map[string]string
	NodeSelector          map[string]string
	SourceURI             string
	SourceDigest          string
	RegistryPlainHTTP     []string
	RunnerResources       corev1.ResourceRequirements
	SourceFetchResources  corev1.ResourceRequirements
	SourceVolumeSizeLimit *resource.Quantity
}

const MaterializedSourcePath = "/var/lib/kova/source/source.zip"

func DefaultRunnerResources() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("100m"),
			corev1.ResourceMemory:           resource.MustParse("2Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("5Gi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("4"),
			corev1.ResourceMemory:           resource.MustParse("2Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("5Gi"),
		},
	}
}

func DefaultSourceFetchResources() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("50m"),
			corev1.ResourceMemory:           resource.MustParse("1Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("1Gi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("2"),
			corev1.ResourceMemory:           resource.MustParse("1Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("1Gi"),
		},
	}
}

func DefaultSourceVolumeSizeLimit() resource.Quantity {
	return resource.MustParse("4Gi")
}

func mergeResources(defaults, overrides corev1.ResourceRequirements) corev1.ResourceRequirements {
	for key, value := range overrides.Requests {
		defaults.Requests[key] = value
	}
	for key, value := range overrides.Limits {
		defaults.Limits[key] = value
	}
	return defaults
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
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot: boolPointer(true),
				RunAsUser:    int64Pointer(65532),
				RunAsGroup:   int64Pointer(65532),
				FSGroup:      int64Pointer(65532),
			},
			Containers: []corev1.Container{
				{
					Name:            "runner",
					Image:           opts.Image,
					ImagePullPolicy: corev1.PullPolicy(opts.ImagePullPolicy),
					Command:         []string{"kovad", "daemon"},
					Resources:       mergeResources(DefaultRunnerResources(), opts.RunnerResources),
					ReadinessProbe: &corev1.Probe{
						ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{
							Command: []string{"/usr/bin/test", "-S", "/tmp/kova.sock"},
						}},
						PeriodSeconds:    1,
						TimeoutSeconds:   1,
						FailureThreshold: 3,
					},
					SecurityContext: &corev1.SecurityContext{
						AllowPrivilegeEscalation: boolPointer(false),
						RunAsNonRoot:             boolPointer(true),
						RunAsUser:                int64Pointer(65532),
						RunAsGroup:               int64Pointer(65532),
						Capabilities: &corev1.Capabilities{
							Drop: []corev1.Capability{"ALL"},
						},
						SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
					},
				},
			},
		},
	}
	if opts.BuildkitAddr != "" {
		pod.Spec.Containers[0].Command = append(pod.Spec.Containers[0].Command, "--addrs", opts.BuildkitAddr)
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
	if opts.SourceURI != "" {
		mount := corev1.VolumeMount{Name: "kova-source", MountPath: "/var/lib/kova/source"}
		pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, mount,
			corev1.VolumeMount{Name: "kova-source", MountPath: "/tmp"})
		volumeLimit := DefaultSourceVolumeSizeLimit()
		if opts.SourceVolumeSizeLimit != nil {
			volumeLimit = opts.SourceVolumeSizeLimit.DeepCopy()
		}
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
			Name:         "kova-source",
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: &volumeLimit}},
		})
		fetch := corev1.Container{
			Name:                     "source-fetch",
			Image:                    opts.Image,
			ImagePullPolicy:          corev1.PullPolicy(opts.ImagePullPolicy),
			TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
			Command: []string{
				"kovad", "source", "fetch",
				"--uri", opts.SourceURI,
				"--digest", opts.SourceDigest,
				"--output", MaterializedSourcePath,
			},
			VolumeMounts: []corev1.VolumeMount{mount},
			Resources:    mergeResources(DefaultSourceFetchResources(), opts.SourceFetchResources),
			SecurityContext: &corev1.SecurityContext{
				AllowPrivilegeEscalation: boolPointer(false),
				RunAsNonRoot:             boolPointer(true),
				RunAsUser:                int64Pointer(65532),
				RunAsGroup:               int64Pointer(65532),
				Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
				SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
		}
		for _, host := range opts.RegistryPlainHTTP {
			fetch.Command = append(fetch.Command, "--registry-plain-http", host)
		}
		pod.Spec.InitContainers = append(pod.Spec.InitContainers, fetch)
	}
	if opts.ImagePullSecret != "" {
		pod.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: opts.ImagePullSecret}}
		pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts,
			corev1.VolumeMount{Name: "docker-config", MountPath: "/home/kova/.docker", ReadOnly: true},
		)
		for index := range pod.Spec.InitContainers {
			pod.Spec.InitContainers[index].VolumeMounts = append(pod.Spec.InitContainers[index].VolumeMounts,
				corev1.VolumeMount{Name: "docker-config", MountPath: "/home/kova/.docker", ReadOnly: true},
			)
			pod.Spec.InitContainers[index].Env = append(pod.Spec.InitContainers[index].Env, corev1.EnvVar{Name: "HOME", Value: "/home/kova"})
		}
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

func boolPointer(value bool) *bool {
	return &value
}

func int64Pointer(value int64) *int64 {
	return &value
}

func sortedEnvNames(values map[string]string) []string {
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
