package runner

import (
	"maps"
	"net/url"
	"sort"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

type ManifestOptions struct {
	PodName              string
	Namespace            string
	Image                string
	ImagePullPolicy      string
	ImagePullSecret      string
	BuildkitAddr         string
	PprofServer          string
	Env                  map[string]string
	Labels               map[string]string
	Annotations          map[string]string
	NodeSelector         map[string]string
	SourcePVCClaim       string
	SourceMountPath      string
	SourceReadOnly       bool
	SourceURI            string
	SourceDigest         string
	ArtifactRoot         string
	ArtifactSecret       string
	S3Endpoint           string
	S3Bucket             string
	S3Region             string
	S3CredentialProvider string
	S3CredentialDir      string
	S3Secure             bool
}

const MaterializedSourcePath = "/var/lib/kova/source/source.zip"

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
					Command: []string{
						"kovad",
						"daemon",
						"--addrs",
						opts.BuildkitAddr,
					},
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
	if source, err := url.Parse(opts.SourceURI); err == nil && source.Scheme == "s3" {
		credentialProvider := opts.S3CredentialProvider
		if credentialProvider == "" {
			credentialProvider = "static"
		}
		credentialDir := opts.S3CredentialDir
		if credentialDir == "" {
			credentialDir = "/var/run/secrets/kova/s3"
		}
		mount := corev1.VolumeMount{Name: "kova-source", MountPath: "/var/lib/kova/source"}
		pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, mount)
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
			Name:         "kova-source",
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		})
		fetch := corev1.Container{
			Name:            "source-fetch",
			Image:           opts.Image,
			ImagePullPolicy: corev1.PullPolicy(opts.ImagePullPolicy),
			Command: []string{
				"kovad", "artifact", "fetch",
				"--uri", opts.SourceURI,
				"--digest", opts.SourceDigest,
				"--output", MaterializedSourcePath,
				"--s3-endpoint", opts.S3Endpoint,
				"--s3-bucket", opts.S3Bucket,
				"--s3-region", opts.S3Region,
				"--s3-credential-provider", credentialProvider,
				"--s3-credential-dir", credentialDir,
				"--s3-secure=" + strconv.FormatBool(opts.S3Secure),
			},
			VolumeMounts: []corev1.VolumeMount{mount},
			SecurityContext: &corev1.SecurityContext{
				AllowPrivilegeEscalation: boolPointer(false),
				RunAsNonRoot:             boolPointer(true),
				RunAsUser:                int64Pointer(65532),
				RunAsGroup:               int64Pointer(65532),
				Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
				SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
		}
		if opts.ArtifactRoot != "" {
			fetch.Env = append(fetch.Env, corev1.EnvVar{Name: "KOVA_ARTIFACT_ROOT", Value: opts.ArtifactRoot})
		}
		if opts.ArtifactSecret != "" && credentialProvider == "static" {
			fetch.EnvFrom = []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: opts.ArtifactSecret},
			}}}
		} else if opts.ArtifactSecret != "" && credentialProvider == "file" {
			fetch.VolumeMounts = append(fetch.VolumeMounts, corev1.VolumeMount{
				Name: "s3-credentials", MountPath: credentialDir, ReadOnly: true,
			})
			pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
				Name: "s3-credentials",
				VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
					SecretName: opts.ArtifactSecret,
				}},
			})
		}
		pod.Spec.InitContainers = append(pod.Spec.InitContainers, fetch)
	}
	if opts.ImagePullSecret != "" {
		pod.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: opts.ImagePullSecret}}
		pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts,
			corev1.VolumeMount{Name: "docker-config", MountPath: "/home/kova/.docker", ReadOnly: true},
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
