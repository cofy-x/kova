{{- define "kova.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "kova.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- include "kova.name" . -}}
{{- end -}}
{{- end -}}

{{- define "kova.namespace" -}}
{{- default .Release.Namespace .Values.namespaceOverride -}}
{{- end -}}

{{- define "kova.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" -}}
{{- end -}}

{{- define "kova.selectorLabels" -}}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/name: {{ include "kova.name" . }}
{{- end -}}

{{- define "kova.labels" -}}
helm.sh/chart: {{ include "kova.chart" . }}
app.kubernetes.io/name: {{ include "kova.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "kova.registryConfigJson" -}}
{{- $registries := .Values.imageRegistries | default list -}}
{{- $auths := dict -}}
{{- range $index, $entry := $registries -}}
{{- if not $entry.name -}}
{{- fail (printf "Values.imageRegistries[%d].name is required" $index) -}}
{{- end -}}
{{- if not $entry.auth -}}
{{- fail (printf "Values.imageRegistries[%d].auth is required" $index) -}}
{{- end -}}
{{- $_ := set $auths $entry.name (dict "auth" $entry.auth) -}}
{{- end -}}
{{- dict "auths" $auths | toJson -}}
{{- end -}}

{{- define "kova.registryConfigJsonB64" -}}
{{- include "kova.registryConfigJson" . | b64enc -}}
{{- end -}}

{{- define "kova.image" -}}
{{- if .Values.imageOverride -}}
{{- .Values.imageOverride -}}
{{- else -}}
{{- if kindIs "string" .Values.image -}}
{{- required "Values.image is required" .Values.image -}}
{{- else -}}
{{- $imageRepository := required "Values.image.repository is required" .Values.image.repository -}}
{{- $imageTag := required "Values.image.tag is required" .Values.image.tag -}}
{{- printf "%s:%s" $imageRepository $imageTag -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "kova.imagePullPolicy" -}}
{{- if kindIs "string" .Values.image -}}
{{- default "IfNotPresent" .Values.imagePullPolicy -}}
{{- else -}}
{{- default "IfNotPresent" .Values.image.pullPolicy -}}
{{- end -}}
{{- end -}}

{{- define "kova.imagePullSecretName" -}}
{{- if kindIs "map" .Values.imagePullSecrets -}}
{{- .Values.imagePullSecrets.name | default "" -}}
{{- end -}}
{{- end -}}

{{- define "kova.imagePullSecretsEnabled" -}}
{{- if kindIs "map" .Values.imagePullSecrets -}}
{{- $secretName := include "kova.imagePullSecretName" . -}}
{{- ternary "true" "false" (or (.Values.imagePullSecrets.create | default false) (ne $secretName "")) -}}
{{- else -}}
{{- ternary "true" "false" (gt (len (.Values.imagePullSecrets | default list)) 0) -}}
{{- end -}}
{{- end -}}

{{- define "kova.renderImagePullSecrets" -}}
{{- if kindIs "slice" .Values.imagePullSecrets -}}
{{- toYaml .Values.imagePullSecrets -}}
{{- else if kindIs "map" .Values.imagePullSecrets -}}
{{- $secretName := include "kova.imagePullSecretName" . -}}
{{- if $secretName -}}
- name: {{ $secretName }}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "kova.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "kova.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "kova.sourcePVCName" -}}
{{- if .Values.sourceStore.pvc.existingClaim -}}
{{- .Values.sourceStore.pvc.existingClaim -}}
{{- else if .Values.sourceStore.pvc.name -}}
{{- .Values.sourceStore.pvc.name -}}
{{- else -}}
{{- printf "%s-sources" (include "kova.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "kova.observabilityEnv" -}}
- name: KOVA_OTEL_ENABLED
  value: {{ .Values.observability.enabled | quote }}
- name: KOVA_OTEL_TRACES_ENABLED
  value: {{ .Values.observability.traces.enabled | quote }}
- name: KOVA_OTEL_METRICS_ENABLED
  value: {{ .Values.observability.metrics.enabled | quote }}
- name: KOVA_OTEL_LOGS_ENABLED
  value: {{ .Values.observability.logs.enabled | quote }}
- name: KOVA_OTEL_METRIC_INTERVAL
  value: {{ .Values.observability.metricInterval | quote }}
- name: OTEL_SERVICE_NAME
  value: {{ .Values.observability.serviceName | quote }}
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: {{ .Values.observability.endpoint | quote }}
- name: OTEL_EXPORTER_OTLP_INSECURE
  value: {{ .Values.observability.insecure | quote }}
{{- $attrs := list -}}
{{- range $key, $value := .Values.observability.resourceAttributes }}
{{- $attrs = append $attrs (printf "%s=%v" $key $value) -}}
{{- end }}
{{- if $attrs }}
- name: OTEL_RESOURCE_ATTRIBUTES
  value: {{ join "," $attrs | quote }}
{{- end }}
{{- end -}}
