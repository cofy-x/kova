{{- define "kova.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Only explicit binary storage quantities are accepted for cache-budget checks. */}}
{{- define "kova.cacheStorageBytes" -}}
{{- $value := toString . -}}
{{- if regexMatch `^[1-9][0-9]*Mi$` $value -}}
{{- $number := trimSuffix "Mi" $value -}}
{{- if gt (len $number) 10 -}}{{- fail "worker cache storage quantity must not exceed 1Pi" -}}{{- end -}}
{{- $quantity := int64 $number -}}
{{- if gt $quantity 1073741824 -}}{{- fail "worker cache storage quantity must not exceed 1Pi" -}}{{- end -}}
{{- mul $quantity 1048576 -}}
{{- else if regexMatch `^[1-9][0-9]*Gi$` $value -}}
{{- $number := trimSuffix "Gi" $value -}}
{{- if gt (len $number) 7 -}}{{- fail "worker cache storage quantity must not exceed 1Pi" -}}{{- end -}}
{{- $quantity := int64 $number -}}
{{- if gt $quantity 1048576 -}}{{- fail "worker cache storage quantity must not exceed 1Pi" -}}{{- end -}}
{{- mul $quantity 1073741824 -}}
{{- else if regexMatch `^[1-9][0-9]*Ti$` $value -}}
{{- $number := trimSuffix "Ti" $value -}}
{{- if gt (len $number) 4 -}}{{- fail "worker cache storage quantity must not exceed 1Pi" -}}{{- end -}}
{{- $quantity := int64 $number -}}
{{- if gt $quantity 1024 -}}{{- fail "worker cache storage quantity must not exceed 1Pi" -}}{{- end -}}
{{- mul $quantity 1099511627776 -}}
{{- else -}}
{{- fail (printf "worker cache storage quantity %q must use an integer Mi, Gi, or Ti value" $value) -}}
{{- end -}}
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

{{- define "kova.roleImage" -}}
{{- $image := required (printf "Values.images.%s is required" .role) (index .root.Values.images .role) -}}
{{- $repository := required (printf "Values.images.%s.repository is required" .role) $image.repository -}}
{{- $defaultTag := printf "%s-%s" .role .root.Chart.AppVersion -}}
{{- $tag := default $defaultTag $image.tag -}}
{{- if $image.digest -}}
{{- printf "%s@%s" $repository $image.digest -}}
{{- else -}}
{{- printf "%s:%s" $repository $tag -}}
{{- end -}}
{{- end -}}

{{- define "kova.roleImagePullPolicy" -}}
{{- $image := required (printf "Values.images.%s is required" .role) (index .root.Values.images .role) -}}
{{- default "IfNotPresent" $image.pullPolicy -}}
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
  value: {{ .Values.observability.controllerServiceName | quote }}
- name: KOVA_RUNNER_OTEL_SERVICE_NAME
  value: {{ .Values.observability.runnerServiceName | quote }}
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
