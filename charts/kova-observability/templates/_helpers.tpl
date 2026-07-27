{{- define "kova-observability.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "kova-observability.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := include "kova-observability.name" . }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "kova-observability.componentName" -}}
{{- printf "%s-%s" (include "kova-observability.fullname" .root) .component | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "kova-observability.labels" -}}
app.kubernetes.io/name: {{ include "kova-observability.name" .root }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
{{- end }}

{{- define "kova-observability.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kova-observability.name" .root }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{- define "kova-observability.image" -}}
{{- printf "%s:%s" .repository .tag }}
{{- end }}

{{- define "kova-observability.podScheduling" -}}
{{- with .root.Values.imagePullSecrets }}
imagePullSecrets:
{{ toYaml . | indent 2 }}
{{- end }}
{{- with .root.Values.scheduling.nodeSelector }}
nodeSelector:
{{ toYaml . | indent 2 }}
{{- end }}
{{- with .root.Values.scheduling.tolerations }}
tolerations:
{{ toYaml . | indent 2 }}
{{- end }}
{{- with .root.Values.scheduling.affinity }}
affinity:
{{ toYaml . | indent 2 }}
{{- end }}
{{- if .root.Values.scheduling.topologySpread.enabled }}
topologySpreadConstraints:
  - maxSkew: {{ .root.Values.scheduling.topologySpread.maxSkew }}
    topologyKey: {{ .root.Values.scheduling.topologySpread.topologyKey }}
    whenUnsatisfiable: {{ .root.Values.scheduling.topologySpread.whenUnsatisfiable }}
    labelSelector:
      matchLabels:
        {{- include "kova-observability.selectorLabels" (dict "root" .root "component" .component) | nindent 8 }}
{{- end }}
{{- end }}
