{{/* Chart name, truncated to the 63-character DNS label limit. */}}
{{- define "ecom-api.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Fully qualified release name, used as the prefix for every resource. */}}
{{- define "ecom-api.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "ecom-api.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "ecom-api.labels" -}}
helm.sh/chart: {{ include "ecom-api.chart" . }}
app.kubernetes.io/part-of: {{ include "ecom-api.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/* Selector labels for a component; call with (dict "ctx" $ "component" "api"). */}}
{{- define "ecom-api.selectorLabels" -}}
app.kubernetes.io/name: {{ include "ecom-api.name" .ctx }}
app.kubernetes.io/instance: {{ .ctx.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{- define "ecom-api.api.fullname" -}}
{{- include "ecom-api.fullname" . }}
{{- end }}

{{- define "ecom-api.redis.fullname" -}}
{{- printf "%s-redis" (include "ecom-api.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "ecom-api.kafka.clusterName" -}}
{{- default (printf "%s-kafka" (include "ecom-api.fullname" .)) .Values.kafka.clusterName | trunc 40 | trimSuffix "-" }}
{{- end }}

{{/* Kafka bootstrap servers: the Strimzi plain listener, or the external cluster. Empty means no Kafka. */}}
{{- define "ecom-api.kafka.bootstrapServers" -}}
{{- if .Values.kafka.enabled }}
{{- printf "%s-kafka-bootstrap:9092" (include "ecom-api.kafka.clusterName" .) }}
{{- else }}
{{- .Values.kafka.externalBootstrapServers }}
{{- end }}
{{- end }}

{{/* Redis connection string: the bundled instance, or the external one. Empty means no Redis. */}}
{{- define "ecom-api.redis.connection" -}}
{{- if .Values.redis.enabled }}
{{- printf "%s:6379" (include "ecom-api.redis.fullname" .) }}
{{- else }}
{{- .Values.redis.externalConnection }}
{{- end }}
{{- end }}

{{- define "ecom-api.api.image" -}}
{{- printf "%s:%s" .Values.api.image.repository (default .Chart.AppVersion .Values.api.image.tag) }}
{{- end }}
