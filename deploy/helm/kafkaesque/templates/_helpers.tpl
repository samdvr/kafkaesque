{{/*
Expand the name of the chart.
*/}}
{{- define "kafkaesque.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "kafkaesque.fullname" -}}
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

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "kafkaesque.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "kafkaesque.labels" -}}
helm.sh/chart: {{ include "kafkaesque.chart" . }}
{{ include "kafkaesque.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "kafkaesque.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kafkaesque.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: broker
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "kafkaesque.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "kafkaesque.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Generate Raft peers string
*/}}
{{- define "kafkaesque.raftPeers" -}}
{{- $fullname := include "kafkaesque.fullname" . -}}
{{- $namespace := .Release.Namespace -}}
{{- $raftPort := .Values.config.raftPort -}}
{{- $replicas := int .Values.replicaCount -}}
{{- $peers := list -}}
{{- range $i := until $replicas }}
{{- $peers = append $peers (printf "%d=%s-%d.%s-headless.%s.svc.cluster.local:%d" $i $fullname $i $fullname $namespace $raftPort) -}}
{{- end }}
{{- join "," $peers }}
{{- end }}
