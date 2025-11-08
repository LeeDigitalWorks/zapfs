{{/*
Expand the name of the chart.
*/}}
{{- define "zapfs.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "zapfs.fullname" -}}
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
{{- define "zapfs.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "zapfs.labels" -}}
helm.sh/chart: {{ include "zapfs.chart" . }}
{{ include "zapfs.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "zapfs.selectorLabels" -}}
app.kubernetes.io/name: {{ include "zapfs.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "zapfs.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "zapfs.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Manager headless service name for Raft peer discovery
*/}}
{{- define "zapfs.manager.headlessService" -}}
{{- include "zapfs.fullname" . }}-manager-headless
{{- end }}

{{/*
Manager service name
*/}}
{{- define "zapfs.manager.service" -}}
{{- include "zapfs.fullname" . }}-manager
{{- end }}

{{/*
Metadata service name
*/}}
{{- define "zapfs.metadata.service" -}}
{{- include "zapfs.fullname" . }}-metadata
{{- end }}

{{/*
File server service name
*/}}
{{- define "zapfs.file.service" -}}
{{- include "zapfs.fullname" . }}-file
{{- end }}

{{/*
Generate manager peer list for Raft bootstrap
Format: manager-0.manager-headless:8051,manager-1.manager-headless:8051,...
*/}}
{{- define "zapfs.manager.raftPeers" -}}
{{- $fullname := include "zapfs.fullname" . -}}
{{- $headless := include "zapfs.manager.headlessService" . -}}
{{- $raftPort := .Values.manager.raftPort | default 8051 -}}
{{- $replicas := .Values.manager.replicas | int -}}
{{- $peers := list -}}
{{- range $i := until $replicas -}}
{{- $peers = append $peers (printf "%s-manager-%d.%s:%d" $fullname $i $headless ($raftPort | int)) -}}
{{- end -}}
{{- join "," $peers -}}
{{- end }}

