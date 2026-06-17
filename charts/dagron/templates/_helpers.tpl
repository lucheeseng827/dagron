{{- define "dagron.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "dagron.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s" (include "dagron.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "dagron.labels" -}}
app.kubernetes.io/name: {{ include "dagron.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{/*
DATABASE_URL: the in-chart Postgres service when enabled, else the external URL.
Fails the render if neither is configured, so a misconfigured install errors at
`helm template` time rather than crash-looping in the cluster.
*/}}
{{- define "dagron.databaseUrl" -}}
{{- if .Values.postgres.enabled -}}
{{- /* urlquery-encode credentials so @ : etc. in them don't break URI parsing. */ -}}
postgres://{{ .Values.postgres.user | urlquery }}:{{ .Values.postgres.password | urlquery }}@{{ include "dagron.fullname" . }}-postgres.{{ .Release.Namespace }}.svc:5432/{{ .Values.postgres.database | urlquery }}
{{- else if .Values.externalDatabaseUrl -}}
{{ .Values.externalDatabaseUrl }}
{{- else -}}
{{- fail "Set postgres.enabled=true, provide externalDatabaseUrl, or set externalDatabaseSecret.name" -}}
{{- end -}}
{{- end -}}

{{/*
Whether the chart manages the DB Secret itself. False when the operator points
DATABASE_URL at a pre-existing Secret (externalDatabaseSecret.name) — that keeps
the connection string (and its password) out of values files and `helm history`.
*/}}
{{- define "dagron.manageDbSecret" -}}
{{- if and (not .Values.postgres.enabled) .Values.externalDatabaseSecret.name -}}
false
{{- else -}}
true
{{- end -}}
{{- end -}}

{{/*
Name of the Secret the engine reads DATABASE_URL from: the externally-created one
when configured, otherwise the chart-managed `<fullname>-db`.
*/}}
{{- define "dagron.dbSecretName" -}}
{{- if eq (include "dagron.manageDbSecret" .) "false" -}}
{{ .Values.externalDatabaseSecret.name }}
{{- else -}}
{{ include "dagron.fullname" . }}-db
{{- end -}}
{{- end -}}

{{/*
Key within the DB Secret that holds the connection string.
*/}}
{{- define "dagron.dbSecretKey" -}}
{{- if eq (include "dagron.manageDbSecret" .) "false" -}}
{{ .Values.externalDatabaseSecret.key | default "DATABASE_URL" }}
{{- else -}}
DATABASE_URL
{{- end -}}
{{- end -}}

{{/*
Whether the chart manages the UI Secret (DAGRON_JWT_SECRET + admin password).
False when dagronApi.existingSecret.name points at a pre-created Secret — keeps
the JWT signing key and admin password out of values files / helm history.
*/}}
{{- define "dagron.manageUiSecret" -}}
{{- if .Values.dagronApi.existingSecret.name -}}
false
{{- else -}}
true
{{- end -}}
{{- end -}}

{{/*
Name of the UI Secret dagron-api reads DAGRON_JWT_SECRET (+ admin password)
from: the external one when set, else the chart-managed `<fullname>-ui`.
*/}}
{{- define "dagron.uiSecretName" -}}
{{- if eq (include "dagron.manageUiSecret" .) "false" -}}
{{ .Values.dagronApi.existingSecret.name }}
{{- else -}}
{{ include "dagron.fullname" . }}-ui
{{- end -}}
{{- end -}}
