{{/*
dagron-ci, in either place this chart runs it: a container in the engine's pod
(engine.yaml) or a Deployment of its own (ci.yaml). What the two have in
common is here, so that they cannot come to differ.
*/}}

{{/*
Where dagron-ci runs: "engine" or "deployment" (values.yaml, ci.placement).
*/}}
{{- define "dagron.ci.placement" -}}
{{- $placement := .Values.ci.placement | default "engine" -}}
{{- if not (has $placement (list "engine" "deployment")) -}}
{{- fail (printf "ci.placement is %q: it is engine (a container in the engine's pod) or deployment (a Deployment of its own)" (toString $placement)) -}}
{{- end -}}
{{- $placement -}}
{{- end -}}

{{/*
"true" when dagron-ci is a container in the engine's pod, and nothing when it
is not, or is not turned on.
*/}}
{{- define "dagron.ci.inEnginePod" -}}
{{- if .Values.ci.enabled -}}
{{- if eq (include "dagron.ci.placement" .) "engine" -}}true{{- end -}}
{{- end -}}
{{- end -}}

{{/*
The Secrets dagron-ci reads, as a JSON list of {id, env, ref}. It reads each
secret from a file, never from its environment: each Secret that is named is
mounted, its one key as one file.
Usage: {{- $files := include "dagron.ci.files" . | fromJsonArray }}
*/}}
{{- define "dagron.ci.files" -}}
{{- $ci := .Values.ci -}}
{{- $files := list -}}
{{- if $ci.apiTokenSecret.name }}{{- $files = append $files (dict "id" "api-token" "env" "CI_API_TOKEN_FILE" "ref" $ci.apiTokenSecret) }}{{- end -}}
{{- if $ci.webhookSecret.name }}{{- $files = append $files (dict "id" "webhook-secret" "env" "CI_GITHUB_WEBHOOK_SECRET_FILE" "ref" $ci.webhookSecret) }}{{- end -}}
{{- if $ci.githubTokenSecret.name }}{{- $files = append $files (dict "id" "github-token" "env" "CI_GITHUB_TOKEN_FILE" "ref" $ci.githubTokenSecret) }}{{- end -}}
{{- if $ci.githubApp.keySecret.name }}{{- $files = append $files (dict "id" "github-app-key" "env" "CI_GITHUB_APP_KEY_FILE" "ref" $ci.githubApp.keySecret) }}{{- end -}}
{{- if $ci.checkoutTokenSecret.name }}{{- $files = append $files (dict "id" "checkout-secret" "env" "CI_CHECKOUT_TOKEN_SECRET_FILE" "ref" $ci.checkoutTokenSecret) }}{{- end -}}
{{- toJson $files -}}
{{- end -}}

{{/*
The fleet's label map (values.yaml, ci.fleet.labels) as the file dagron-ci
reads (CI_GITHUB_FLEET_FILE), and nothing where no label is named. It is
written as JSON, which the YAML dagron-ci reads holds: a whole number stays
one however large, where toYaml writes a value file's 1000000 as 1e+06, which
dagron-ci would not take for one.
*/}}
{{- define "dagron.ci.fleetFile" -}}
{{- with .Values.ci.fleet.labels -}}
{{- toPrettyJson (dict "labels" .) -}}
{{- end -}}
{{- end -}}

{{/*
What a pod that holds dagron-ci is annotated with, as items of its template's
`annotations`: the label map's checksum. dagron-ci reads the map once, as it
starts, so a map that changed has to start it again. Nothing without a map.
Usage: {{- with (include "dagron.ci.podAnnotations" . | trim) }} … {{- . | nindent 8 }}
*/}}
{{- define "dagron.ci.podAnnotations" -}}
{{- with (include "dagron.ci.fleetFile" .) }}
checksum/ci-fleet: {{ sha256sum . }}
{{- end }}
{{- end -}}

{{/*
dagron-ci's container, as one item of a pod's `containers`.
Usage: {{- include "dagron.ci.container" . | nindent 8 }}
*/}}
{{- define "dagron.ci.container" -}}
{{- $ci := .Values.ci -}}
{{- $files := include "dagron.ci.files" . | fromJsonArray -}}
{{- $fleet := include "dagron.ci.fleetFile" . -}}
{{- $dbSecret := include "dagron.dbSecretName" . -}}
{{- $dbKey := include "dagron.dbSecretKey" . -}}
{{- if $ci.databaseSecret.name -}}
{{- $dbSecret = $ci.databaseSecret.name -}}
{{- $dbKey = $ci.databaseSecret.key | default "CI_DATABASE_URL" -}}
{{- end -}}
- name: dagron-ci
  # No command: the image's entrypoint serves.
  image: {{ include "dagron.image" (dict "image" $ci.image "root" $) | quote }}
  imagePullPolicy: {{ $ci.imagePullPolicy }}
  securityContext:
    allowPrivilegeEscalation: false
    # It writes nothing to disk: its state is the database.
    readOnlyRootFilesystem: true
    capabilities:
      drop: ["ALL"]
  env:
    - name: CI_DATABASE_URL
      valueFrom:
        secretKeyRef:
          name: {{ $dbSecret }}
          key: {{ $dbKey }}
    - name: CI_DATABASE_SCHEMA
      value: {{ $ci.databaseSchema | quote }}
    - name: CI_LISTEN
      value: "0.0.0.0:{{ $ci.port }}"
    - name: RUST_LOG
      value: {{ $ci.rustLog | quote }}
    {{- if $ci.cells }}
    - name: CI_CELLS
      value: {{ $ci.cells | quote }}
    {{- end }}
    {{- range $files }}
    - name: {{ .env }}
      value: /etc/dagron-ci/{{ .id }}/value
    {{- end }}
    {{- if $ci.githubApp.id }}
    - name: CI_GITHUB_APP_ID
      value: {{ int64 $ci.githubApp.id | quote }}
    {{- end }}
    {{- if $fleet }}
    - name: CI_GITHUB_FLEET_FILE
      value: /etc/dagron-ci/fleet/fleet.json
    {{- end }}
    {{- with $ci.runUrl }}
    - name: CI_RUN_URL
      value: {{ . | quote }}
    {{- end }}
    {{- with $ci.extraEnv }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
  ports:
    - name: http
      containerPort: {{ $ci.port }}
  # /readyz is "the database answers within a second"; /healthz is
  # "the process answers". dagron-ci stops if its database does not
  # answer when it starts, and is started again: on a first install
  # beside the chart's own Postgres, expect a restart or two before
  # the pod is ready.
  startupProbe:
    httpGet: { path: /readyz, port: http }
    periodSeconds: 2
    timeoutSeconds: {{ $ci.probeTimeoutSeconds | default 3 }}
    failureThreshold: 60
  readinessProbe:
    httpGet: { path: /readyz, port: http }
    periodSeconds: 5
    timeoutSeconds: {{ $ci.probeTimeoutSeconds | default 3 }}
    failureThreshold: 3
  livenessProbe:
    httpGet: { path: /healthz, port: http }
    periodSeconds: 15
  {{- if or $files $fleet }}
  volumeMounts:
    {{- range $files }}
    - name: {{ .id }}
      mountPath: /etc/dagron-ci/{{ .id }}
      readOnly: true
    {{- end }}
    {{- if $fleet }}
    - name: fleet
      mountPath: /etc/dagron-ci/fleet
      readOnly: true
    {{- end }}
  {{- end }}
  resources:
    {{- toYaml $ci.resources | nindent 4 }}
{{- end -}}

{{/*
The volumes that container mounts, as items of a pod's `volumes`: one Secret
each, its one key as the file `value`, and the fleet's label map where there
is one. Nothing when no Secret is named and no label.
Usage: {{- with (include "dagron.ci.volumes" . | trim) }} … {{- . | nindent 8 }}
*/}}
{{- define "dagron.ci.volumes" -}}
{{- range (include "dagron.ci.files" . | fromJsonArray) }}
- name: {{ .id }}
  secret:
    secretName: {{ .ref.name }}
    defaultMode: 0440
    items:
      - key: {{ .ref.key }}
        path: value
{{- end }}
{{- if include "dagron.ci.fleetFile" . }}
- name: fleet
  configMap:
    name: {{ include "dagron.fullname" . }}-ci-fleet
{{- end }}
{{- end -}}
