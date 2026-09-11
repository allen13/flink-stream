{{/*
Shared naming and label helpers.

Every resource in this chart is named <release>-<component> and carries the standard Kubernetes recommended
labels, so `kubectl get all -l app.kubernetes.io/instance=<release>` shows the whole environment and
`-l app.kubernetes.io/component=kafka` narrows it to one piece.
*/}}

{{- define "flink-stream.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "flink-stream.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "flink-stream.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "flink-stream.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: flink-stream
{{- end -}}

{{/* Selector labels for one component. Usage: {{ include "flink-stream.selectorLabels" (dict "ctx" . "component" "kafka") }} */}}
{{- define "flink-stream.selectorLabels" -}}
app.kubernetes.io/name: {{ include "flink-stream.name" .ctx }}
app.kubernetes.io/instance: {{ .ctx.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{- define "flink-stream.componentName" -}}
{{- printf "%s-%s" (include "flink-stream.fullname" .ctx) .component | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
The bootstrap string every client uses.

It lists all broker pods through the headless service rather than the load-balancing ClusterIP: a Kafka client
must reach each broker individually, because the partition leader it needs is a specific pod. Pointing a client
at a round-robin service is a classic way to get intermittent NOT_LEADER errors.
*/}}
{{- define "flink-stream.kafkaBootstrap" -}}
{{- $full := include "flink-stream.fullname" . -}}
{{- $ns := .Release.Namespace -}}
{{- $replicas := int .Values.kafka.replicas -}}
{{- $servers := list -}}
{{- range $i := until $replicas -}}
{{- $servers = append $servers (printf "%s-kafka-%d.%s-kafka-headless.%s.svc.cluster.local:9092" $full $i $full $ns) -}}
{{- end -}}
{{- join "," $servers -}}
{{- end -}}

{{- define "flink-stream.schemaRegistryUrl" -}}
{{- printf "http://%s-schema-registry:8081" (include "flink-stream.fullname" .) -}}
{{- end -}}

{{- define "flink-stream.yugabyteJdbcUrl" -}}
{{- printf "jdbc:postgresql://%s-yugabyte-ysql:5433/%s" (include "flink-stream.fullname" .) .Values.yugabyte.database -}}
{{- end -}}

{{- define "flink-stream.icebergRestUri" -}}
{{- printf "http://%s-iceberg-rest:8181" (include "flink-stream.fullname" .) -}}
{{- end -}}

{{- define "flink-stream.minioEndpoint" -}}
{{- printf "http://%s-minio:9000" (include "flink-stream.fullname" .) -}}
{{- end -}}

{{/*
The Secret holding the Snowflake programmatic access token: one you created out of band if you named it,
otherwise the one this chart renders from snowflake.pat.value.
*/}}
{{- define "flink-stream.snowflakePatSecret" -}}
{{- if .Values.snowflake.pat.existingSecret -}}
{{- .Values.snowflake.pat.existingSecret -}}
{{- else -}}
{{- printf "%s-snowflake-pat" (include "flink-stream.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/*
Snowflake coordinates for the Flink jobs. Everything here is non-secret and safe in a pod spec; the token
itself arrives as a file from the Secret volume added by flinkPodTemplate, and is never an env value or a job
argument - Flink renders job arguments on the job's page in the web UI.
*/}}
{{- define "flink-stream.snowflakeEnv" -}}
- name: SNOWFLAKE_ENABLED
  value: {{ .Values.snowflake.enabled | quote }}
{{- if .Values.snowflake.enabled }}
- name: SNOWFLAKE_PAT_FILE
  value: {{ printf "/etc/snowflake/%s" .Values.snowflake.pat.key | quote }}
{{- with .Values.snowflake.url }}
- name: SNOWFLAKE_URL
  value: {{ . | quote }}
{{- end }}
{{- with .Values.snowflake.account }}
- name: SNOWFLAKE_ACCOUNT
  value: {{ . | quote }}
{{- end }}
{{- with .Values.snowflake.host }}
- name: SNOWFLAKE_HOST
  value: {{ . | quote }}
{{- end }}
- name: SNOWFLAKE_USER
  value: {{ .Values.snowflake.user | quote }}
- name: SNOWFLAKE_ROLE
  value: {{ .Values.snowflake.role | quote }}
- name: SNOWFLAKE_WAREHOUSE
  value: {{ .Values.snowflake.warehouse | quote }}
- name: SNOWFLAKE_DATABASE
  value: {{ .Values.snowflake.database | quote }}
- name: SNOWFLAKE_SCHEMA
  value: {{ .Values.snowflake.schema | quote }}
- name: SNOWFLAKE_AUTHENTICATOR
  value: {{ .Values.snowflake.authenticator | quote }}
- name: SNOWFLAKE_BATCH_SIZE
  value: {{ .Values.snowflake.batchSize | quote }}
- name: SNOWFLAKE_BATCH_INTERVAL_MS
  value: {{ .Values.snowflake.batchIntervalMs | quote }}
{{- end }}
{{- end -}}


{{/*
The environment block shared by the generator and every Flink job, so a job never hardcodes an endpoint.
*/}}
{{- define "flink-stream.commonEnv" -}}
- name: KAFKA_BOOTSTRAP_SERVERS
  value: {{ include "flink-stream.kafkaBootstrap" . | quote }}
- name: SCHEMA_REGISTRY_URL
  value: {{ include "flink-stream.schemaRegistryUrl" . | quote }}
- name: YUGABYTE_JDBC_URL
  value: {{ include "flink-stream.yugabyteJdbcUrl" . | quote }}
- name: YUGABYTE_USER
  value: {{ .Values.yugabyte.username | quote }}
- name: YUGABYTE_PASSWORD
  value: {{ .Values.yugabyte.password | quote }}
- name: ICEBERG_REST_URI
  value: {{ include "flink-stream.icebergRestUri" . | quote }}
- name: ICEBERG_WAREHOUSE
  value: {{ .Values.iceberg.warehouse | quote }}
- name: S3_ENDPOINT
  value: {{ include "flink-stream.minioEndpoint" . | quote }}
- name: S3_ACCESS_KEY
  value: {{ .Values.minio.accessKey | quote }}
- name: S3_SECRET_KEY
  value: {{ .Values.minio.secretKey | quote }}
- name: AWS_ACCESS_KEY_ID
  value: {{ .Values.minio.accessKey | quote }}
- name: AWS_SECRET_ACCESS_KEY
  value: {{ .Values.minio.secretKey | quote }}
- name: AWS_REGION
  value: {{ .Values.minio.region | quote }}
{{- end -}}


{{/*
An initContainer that blocks until Kafka, Schema Registry, the Iceberg catalog and YugabyteDB all answer.

Both the Flink jobs and the generator connect to all of these the instant they start. Without this, a cold
cluster is a startup race: whichever pod wins, the losers crash, and in a Flink application deployment the
failure is in main() rather than in the job - so Flink's restart strategy does not even apply and only the
operator (with kubernetes.operator.job.restart.failed) can recover it.

Waiting here turns that race into a few seconds of Init state, and the container logs say exactly what is
missing when something really is broken.
*/}}
{{- define "flink-stream.waitForDependencies" -}}
initContainers:
  - name: wait-for-dependencies
    image: {{ .Values.kafka.image }}
    command: ["/bin/bash", "-c"]
    args:
      - |
        set -uo pipefail
        wait_for() {
          local label="$1" ; shift
          echo -n "waiting for ${label} "
          for i in $(seq 1 120); do
            if "$@" >/dev/null 2>&1; then echo "ok"; return 0; fi
            echo -n "."
            sleep 5
          done
          echo "TIMEOUT"
          return 1
        }
        wait_for "kafka"           kafka-broker-api-versions --bootstrap-server "{{ include "flink-stream.kafkaBootstrap" . }}"
        wait_for "schema-registry" curl -fsS "{{ include "flink-stream.schemaRegistryUrl" . }}/subjects"
        {{- if .Values.iceberg.enabled }}
        wait_for "iceberg-rest"    curl -fsS "{{ include "flink-stream.icebergRestUri" . }}/v1/config"
        {{- end }}
        {{- if .Values.yugabyte.enabled }}
        wait_for "yugabyte"        bash -c 'exec 3<>/dev/tcp/{{ include "flink-stream.fullname" . }}-yugabyte-ysql/5433'
        {{- end }}
        echo "all dependencies reachable"
    resources:
      requests: { cpu: 50m, memory: 192Mi }
      limits:   { memory: 384Mi }
{{- end -}}
