{{/*
Naming, labels, credentials and the client properties that several templates need to agree on.

Everything is named <release>-<component> and carries the standard recommended labels, so
`kubectl get all -l app.kubernetes.io/instance=<release>` shows the release and
`-l app.kubernetes.io/component=flink-sql` narrows it to one job.
*/}}

{{- define "flink-stream-eks.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "flink-stream-eks.fullname" -}}
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

{{- define "flink-stream-eks.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "flink-stream-eks.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: flink-stream
{{- end -}}

{{- define "flink-stream-eks.selectorLabels" -}}
app.kubernetes.io/name: {{ include "flink-stream-eks.name" .ctx }}
app.kubernetes.io/instance: {{ .ctx.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{/*
The Secret every credential comes from: the one this chart creates, or the one you point it at.
Empty when neither is configured, which the callers treat as "no credentials available".
*/}}
{{- define "flink-stream-eks.secretName" -}}
{{- if .Values.secret.existingSecret -}}
{{- .Values.secret.existingSecret -}}
{{- else if .Values.secret.create -}}
{{- printf "%s-credentials" (include "flink-stream-eks.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/*
s3://<bucket>/<path>/<cluster>/<kind> - one prefix per Flink cluster, never shared: two jobs writing to one
checkpoint directory will overwrite each other's metadata.

With "entropy" the literal _entropy_ is placed directly under the bucket. Flink replaces it with four random
characters per file, which matters because S3 partitions keys by their leading characters and a job checkpointing
at parallelism 12 writes every file under the same prefix at the same instant. The setting only does anything if
the marker is actually in the path, which is why it is here and not only in the config.
*/}}
{{- define "flink-stream-eks.s3Path" -}}
{{- $b := required "stateStore.bucket is required: Flink needs somewhere durable for checkpoints" .ctx.Values.stateStore.bucket -}}
{{- $prefix := trimSuffix "/" .ctx.Values.stateStore.path -}}
{{- if .entropy -}}
{{- printf "s3://%s/_entropy_/%s/%s/%s" $b $prefix .name .kind -}}
{{- else -}}
{{- printf "s3://%s/%s/%s/%s" $b $prefix .name .kind -}}
{{- end -}}
{{- end -}}

{{- define "flink-stream-eks.icebergWarehouse" -}}
{{- if .Values.iceberg.warehouse -}}
{{- .Values.iceberg.warehouse -}}
{{- else -}}
{{- printf "s3://%s/warehouse" (required "stateStore.bucket is required" .Values.stateStore.bucket) -}}
{{- end -}}
{{- end -}}

{{/* ---------------------------------------------------------------------------------------------------------
Kafka client properties.

One definition, three consumers: the KAFKA_CLIENT_PROPS env var, the topics init Job's --command-config file,
and the `properties.*` options injected into every Kafka table in the SQL labs. They must agree, so they are all
derived from here.

Passwords are never interpolated: the JAAS string carries a literal ${KAFKA_SASL_PASSWORD}, which the SQL runner
resolves from the environment and the init Job's shell expands. The value itself comes from a Secret.
--------------------------------------------------------------------------------------------------------- */}}
{{- define "flink-stream-eks.kafkaProperties" -}}
{{- $p := dict -}}
{{- $auth := .Values.kafka.auth | default "none" -}}
{{- if eq $auth "msk_iam" -}}
{{- $_ := set $p "security.protocol" "SASL_SSL" -}}
{{- $_ := set $p "sasl.mechanism" "AWS_MSK_IAM" -}}
{{- $_ := set $p "sasl.jaas.config" "software.amazon.msk.auth.iam.IAMLoginModule required;" -}}
{{- $_ := set $p "sasl.client.callback.handler.class" "software.amazon.msk.auth.iam.IAMClientCallbackHandler" -}}
{{- else if eq $auth "sasl_scram" -}}
{{- $u := required "kafka.saslUsername is required for auth: sasl_scram" .Values.kafka.saslUsername -}}
{{- $_ := set $p "security.protocol" "SASL_SSL" -}}
{{- $_ := set $p "sasl.mechanism" "SCRAM-SHA-512" -}}
{{- $_ := set $p "sasl.jaas.config" (printf "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"${KAFKA_SASL_PASSWORD}\";" $u) -}}
{{- else if eq $auth "sasl_plain" -}}
{{- $u := required "kafka.saslUsername is required for auth: sasl_plain" .Values.kafka.saslUsername -}}
{{- $_ := set $p "security.protocol" "SASL_SSL" -}}
{{- $_ := set $p "sasl.mechanism" "PLAIN" -}}
{{- $_ := set $p "sasl.jaas.config" (printf "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"${KAFKA_SASL_PASSWORD}\";" $u) -}}
{{- else if eq $auth "tls" -}}
{{- $_ := set $p "security.protocol" "SSL" -}}
{{- else if ne $auth "none" -}}
{{- fail (printf "kafka.auth: %q is not one of none|msk_iam|sasl_scram|sasl_plain|tls" $auth) -}}
{{- end -}}
{{- range $k, $v := .Values.kafka.properties -}}
{{- $_ := set $p $k $v -}}
{{- end -}}
{{- toYaml $p -}}
{{- end -}}

{{/* Schema Registry options, without the format prefix - the caller supplies it. */}}
{{- define "flink-stream-eks.schemaRegistryProperties" -}}
{{- $p := dict -}}
{{- if eq (.Values.schemaRegistry.auth | default "none") "basic" -}}
{{- $u := required "schemaRegistry.username is required for auth: basic" .Values.schemaRegistry.username -}}
{{- $_ := set $p "basic-auth.credentials-source" "USER_INFO" -}}
{{- $_ := set $p "basic-auth.user-info" (printf "%s:${SCHEMA_REGISTRY_PASSWORD}" $u) -}}
{{- end -}}
{{- toYaml $p -}}
{{- end -}}

{{/*
Properties as Java .properties lines, for the KAFKA_CLIENT_PROPS env var and the init Job's config file.
*/}}
{{- define "flink-stream-eks.propertyLines" -}}
{{- range $k, $v := . }}
{{ $k }}={{ $v }}
{{- end }}
{{- end -}}

{{/*
The same properties as SQL connector options, ready to be spliced into a WITH block.

Each option is emitted as ",\n    '<prefix><key>' = '<value>'" - leading comma, no trailing one - so it can be
appended straight after an existing option and inherit that option's trailing comma (or its absence, at the end
of a WITH block). $ is doubled because this string is used as a regexp replacement, where $ introduces a capture
reference; ${1} is intentional and expands to the format prefix the regexp matched.
*/}}
{{- define "flink-stream-eks.sqlOptions" -}}
{{- $prefix := .prefix -}}
{{- range $k, $v := .props -}}
,
    '{{ $prefix }}{{ $k }}' = '{{ $v | replace "$" "$$" }}'
{{- end -}}
{{- end -}}

{{/* ---------------------------------------------------------------------------------------------------------
The environment shared by every Flink job and the generator.

Endpoints come from values; secrets come from the Secret; the AWS credentials nobody sets here come from the
ServiceAccount's IRSA annotation through the SDK's default chain.
--------------------------------------------------------------------------------------------------------- */}}
{{- define "flink-stream-eks.commonEnv" -}}
{{- $secret := include "flink-stream-eks.secretName" . -}}
{{- $kafkaProps := include "flink-stream-eks.kafkaProperties" . | fromYaml -}}
- name: KAFKA_BOOTSTRAP_SERVERS
  value: {{ required "kafka.bootstrapServers is required" .Values.kafka.bootstrapServers | quote }}
- name: SCHEMA_REGISTRY_URL
  value: {{ required "schemaRegistry.url is required" .Values.schemaRegistry.url | quote }}
- name: KAFKA_GROUP_ID
  value: {{ .Values.kafka.consumerGroup | quote }}
- name: KAFKA_TXN_ID_PREFIX
  value: {{ .Values.kafka.transactionalIdPrefix | quote }}
{{- if $kafkaProps }}
# Read by the SQL job through its DDL. See the README for the JobConfig change the DataStream job needs before
# it honours these too; with kafka.auth: none there is nothing to honour and every job works as is.
- name: KAFKA_CLIENT_PROPS
  value: |-
    {{- include "flink-stream-eks.propertyLines" $kafkaProps | trim | nindent 4 }}
{{- end }}
{{- if and (has .Values.kafka.auth (list "sasl_scram" "sasl_plain")) $secret }}
- name: KAFKA_SASL_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ $secret }}
      key: {{ .Values.secret.keys.kafkaSaslPassword }}
{{- end }}
{{- if and (eq (.Values.schemaRegistry.auth | default "none") "basic") $secret }}
- name: SCHEMA_REGISTRY_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ $secret }}
      key: {{ .Values.secret.keys.schemaRegistryPassword }}
{{- end }}
- name: AWS_REGION
  value: {{ .Values.aws.region | quote }}
- name: AWS_DEFAULT_REGION
  value: {{ .Values.aws.region | quote }}
# Regional STS endpoints: the global one is a single point of failure and is rate-limited per account, which a
# cluster restarting a few dozen TaskManagers at once will find.
- name: AWS_STS_REGIONAL_ENDPOINTS
  value: "regional"
{{- if .Values.stateStore.builtInPlugins }}
# The entrypoint copies these jars from /opt/flink/opt into /opt/flink/plugins/<name>/ before starting, which is
# the only way a Flink filesystem is loaded. Without it every s3:// path fails with "Could not find a file
# system implementation for scheme 's3'".
- name: ENABLE_BUILT_IN_PLUGINS
  value: {{ join ";" .Values.stateStore.builtInPlugins | quote }}
{{- end }}
- name: YUGABYTE_ENABLED
  value: {{ .Values.database.enabled | quote }}
{{- if .Values.database.enabled }}
- name: YUGABYTE_JDBC_URL
  value: {{ required "database.jdbcUrl is required when database.enabled" .Values.database.jdbcUrl | quote }}
- name: YUGABYTE_USER
  value: {{ .Values.database.username | quote }}
- name: YUGABYTE_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ required "database.enabled needs credentials: set secret.existingSecret, or secret.create with secret.values.databasePassword" $secret }}
      key: {{ .Values.secret.keys.databasePassword }}
{{- end }}
{{- if .Values.iceberg.enabled }}
- name: ICEBERG_WAREHOUSE
  value: {{ include "flink-stream-eks.icebergWarehouse" . | quote }}
{{- end }}
{{- end -}}
