{{/* ---------------------------------------------------------------------------------------------------------
Cluster-level Flink configuration, shared by every FlinkDeployment in this chart.

Anything a *job* reasons about (windows, TTL, watermarks) is set in code or in a SET statement, next to the logic
that depends on it. What is here belongs to the cluster: where state goes, how it recovers, and who may rescale it.
--------------------------------------------------------------------------------------------------------- */}}
{{- define "flink-stream-eks.flinkConfig" -}}
{{- $ctx := .ctx -}}
{{- $name := .name -}}
taskmanager.numberOfTaskSlots: {{ $ctx.Values.flink.taskManager.numberOfTaskSlots | quote }}

# --- checkpointing & state --------------------------------------------------------------------------------
# The interval is longer than the local chart's 30s on purpose: every checkpoint here is a set of PUTs to S3,
# billed per request and with first-byte latency to match. 60s is a starting point, not a law - shorten it if
# your recovery objective needs it and you are willing to pay for the requests.
execution.checkpointing.interval: {{ printf "%ds" (int $ctx.Values.flink.common.checkpointIntervalSeconds) | quote }}
execution.checkpointing.mode: "EXACTLY_ONCE"
execution.checkpointing.min-pause: "10s"
execution.checkpointing.timeout: "10min"
execution.checkpointing.externalized-checkpoint-retention: "RETAIN_ON_CANCELLATION"
# Barriers may overtake in-flight records rather than waiting for alignment. The cross-shard joins in this job
# create wide exchanges, which is exactly where alignment stalls under load.
execution.checkpointing.unaligned.enabled: "true"
execution.checkpointing.aligned-checkpoint-timeout: "30s"
state.backend.type: "rocksdb"
state.backend.incremental: "true"
# RocksDB's working files stay on the node (the container's ephemeral disk); only checkpoints go to S3. If your
# nodegroup has NVMe instance storage, point state.backend.rocksdb.localdir at it - that disk is the single
# biggest lever on a large keyed job's throughput.
state.checkpoints.dir: {{ include "flink-stream-eks.s3Path" (dict "ctx" $ctx "name" $name "kind" "checkpoints" "entropy" true) | quote }}
state.savepoints.dir: {{ include "flink-stream-eks.s3Path" (dict "ctx" $ctx "name" $name "kind" "savepoints") | quote }}
state.checkpoints.num-retained: "5"
# Replaces the literal _entropy_ in the checkpoint path above with four random characters per file, so a
# parallelism-12 checkpoint does not land every file on one S3 partition. Savepoints and HA metadata keep a
# stable path on purpose - you have to be able to find them by hand.
s3.entropy.key: "_entropy_"
s3.entropy.length: "4"
{{- range $k, $v := $ctx.Values.stateStore.extraConfig }}
{{ $k }}: {{ $v | quote }}
{{- end }}

# --- high availability ------------------------------------------------------------------------------------
{{- if $ctx.Values.flink.common.highAvailability }}
# Leader election lives in this namespace (ConfigMaps/Leases, which is what the Role grants); the job graph and
# the checkpoint pointers live in S3. That split is what lets a standby JobManager take over and resume rather
# than restart the job from nothing.
high-availability.type: "kubernetes"
high-availability.storageDir: {{ include "flink-stream-eks.s3Path" (dict "ctx" $ctx "name" $name "kind" "ha") | quote }}
{{- end }}

# --- restart strategy -------------------------------------------------------------------------------------
restart-strategy.type: "exponential-delay"
restart-strategy.exponential-delay.initial-backoff: "10s"
restart-strategy.exponential-delay.max-backoff: "5min"
restart-strategy.exponential-delay.reset-backoff-threshold: "10min"
# Flink's restart strategy only covers a *running* job failing. A job whose main() threw never started, so only
# the operator can bring it back - and by default it will not. On EKS the usual cause is transient: a broker
# rolling, a Secret not yet synced, an IAM role not yet propagated.
kubernetes.operator.job.restart.failed: "true"

# --- upgrades ---------------------------------------------------------------------------------------------
# If a savepoint upgrade fails, fall back to the last checkpoint rather than leaving the job down.
kubernetes.operator.job.upgrade.last-state-fallback.enabled: "true"
# Roll back to the last known-good spec when a deployment does not become healthy.
kubernetes.operator.deployment.rollback.enabled: "true"
kubernetes.operator.deployment.readiness.timeout: "10min"
{{- if $ctx.Values.flink.snapshots.enabled }}
kubernetes.operator.periodic.savepoint.interval: "{{ $ctx.Values.flink.snapshots.intervalHours }}h"
kubernetes.operator.savepoint.history.max.count: "10"
kubernetes.operator.savepoint.history.max.age: "7d"
{{- end }}

# --- classloading -------------------------------------------------------------------------------------------
# child-first means the job jar's copy of a class wins over /opt/flink/lib, which is what lets the unshaded
# connectors inside the job jar coexist with the shaded ones in lib.
classloader.resolve-order: "child-first"
classloader.check-leaked-classloader: "false"

# --- metrics ------------------------------------------------------------------------------------------------
{{- if $ctx.Values.metrics.prometheus.enabled }}
metrics.reporter.prom.factory.class: "org.apache.flink.metrics.prometheus.PrometheusReporterFactory"
metrics.reporter.prom.port: {{ $ctx.Values.metrics.prometheus.port | quote }}
{{- end }}

# --- autoscaler -----------------------------------------------------------------------------------------------
{{- if $ctx.Values.flink.autoscaler.enabled }}
job.autoscaler.enabled: "true"
job.autoscaler.scaling.enabled: {{ $ctx.Values.flink.autoscaler.scaling | quote }}
job.autoscaler.stabilization.interval: {{ printf "%ds" (int $ctx.Values.flink.autoscaler.stabilizationIntervalSeconds) | quote }}
job.autoscaler.metrics.window: {{ printf "%ds" (int $ctx.Values.flink.autoscaler.metricsWindowSeconds) | quote }}
job.autoscaler.target.utilization: {{ $ctx.Values.flink.autoscaler.targetUtilization | quote }}
job.autoscaler.restart.time-tracking.enabled: "true"
pipeline.max-parallelism: {{ $ctx.Values.flink.autoscaler.maxParallelism | quote }}
{{- end }}

# --- misc ---------------------------------------------------------------------------------------------------
taskmanager.memory.managed.fraction: "0.4"
env.java.opts.all: "--add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED"
# The operator creates a ClusterIP rest Service; exposure to people is the Ingress's job, not the Service's.
kubernetes.rest-service.exposed.type: "ClusterIP"
{{- range $k, $v := $ctx.Values.flink.extraConfig }}
{{ $k }}: {{ $v | quote }}
{{- end }}
{{- end -}}


{{/* ---------------------------------------------------------------------------------------------------------
The pod template shared by every FlinkDeployment: identity-free AWS credentials, the shared environment, pod
placement, and the SQL ConfigMap where it is wanted.

There are no volumes for state here - that is the whole point of putting checkpoints in S3.
--------------------------------------------------------------------------------------------------------- */}}
{{- define "flink-stream-eks.flinkPodTemplate" -}}
{{- $ctx := .ctx -}}
{{- $full := include "flink-stream-eks.fullname" $ctx -}}
apiVersion: v1
kind: Pod
metadata:
  {{- if $ctx.Values.metrics.prometheus.enabled }}
  annotations:
    prometheus.io/scrape: "true"
    prometheus.io/port: {{ $ctx.Values.metrics.prometheus.port | quote }}
  {{- end }}
  labels:
    {{- include "flink-stream-eks.labels" $ctx | nindent 4 }}
    app.kubernetes.io/component: flink-{{ .name }}
spec:
  {{- with $ctx.Values.image.pullSecrets }}
  imagePullSecrets:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  {{- with $ctx.Values.placement.nodeSelector }}
  nodeSelector:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  {{- with $ctx.Values.placement.tolerations }}
  tolerations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  {{- with $ctx.Values.placement.affinity }}
  affinity:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  {{- with $ctx.Values.placement.topologySpreadConstraints }}
  topologySpreadConstraints:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  {{- with $ctx.Values.placement.priorityClassName }}
  priorityClassName: {{ . }}
  {{- end }}
  {{- if $ctx.Values.flink.common.dependencyCheck }}
  # A cheap reachability check before the JVM starts. An application-mode job builds its Kafka clients in main(),
  # so an unreachable broker is not a job failure Flink can restart - it is a JobManager that never came up. A
  # few seconds in Init turns that into a log line that names what is missing. Uses the job image and bash's
  # /dev/tcp, because the Confluent runtime image has no curl and no netcat.
  initContainers:
    - name: wait-for-endpoints
      image: {{ $ctx.Values.image.repository }}:{{ $ctx.Values.image.tag }}
      imagePullPolicy: {{ $ctx.Values.image.pullPolicy }}
      command: ["/bin/bash", "-c"]
      args:
        - |
          set -uo pipefail
          probe() {
            local label="$1" hostport="$2"
            local host="${hostport%%:*}" port="${hostport##*:}"
            echo -n "waiting for ${label} (${host}:${port}) "
            for i in $(seq 1 60); do
              if (exec 3<>/dev/tcp/"${host}"/"${port}") 2>/dev/null; then echo "ok"; return 0; fi
              echo -n "."
              sleep 5
            done
            echo "TIMEOUT"
            return 1
          }
          probe "kafka" "$(echo "${KAFKA_BOOTSTRAP_SERVERS}" | cut -d, -f1)"
          SR="${SCHEMA_REGISTRY_URL#*://}"
          SR="${SR%%/*}"
          case "${SCHEMA_REGISTRY_URL}" in
            https://*) [[ "${SR}" == *:* ]] || SR="${SR}:443" ;;
            *)         [[ "${SR}" == *:* ]] || SR="${SR}:80"  ;;
          esac
          probe "schema-registry" "${SR}"
          echo "endpoints reachable"
      env:
        - name: KAFKA_BOOTSTRAP_SERVERS
          value: {{ required "kafka.bootstrapServers is required" $ctx.Values.kafka.bootstrapServers | quote }}
        - name: SCHEMA_REGISTRY_URL
          value: {{ required "schemaRegistry.url is required" $ctx.Values.schemaRegistry.url | quote }}
      resources:
        requests: { cpu: 50m, memory: 128Mi }
        limits:   { memory: 256Mi }
  {{- end }}
  containers:
    # flink-main-container is the name the operator gives the JobManager and TaskManager containers; matching it
    # here merges into that container rather than adding a sidecar.
    - name: flink-main-container
      env:
        {{- include "flink-stream-eks.commonEnv" $ctx | nindent 8 }}
        - name: JOB_PARALLELISM
          value: {{ .parallelism | quote }}
        {{- with .extraEnv }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
      {{- if $ctx.Values.metrics.prometheus.enabled }}
      ports:
        - name: metrics
          containerPort: {{ $ctx.Values.metrics.prometheus.port }}
      {{- end }}
      {{- if .mountSql }}
      volumeMounts:
        - name: sql-labs
          mountPath: {{ $ctx.Values.flink.sql.sqlDir }}
          readOnly: true
      {{- end }}
  {{- if .mountSql }}
  volumes:
    - name: sql-labs
      configMap:
        name: {{ $full }}-sql-labs
  {{- end }}
{{- end -}}
