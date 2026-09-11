{{/*
Flink configuration shared by every FlinkDeployment in this chart.

These are the `flinkConfiguration` entries that belong to the *cluster*, not to a job. Anything a job needs to
reason about (windows, TTL, watermarks) is set in code or in a SET statement instead, so it lives next to the
logic it affects.
*/}}
{{- define "flink-stream.flinkConfig" -}}
{{- $full := include "flink-stream.fullname" .ctx -}}
taskmanager.numberOfTaskSlots: {{ .ctx.Values.flink.taskManager.numberOfTaskSlots | quote }}

# --- checkpointing & state --------------------------------------------------------------------------------
execution.checkpointing.interval: {{ printf "%ds" (int .ctx.Values.flink.common.checkpointIntervalSeconds) | quote }}
execution.checkpointing.mode: "EXACTLY_ONCE"
execution.checkpointing.min-pause: "5s"
execution.checkpointing.timeout: "5min"
execution.checkpointing.externalized-checkpoint-retention: "RETAIN_ON_CANCELLATION"
# Barriers may overtake in-flight records instead of waiting for alignment. Matters here because the
# cross-shard joins create wide exchanges that are exactly where alignment stalls.
execution.checkpointing.unaligned.enabled: "true"
execution.checkpointing.aligned-checkpoint-timeout: "10s"
state.backend.type: "rocksdb"
state.backend.incremental: "true"
state.checkpoints.dir: "file:///flink-data/{{ .name }}/checkpoints"
state.savepoints.dir: "file:///flink-data/{{ .name }}/savepoints"
state.checkpoints.num-retained: "5"

# --- high availability ------------------------------------------------------------------------------------
{{- if .ctx.Values.flink.common.highAvailability }}
# The Kubernetes HA service keeps the JobManager's leader election and job graph pointers in ConfigMaps. The
# state itself stays in the checkpoint directory - HA only removes the JobManager as a single point of failure.
high-availability.type: "kubernetes"
high-availability.storageDir: "file:///flink-data/{{ .name }}/ha"
{{- end }}

# --- restart strategy -------------------------------------------------------------------------------------
restart-strategy.type: "exponential-delay"
restart-strategy.exponential-delay.initial-backoff: "10s"
restart-strategy.exponential-delay.max-backoff: "2min"
restart-strategy.exponential-delay.reset-backoff-threshold: "10min"
# Flink's restart strategy covers *job* failures. A job whose main() threw never started, so only the operator
# can bring it back - and by default it will not. With the init container above this should be rare, but a
# dependency that dies later (Schema Registry restarting, say) still needs it.
kubernetes.operator.job.restart.failed: "true"

# --- classloading -----------------------------------------------------------------------------------------
# child-first means the job jar's copy of a class wins over /opt/flink/lib. That is what lets the unshaded
# flink-connector-kafka inside the job jar coexist with the shaded flink-sql-connector-kafka in lib.
classloader.resolve-order: "child-first"
classloader.check-leaked-classloader: "false"

# --- metrics ----------------------------------------------------------------------------------------------
{{- if .ctx.Values.metrics.prometheus.enabled }}
metrics.reporter.prom.factory.class: "org.apache.flink.metrics.prometheus.PrometheusReporterFactory"
metrics.reporter.prom.port: {{ .ctx.Values.metrics.prometheus.port | quote }}
# Flink's default operator scope already carries <subtask_index>, which is what makes ShardObserver's
# per-subtask gauges distinguishable in Prometheus rather than collapsed into one series.
{{- end }}

# --- autoscaler -------------------------------------------------------------------------------------------
{{- if .ctx.Values.flink.autoscaler.enabled }}
job.autoscaler.enabled: "true"
# false = advisory mode. The operator computes and logs a recommended parallelism per vertex but changes
# nothing, which is what you want while learning what it reacts to.
job.autoscaler.scaling.enabled: {{ .ctx.Values.flink.autoscaler.scaling | quote }}
job.autoscaler.stabilization.interval: {{ printf "%ds" (int .ctx.Values.flink.autoscaler.stabilizationIntervalSeconds) | quote }}
job.autoscaler.metrics.window: {{ printf "%ds" (int .ctx.Values.flink.autoscaler.metricsWindowSeconds) | quote }}
job.autoscaler.target.utilization: {{ .ctx.Values.flink.autoscaler.targetUtilization | quote }}
job.autoscaler.restart.time-tracking.enabled: "true"
# The autoscaler can only rescale within the key-group count fixed at first checkpoint. Setting it explicitly
# (and high) leaves headroom; changing it later invalidates existing state, so it is chosen once, up front.
pipeline.max-parallelism: "120"
{{- end }}

# --- misc -------------------------------------------------------------------------------------------------
taskmanager.memory.managed.fraction: "0.4"
env.java.opts.all: "--add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED"
{{- end -}}


{{/*
Pod template shared by every FlinkDeployment: the endpoint environment, the checkpoint volume, and the
Prometheus annotations.
*/}}
{{- define "flink-stream.flinkPodTemplate" -}}
{{- $full := include "flink-stream.fullname" .ctx -}}
apiVersion: v1
kind: Pod
metadata:
  {{- if .ctx.Values.metrics.prometheus.enabled }}
  annotations:
    prometheus.io/scrape: "true"
    prometheus.io/port: {{ .ctx.Values.metrics.prometheus.port | quote }}
  {{- end }}
  labels:
    {{- include "flink-stream.labels" .ctx | nindent 4 }}
    app.kubernetes.io/component: {{ .name }}
spec:
  {{- include "flink-stream.waitForDependencies" .ctx | nindent 2 }}
  containers:
    - name: flink-main-container
      env:
        {{- include "flink-stream.commonEnv" .ctx | nindent 8 }}
        {{- include "flink-stream.snowflakeEnv" .ctx | nindent 8 }}
        - name: JOB_PARALLELISM
          value: {{ .parallelism | quote }}
        {{- with .extraEnv }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
      {{- if .ctx.Values.metrics.prometheus.enabled }}
      ports:
        - name: metrics
          containerPort: {{ .ctx.Values.metrics.prometheus.port }}
      {{- end }}
      volumeMounts:
        - name: flink-data
          mountPath: /flink-data
        {{- if .mountSql }}
        - name: sql-labs
          mountPath: /opt/flink/sql-labs
          readOnly: true
        {{- end }}
        {{- if .ctx.Values.snowflake.enabled }}
        # The Snowflake token as a file rather than an env var: unlike an env var it does not show up in
        # `kubectl describe pod`, in a dump of the process environment, or in child processes. Rotating it is
        # then `kubectl patch secret` plus a job restart, rather than a redeploy.
        - name: snowflake-pat
          mountPath: /etc/snowflake
          readOnly: true
        {{- end }}
  volumes:
    # One PVC per Flink cluster. A production deployment points state.checkpoints.dir at S3/GCS instead;
    # a local-path RWO volume is the closest single-node equivalent, and it is why each cluster gets its own.
    - name: flink-data
      persistentVolumeClaim:
        claimName: {{ $full }}-{{ .name }}-data
    {{- if .mountSql }}
    - name: sql-labs
      configMap:
        name: {{ $full }}-sql-labs
    {{- end }}
    {{- if .ctx.Values.snowflake.enabled }}
    - name: snowflake-pat
      secret:
        secretName: {{ include "flink-stream.snowflakePatSecret" .ctx }}
        # Read-only to everyone in the pod, not 0400: a Secret volume's files are owned by root and the Flink
        # image runs as uid 9999, so owner-only permissions would make the token unreadable by the job. The
        # isolation boundary here is the pod, not the user inside it.
        defaultMode: 0444
        items:
          - key: {{ .ctx.Values.snowflake.pat.key }}
            path: {{ .ctx.Values.snowflake.pat.key }}
    {{- end }}
{{- end -}}
