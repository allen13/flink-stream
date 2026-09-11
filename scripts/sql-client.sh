#!/usr/bin/env bash
#
# Opens the Flink SQL client against the session cluster.
#
# Needs the session cluster:
#   helm upgrade fs charts/flink-stream -n flink-stream --reuse-values --set flink.session.enabled=true
#
# Inside the client, load the lab DDL and then explore:
#   Flink SQL> SET 'sql-client.execution.result-mode' = 'tableau';
#   Flink SQL> -- paste from charts/flink-stream/files/sql/10-sources.sql, then:
#   Flink SQL> SELECT `partition`, shard_id, COUNT(*) FROM transactions GROUP BY `partition`, shard_id;

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require kubectl

FULL="$(helm_release_name)"
JM=$(kubectl -n "${NAMESPACE}" get pod \
      -l "app.kubernetes.io/name=flink-stream,component=jobmanager" \
      -o name 2>/dev/null | grep session | head -1)

if [[ -z "$JM" ]]; then
  # The operator labels JobManager pods by deployment name rather than by our chart labels.
  JM=$(kubectl -n "${NAMESPACE}" get pod -o name 2>/dev/null | grep "${FULL}-session" | head -1)
fi
[[ -n "$JM" ]] || die "no session-cluster JobManager pod found. Enable it with --set flink.session.enabled=true"

info "attaching to ${JM}"
info "the SQL lab files are mounted inside the pod at /opt/flink/sql-labs"
echo

# -j adds a jar to the *user* classloader of whatever this client submits, and Flink ships it to the
# TaskManagers through the blob server. The job jar is exactly the right thing to pass: it already carries the
# unshaded Kafka connector, the avro-confluent format and this project's UDFs, all mutually consistent. The fat
# flink-sql-* connectors are deliberately not installed anywhere in the image - see flink-jobs/Dockerfile for
# the classloading reason.
kubectl -n "${NAMESPACE}" exec -it "${JM##*/}" -- \
  /opt/flink/bin/sql-client.sh embedded \
    -j /opt/flink/usrlib/flink-stream-jobs.jar \
    -Dexecution.checkpointing.interval=30s \
    -Dexecution.runtime-mode=STREAMING \
    -Dtable.exec.source.idle-timeout=15s
