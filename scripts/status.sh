#!/usr/bin/env bash
# A one-screen view of the whole environment.

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require kubectl

step "Pods"
kubectl -n "${NAMESPACE}" get pods -o wide --sort-by=.metadata.name 2>/dev/null || die "namespace ${NAMESPACE} not found"

step "Flink jobs (as the operator sees them)"
kubectl -n "${NAMESPACE}" get flinkdeployment -o custom-columns=\
'NAME:.metadata.name,LIFECYCLE:.status.lifecycleState,JOB:.status.jobStatus.state,JOBMANAGER:.status.jobManagerDeploymentStatus,RESTARTS:.status.jobStatus.jobId' 2>/dev/null \
  || info "no FlinkDeployments yet"

step "Init jobs"
kubectl -n "${NAMESPACE}" get jobs 2>/dev/null | sed 's/^/    /' || true

step "Kafka topics"
POD=$(kubectl -n "${NAMESPACE}" get pod -l app.kubernetes.io/component=kafka -o name 2>/dev/null | head -1)
if [[ -n "$POD" ]]; then
  kubectl -n "${NAMESPACE}" exec "$POD" -- bash -c \
    'kafka-topics --bootstrap-server localhost:9092 --describe 2>/dev/null | grep "^Topic:" | awk "{print \$2, \$4, \$6}" | column -t' \
    2>/dev/null | sed 's/^/    /' || info "Kafka not answering yet"
else
  info "no Kafka pod"
fi

step "Registered Avro subjects"
SR=$(kubectl -n "${NAMESPACE}" get pod -l app.kubernetes.io/component=schema-registry -o name 2>/dev/null | head -1)
if [[ -n "$SR" ]]; then
  kubectl -n "${NAMESPACE}" exec "$SR" -- curl -s localhost:8081/subjects 2>/dev/null \
    | tr ',' '\n' | tr -d '[]"' | sed 's/^/    /' || info "Schema Registry not answering yet"
fi

step "Recent warnings"
kubectl -n "${NAMESPACE}" get events --field-selector type=Warning \
  --sort-by=.lastTimestamp 2>/dev/null | tail -8 | sed 's/^/    /' || true
