#!/usr/bin/env bash
#
# Opens every UI at once. Ctrl-C tears them all down.

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require kubectl

FULL="$(helm_release_name)"
PIDS=()
cleanup() { echo; info "closing port-forwards"; for pid in ${PIDS[@]+"${PIDS[@]}"}; do kill "$pid" 2>/dev/null || true; done; }
trap cleanup EXIT INT TERM

forward() {
  local svc="$1" local_port="$2" remote_port="$3" label="$4"
  if kubectl -n "${NAMESPACE}" get svc "$svc" >/dev/null 2>&1; then
    kubectl -n "${NAMESPACE}" port-forward "svc/$svc" "${local_port}:${remote_port}" >/dev/null 2>&1 &
    PIDS+=($!)
    printf "    %-26s ${BOLD}http://localhost:%s${RESET}\n" "$label" "$local_port"
  else
    printf "    %-26s ${DIM}(not deployed)${RESET}\n" "$label"
  fi
}

step "Forwarding"
forward "${FULL}-datastream-rest"  8081 8081 "Flink UI - DataStream"
forward "${FULL}-sql-rest"         8082 8081 "Flink UI - SQL"
forward "${FULL}-hybrid-rest"      8083 8081 "Flink UI - hybrid"
forward "${FULL}-session-rest"     8084 8081 "Flink UI - session"
forward "${FULL}-kafka-ui"         8080 8080 "Kafka UI"
forward "${FULL}-schema-registry"  8085 8081 "Schema Registry API"
forward "${FULL}-minio"            9001 9001 "MinIO console"
forward "${FULL}-iceberg-rest"     8181 8181 "Iceberg REST catalog"
forward "${FULL}-yugabyte-ysql"   15433 15433 "YugabyteDB UI"
forward "${FULL}-yugabyte-ysql"    5433 5433  "YugabyteDB YSQL (psql)"

echo
info "MinIO login: see minio.accessKey / minio.secretKey in values.yaml"
info "psql: psql -h localhost -p 5433 -U yugabyte -d flink_stream"
echo
info "Ctrl-C to stop."
wait
