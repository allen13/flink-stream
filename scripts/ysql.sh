#!/usr/bin/env bash
#
# A ysqlsh shell inside the YugabyteDB pod, or a one-off query:
#   scripts/ysql.sh
#   scripts/ysql.sh "SELECT region, SUM(total_usd) FROM region_revenue GROUP BY region;"

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require kubectl

POD=$(kubectl -n "${NAMESPACE}" get pod -l app.kubernetes.io/component=yugabyte -o name | head -1)
[[ -n "$POD" ]] || die "no YugabyteDB pod found"
POD="${POD##*/}"
DB="${YUGABYTE_DB:-flink_stream}"

if [[ $# -gt 0 ]]; then
  kubectl -n "${NAMESPACE}" exec "$POD" -- bash -c \
    "/home/yugabyte/bin/ysqlsh -h \$(hostname -i) -p 5433 -U yugabyte -d ${DB} -c \"$*\""
else
  kubectl -n "${NAMESPACE}" exec -it "$POD" -- bash -c \
    "/home/yugabyte/bin/ysqlsh -h \$(hostname -i) -p 5433 -U yugabyte -d ${DB}"
fi
