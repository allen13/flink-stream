#!/usr/bin/env bash
#
# Installs (or upgrades) the flink-stream chart.
#
#   scripts/deploy.sh                       full profile
#   scripts/deploy.sh --lite                single broker, one Flink job, ~5 GiB
#   scripts/deploy.sh --set flink.session.enabled=true
#
# Any extra arguments are passed through to helm.

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

require kubectl helm docker
require_local_cluster

# macOS still ships bash 3.2, where expanding an empty array under `set -u` is an error. The
# ${arr[@]+"${arr[@]}"} idiom expands to nothing when the array is empty and to the elements otherwise.
EXTRA=()
for arg in "$@"; do
  case "$arg" in
    --lite) EXTRA+=(-f "${REPO_ROOT}/charts/flink-stream/values-lite.yaml") ;;
    *)      EXTRA+=("$arg") ;;
  esac
done

step "Preflight"
if ! docker image inspect "${IMAGE}" >/dev/null 2>&1; then
  die "image ${IMAGE} not found locally - run scripts/build-image.sh first"
fi
ok "image ${IMAGE} present"

if ! kubectl get crd flinkdeployments.flink.apache.org >/dev/null 2>&1; then
  die "the Flink operator CRDs are missing - run scripts/bootstrap.sh first"
fi
ok "Flink operator CRDs present"

# Memory is the binding constraint on a laptop cluster, and the failure mode without this check is a set of
# pods stuck Pending with "Insufficient memory" buried in their events.
ALLOCATABLE_MI=$(kubectl get nodes -o jsonpath='{.items[*].status.allocatable.memory}' \
  | tr ' ' '\n' | sed 's/Ki$//' | awk '{s+=$1} END {printf "%d", s/1024}')
# Everything already requested, *excluding* this release - otherwise an upgrade counts its own current pods
# against itself and always looks like it will not fit.
REQUESTED_MI=$(kubectl get pods -A -o json 2>/dev/null | python3 -c '
import json, sys, re
release = sys.argv[1]; ns = sys.argv[2]
def mi(value):
    if not value: return 0.0
    m = re.match(r"^(\d+(?:\.\d+)?)(Gi|Mi|Ki|G|M|K)?$", str(value))
    if not m: return 0.0
    n = float(m.group(1)); unit = m.group(2) or ""
    return n * {"Gi":1024,"Mi":1,"Ki":1/1024,"G":954,"M":0.954,"K":0.00093,"":1/1048576}[unit]
total = 0.0
for pod in json.load(sys.stdin)["items"]:
    meta, spec = pod["metadata"], pod["spec"]
    if pod["status"].get("phase") not in ("Running", "Pending"): continue
    if meta["namespace"] == ns and meta.get("labels", {}).get("app.kubernetes.io/instance") == release: continue
    for c in spec.get("containers", []):
        total += mi(c.get("resources", {}).get("requests", {}).get("memory"))
print(int(total))
' "${RELEASE}" "${NAMESPACE}")
FREE_MI=$(( ALLOCATABLE_MI - REQUESTED_MI ))
NEEDED_MI=11000
if [[ -n "${EXTRA[*]:-}" && "${EXTRA[*]}" == *values-lite* ]]; then NEEDED_MI=5200; fi

info "node memory: ${ALLOCATABLE_MI} Mi allocatable, ${REQUESTED_MI} Mi already requested, ${FREE_MI} Mi free"
if (( FREE_MI < NEEDED_MI )); then
  warn "this profile wants about ${NEEDED_MI} Mi and only ${FREE_MI} Mi is free."
  warn "Options: use --lite, raise the VM's memory (OrbStack: Settings > Resources), or free some workloads."
  read -r -p "    Continue anyway? [y/N] " reply
  [[ "$reply" == "y" || "$reply" == "Y" ]] || die "aborted"
else
  ok "enough free memory for this profile (~${NEEDED_MI} Mi needed)"
fi

# Init Jobs are named per revision because a completed Job is immutable. Reap the ones from older revisions so
# their (now pointless) pods stop holding memory.
CURRENT_REV=$(helm history "${RELEASE}" -n "${NAMESPACE}" -o json 2>/dev/null | python3 -c 'import json,sys;h=json.load(sys.stdin);print(h[-1]["revision"] if h else 0)' 2>/dev/null || echo 0)
for job in $(kubectl -n "${NAMESPACE}" get jobs -o name 2>/dev/null | grep -- "-init-" || true); do
  rev="${job##*-}"
  if [[ "$rev" =~ ^[0-9]+$ ]] && (( rev <= CURRENT_REV )); then
    kubectl -n "${NAMESPACE}" delete "$job" --ignore-not-found >/dev/null 2>&1 || true
  fi
done

step "helm upgrade --install $(helm_release_name)"
helm upgrade --install "${RELEASE}" "${REPO_ROOT}/charts/flink-stream" \
  --namespace "${NAMESPACE}" --create-namespace \
  --set flink.image="${IMAGE}" \
  --timeout 15m \
  ${EXTRA[@]+"${EXTRA[@]}"}

step "Waiting for the platform to settle"
# The Flink jobs cannot start until Kafka, the registry and the sinks are up, and the operator retries on its
# own until they are - so this only waits on the infrastructure and then reports.
for component in kafka schema-registry yugabyte minio iceberg-rest; do
  info "waiting for ${component} ..."
  kubectl -n "${NAMESPACE}" wait --for=condition=ready pod \
    -l "app.kubernetes.io/component=${component},app.kubernetes.io/instance=${RELEASE}" \
    --timeout=10m 2>/dev/null || warn "${component} not ready yet (it may still be starting)"
done

step "Status"
"${REPO_ROOT}/scripts/status.sh"
