#!/usr/bin/env bash
# Shared settings and helpers. Sourced by every other script; not meant to be run directly.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# --- what gets deployed where ---------------------------------------------------------------------------------
export NAMESPACE="${NAMESPACE:-flink-stream}"
export RELEASE="${RELEASE:-fs}"
export OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-flink-operator}"

# --- versions -------------------------------------------------------------------------------------------------
# Confluent's build of the Apache Flink Kubernetes Operator. Chart 1.150.3 == operator 1.15.0-cp3.
export CONFLUENT_HELM_REPO="${CONFLUENT_HELM_REPO:-https://packages.confluent.io/helm}"
export FLINK_OPERATOR_CHART_VERSION="${FLINK_OPERATOR_CHART_VERSION:-1.150.3}"
export CERT_MANAGER_VERSION="${CERT_MANAGER_VERSION:-v1.16.2}"

# --- the job image --------------------------------------------------------------------------------------------
export IMAGE_NAME="${IMAGE_NAME:-flink-stream-jobs}"
export IMAGE_TAG="${IMAGE_TAG:-1.0.0}"
export IMAGE="${IMAGE_NAME}:${IMAGE_TAG}"

# --- pretty output --------------------------------------------------------------------------------------------
if [[ -t 1 ]]; then
  BOLD=$'\033[1m'; DIM=$'\033[2m'; RED=$'\033[31m'; GREEN=$'\033[32m'; YELLOW=$'\033[33m'; RESET=$'\033[0m'
else
  BOLD=""; DIM=""; RED=""; GREEN=""; YELLOW=""; RESET=""
fi

step() { echo; echo "${BOLD}==> $*${RESET}"; }
info() { echo "    $*"; }
warn() { echo "${YELLOW}    ! $*${RESET}"; }
die()  { echo "${RED}    x $*${RESET}" >&2; exit 1; }
ok()   { echo "${GREEN}    v $*${RESET}"; }

require() {
  for cmd in "$@"; do
    command -v "$cmd" >/dev/null 2>&1 || die "'$cmd' is required but not on PATH"
  done
}

# Verifies we are pointed at the local OrbStack cluster and not, say, a production context.
require_local_cluster() {
  local ctx
  ctx="$(kubectl config current-context 2>/dev/null || true)"
  [[ -n "$ctx" ]] || die "no kubectl context is set"
  if [[ "$ctx" != "orbstack" && "$ctx" != "docker-desktop" && "$ctx" != minikube* && "$ctx" != kind-* && "$ctx" != rancher-desktop ]]; then
    warn "kubectl context is '${ctx}', which does not look like a local cluster."
    read -r -p "    Continue anyway? [y/N] " reply
    [[ "$reply" == "y" || "$reply" == "Y" ]] || die "aborted"
  fi
}

helm_release_name() { echo "${RELEASE}-flink-stream"; }
