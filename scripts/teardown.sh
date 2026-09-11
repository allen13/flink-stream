#!/usr/bin/env bash
#
#   scripts/teardown.sh          remove the flink-stream release (keeps PVCs)
#   scripts/teardown.sh --all    also delete PVCs, the namespace, the operator and cert-manager

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require kubectl helm

step "Removing the ${RELEASE} release from ${NAMESPACE}"
helm uninstall "${RELEASE}" -n "${NAMESPACE}" 2>/dev/null || warn "release not found"

# FlinkDeployments have finalizers; the operator clears them once it has torn the cluster down. If the operator
# is already gone they would block namespace deletion forever.
info "waiting for FlinkDeployments to finalize"
kubectl -n "${NAMESPACE}" wait --for=delete flinkdeployment --all --timeout=3m 2>/dev/null || true

if [[ "${1:-}" == "--all" ]]; then
  step "Deleting persistent volumes and the namespace"
  kubectl -n "${NAMESPACE}" delete pvc --all --timeout=2m 2>/dev/null || true
  kubectl delete namespace "${NAMESPACE}" --timeout=5m 2>/dev/null || true

  step "Removing the operator and cert-manager"
  helm uninstall flink-kubernetes-operator -n "${OPERATOR_NAMESPACE}" 2>/dev/null || true
  kubectl delete namespace "${OPERATOR_NAMESPACE}" --timeout=3m 2>/dev/null || true
  helm uninstall cert-manager -n cert-manager 2>/dev/null || true
  kubectl delete namespace cert-manager --timeout=3m 2>/dev/null || true

  warn "CRDs are left in place on purpose (deleting them would delete any FlinkDeployment in any namespace)."
  info "To remove them too: kubectl get crd -o name | grep flink.apache.org | xargs kubectl delete"
fi

ok "done"
