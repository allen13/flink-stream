#!/usr/bin/env bash
#
# One-time cluster prerequisites: cert-manager, then the Confluent Flink Kubernetes Operator.
#
# Safe to re-run - every step is an idempotent `helm upgrade --install`.

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

require kubectl helm
require_local_cluster

step "Cluster"
kubectl get nodes -o wide

step "cert-manager ${CERT_MANAGER_VERSION}"
# The Flink operator's admission webhook is served over TLS, and its MutatingWebhookConfiguration carries a
# cert-manager.io/inject-ca-from annotation. Without cert-manager the webhook has no certificate, the API server
# refuses to call it, and every FlinkDeployment create is rejected.
#
# Alternative if you would rather not run cert-manager: install the operator with --set webhook.create=false.
# The operator still works; you lose CR defaulting and validation at admission time (errors surface later, in
# the operator log, instead of at kubectl apply).
helm repo add jetstack https://charts.jetstack.io >/dev/null 2>&1 || true
helm repo update jetstack >/dev/null
helm upgrade --install cert-manager jetstack/cert-manager \
  --namespace cert-manager --create-namespace \
  --version "${CERT_MANAGER_VERSION}" \
  --set crds.enabled=true \
  --set global.leaderElection.namespace=cert-manager \
  --wait --timeout 5m
ok "cert-manager ready"

step "Confluent Flink Kubernetes Operator (chart ${FLINK_OPERATOR_CHART_VERSION})"
helm repo add confluentinc "${CONFLUENT_HELM_REPO}" >/dev/null 2>&1 || true
helm repo update confluentinc >/dev/null

# Cluster-scoped on purpose (watchNamespaces left empty): one operator reconciles FlinkDeployments in any
# namespace, which is how it is normally run. Restricting it is a --set away.
#
# The chart's default request is 3 GiB, which is most of a laptop's Kubernetes VM for a process that only
# reconciles custom resources. 1 GiB is comfortable for a handful of FlinkDeployments; raise it if you run
# dozens, since the operator holds the job graph of every deployment it watches.
helm upgrade --install flink-kubernetes-operator confluentinc/flink-kubernetes-operator \
  --namespace "${OPERATOR_NAMESPACE}" --create-namespace \
  --version "${FLINK_OPERATOR_CHART_VERSION}" \
  --set webhook.create=true \
  --set operatorPod.resources.requests.cpu=200m \
  --set operatorPod.resources.requests.memory=1Gi \
  --set operatorPod.resources.limits.memory=1536Mi \
  --wait --timeout 5m

step "Verify"
kubectl -n "${OPERATOR_NAMESPACE}" get pods
echo
info "Custom resources the operator now understands:"
kubectl get crd -o name | grep flink.apache.org | sed 's/^/      /'
echo
info "Operator image (Confluent's build of the Apache Flink operator):"
kubectl -n "${OPERATOR_NAMESPACE}" get deploy flink-kubernetes-operator \
  -o jsonpath='{.spec.template.spec.containers[*].image}{"\n"}' | sed 's/^/      /'

ok "Prerequisites installed. Next: scripts/build-image.sh"
