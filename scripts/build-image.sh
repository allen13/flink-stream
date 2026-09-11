#!/usr/bin/env bash
#
# Builds the job image on top of Confluent's Flink distribution.
#
# The Maven build happens *inside* the image, so no local Maven or JDK is needed. OrbStack shares its Docker
# image store with its Kubernetes node, so the resulting tag is immediately usable by the chart with
# imagePullPolicy: IfNotPresent and no registry involved.

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

require docker

step "Building ${IMAGE}"
info "base image: $(grep -m1 'ARG CP_FLINK_IMAGE' "${REPO_ROOT}/flink-jobs/Dockerfile" | cut -d= -f2)"

docker build \
  --platform "${PLATFORM:-linux/$(uname -m | sed 's/x86_64/amd64/; s/aarch64/arm64/')}" \
  -t "${IMAGE}" \
  -f "${REPO_ROOT}/flink-jobs/Dockerfile" \
  "${REPO_ROOT}/flink-jobs" \
  "$@"

step "Image contents"
docker run --rm --entrypoint sh "${IMAGE}" -c '
  echo "job jar:"; ls -la /opt/flink/usrlib
  echo; echo "connectors added to /opt/flink/lib:"
  ls /opt/flink/lib | grep -E "jdbc|postgres|iceberg|hadoop" || true
'

ok "Built ${IMAGE}"
