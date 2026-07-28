#!/usr/bin/env bash

set -euo pipefail

source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

ROOT=$(repo_root)
RUNNER_IMAGE=${RUNNER_IMAGE:-localhost:5002/kova:runner-dev}
BUILDKIT_ADDR=${BUILDKIT_ADDR:-tcp://kova.kova.svc:9094}
KIND_KUBECONFIG=${KIND_KUBECONFIG:-.kind/kova-local.kubeconfig}
KOVA_RUNNER_NAME=${KOVA_RUNNER_NAME:-e2e}
NAMESPACE=${NAMESPACE:-kova}
WORK_DIR=${WORK_DIR:-.work}
SOURCE_ZIP=${SOURCE_ZIP:-${WORK_DIR}/source.zip}
RESULT_JSONL=${RESULT_JSONL:-${WORK_DIR}/result.jsonl}
CLUSTER_REGISTRY=${CLUSTER_REGISTRY:-kind-registry:5000}
REGISTRY_HOST=${REGISTRY_HOST:-localhost:5002}
SINGLE_DIR_TARGET=${SINGLE_DIR_TARGET:-${CLUSTER_REGISTRY}/kova-examples/simple-dir:dev}
SINGLE_DIR_PULL_TARGET=${SINGLE_DIR_PULL_TARGET:-${REGISTRY_HOST}/kova-examples/simple-dir:dev}
E2E_BUILD_IMAGE=${E2E_BUILD_IMAGE:-true}

if [[ "${KIND_KUBECONFIG}" == /* ]]; then
  kubeconfig=${KIND_KUBECONFIG}
else
  kubeconfig=${ROOT}/${KIND_KUBECONFIG}
fi

make -C "${ROOT}" kova
if [[ "${E2E_BUILD_IMAGE}" == "true" ]]; then
  make -C "${ROOT}" image
fi
KOVA=("${ROOT}/bin/kova")

dump_debug() {
  echo "---- Kova pods ----" >&2
  kubectl --kubeconfig "${kubeconfig}" get pods -n "${NAMESPACE}" -o wide >&2 || true
  echo "---- Kova events ----" >&2
  kubectl --kubeconfig "${kubeconfig}" get events -n "${NAMESPACE}" \
    --sort-by=.lastTimestamp >&2 || true
  echo "---- runner logs ----" >&2
  "${KOVA[@]}" --kubeconfig "${kubeconfig}" --name "${KOVA_RUNNER_NAME}" \
    logs --tail=200 >&2 || true
}

trap 'dump_debug' ERR

"${ROOT}/scripts/kind/deploy-kind.sh"
"${ROOT}/scripts/package/package-example.sh"

KOVA_IMAGE_PULL_SECRET='' "${KOVA[@]}" \
  --kubeconfig "${kubeconfig}" \
  --name "${KOVA_RUNNER_NAME}" \
  destroy || true

KOVA_DAEMON_OTEL_ENABLED=${KOVA_DAEMON_OTEL_ENABLED:-true} \
KOVA_DAEMON_OTEL_METRIC_INTERVAL=${KOVA_DAEMON_OTEL_METRIC_INTERVAL:-5s} \
KOVA_DAEMON_OTEL_SERVICE_NAME=${KOVA_DAEMON_OTEL_SERVICE_NAME:-kova-runner} \
KOVA_DAEMON_OTEL_EXPORTER_OTLP_ENDPOINT=${KOVA_DAEMON_OTEL_EXPORTER_OTLP_ENDPOINT:-host.docker.internal:${KOVA_LOCAL_OTLP_GRPC_PORT:-14317}} \
KOVA_DAEMON_OTEL_EXPORTER_OTLP_INSECURE=${KOVA_DAEMON_OTEL_EXPORTER_OTLP_INSECURE:-true} \
KOVA_DAEMON_OTEL_RESOURCE_ATTRIBUTES=${KOVA_DAEMON_OTEL_RESOURCE_ATTRIBUTES:-deployment.environment=kind} \
"${KOVA[@]}" \
  --kubeconfig "${kubeconfig}" \
  --buildkit-addr "${BUILDKIT_ADDR}" \
  --name "${KOVA_RUNNER_NAME}" \
  prepare \
  --image "${RUNNER_IMAGE}" \
  --image-pull-policy IfNotPresent \
  --image-pull-secret ""

"${KOVA[@]}" \
  --kubeconfig "${kubeconfig}" \
  --buildkit-addr "${BUILDKIT_ADDR}" \
  --name "${KOVA_RUNNER_NAME}" \
  build --format oci --concurrency 1 --timeout 600 --fail-fast --verbose \
  --var "KOVA_IMAGE_REGISTRY=${CLUSTER_REGISTRY}" \
  < "${ROOT}/${SOURCE_ZIP}"

"${KOVA[@]}" \
  --kubeconfig "${kubeconfig}" \
  --name "${KOVA_RUNNER_NAME}" \
  wait --timeout 600

"${KOVA[@]}" \
  --kubeconfig "${kubeconfig}" \
  --name "${KOVA_RUNNER_NAME}" \
  export --result "${ROOT}/${RESULT_JSONL}" --oci

docker pull "${REGISTRY_HOST}/kova-examples/simple:dev"

"${KOVA[@]}" \
  --kubeconfig "${kubeconfig}" \
  --buildkit-addr "${BUILDKIT_ADDR}" \
  --name "${KOVA_RUNNER_NAME}" \
  build "${ROOT}/examples/simple" \
  --target "${SINGLE_DIR_TARGET}" \
  --format oci --concurrency 1 --timeout 600 --fail-fast --verbose

"${KOVA[@]}" \
  --kubeconfig "${kubeconfig}" \
  --name "${KOVA_RUNNER_NAME}" \
  wait --timeout 600

docker pull "${SINGLE_DIR_PULL_TARGET}"
