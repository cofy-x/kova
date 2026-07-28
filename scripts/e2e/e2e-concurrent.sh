#!/usr/bin/env bash

set -euo pipefail

source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

ROOT=$(repo_root)
RUNNER_IMAGE=${RUNNER_IMAGE:-localhost:5002/kova:runner-dev}
BUILDKIT_ADDR=${BUILDKIT_ADDR:-tcp://kova.kova.svc:9094}
KIND_KUBECONFIG=${KIND_KUBECONFIG:-.kind/kova-local.kubeconfig}
NAMESPACE=${NAMESPACE:-kova}
RUNNER_NAMESPACE=${KOVA_NAMESPACE:-default}
KOVA_RUNNER_NAME=${KOVA_RUNNER_NAME:-e2e-concurrent}
WORK_DIR=${WORK_DIR:-.work}
SOURCE_ZIP=${SOURCE_ZIP:-${WORK_DIR}/source-concurrent.zip}
RESULT_JSONL=${RESULT_JSONL:-${WORK_DIR}/result-concurrent.jsonl}
CLUSTER_REGISTRY=${CLUSTER_REGISTRY:-kind-registry:5000}
REGISTRY_HOST=${REGISTRY_HOST:-localhost:5002}
EXAMPLE_COUNT=${EXAMPLE_COUNT:-12}
BUILD_CONCURRENCY=${BUILD_CONCURRENCY:-4}
MIN_BUILDKIT_NODE_IPS=${MIN_BUILDKIT_NODE_IPS:-2}

if ! [[ "${EXAMPLE_COUNT}" =~ ^[0-9]+$ ]] || [[ "${EXAMPLE_COUNT}" -lt 1 ]]; then
  echo "error: EXAMPLE_COUNT must be a positive integer" >&2
  exit 1
fi
if ! [[ "${BUILD_CONCURRENCY}" =~ ^[0-9]+$ ]] || [[ "${BUILD_CONCURRENCY}" -lt 1 ]]; then
  echo "error: BUILD_CONCURRENCY must be a positive integer" >&2
  exit 1
fi
if ! [[ "${MIN_BUILDKIT_NODE_IPS}" =~ ^[0-9]+$ ]]; then
  echo "error: MIN_BUILDKIT_NODE_IPS must be a non-negative integer" >&2
  exit 1
fi

make -C "${ROOT}" kova
KOVA=("${ROOT}/bin/kova")

dump_debug() {
  echo "---- runner pod ----" >&2
  "${KOVA[@]}" --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" --namespace "${RUNNER_NAMESPACE}" list -o wide >&2 || true
  "${KOVA[@]}" --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" --namespace "${RUNNER_NAMESPACE}" --name "${KOVA_RUNNER_NAME}" logs --tail=100 >&2 || true
  echo "---- buildkit pods ----" >&2
  "${KOVA[@]}" --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" --namespace "${NAMESPACE}" list -o wide >&2 || true
  if [[ -f "${ROOT}/${RESULT_JSONL}" ]]; then
    echo "---- ${RESULT_JSONL} ----" >&2
    sed -n '1,20p' "${ROOT}/${RESULT_JSONL}" >&2 || true
  fi
}

trap 'dump_debug' ERR

"${ROOT}/scripts/kind/deploy-kind.sh"
SOURCE_ZIP="${SOURCE_ZIP}" EXAMPLE_COUNT="${EXAMPLE_COUNT}" "${ROOT}/scripts/package/package-concurrent-example.sh"

KOVA_IMAGE_PULL_SECRET='' "${KOVA[@]}" \
  --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" \
  --name "${KOVA_RUNNER_NAME}" \
  destroy || true

KOVA_DAEMON_OTEL_ENABLED=${KOVA_DAEMON_OTEL_ENABLED:-true} \
KOVA_DAEMON_OTEL_METRIC_INTERVAL=${KOVA_DAEMON_OTEL_METRIC_INTERVAL:-5s} \
KOVA_DAEMON_OTEL_SERVICE_NAME=${KOVA_DAEMON_OTEL_SERVICE_NAME:-kova-runner} \
KOVA_DAEMON_OTEL_EXPORTER_OTLP_ENDPOINT=${KOVA_DAEMON_OTEL_EXPORTER_OTLP_ENDPOINT:-host.docker.internal:${KOVA_LOCAL_OTLP_GRPC_PORT:-14317}} \
KOVA_DAEMON_OTEL_EXPORTER_OTLP_INSECURE=${KOVA_DAEMON_OTEL_EXPORTER_OTLP_INSECURE:-true} \
KOVA_DAEMON_OTEL_RESOURCE_ATTRIBUTES=${KOVA_DAEMON_OTEL_RESOURCE_ATTRIBUTES:-deployment.environment=kind} \
"${KOVA[@]}" \
  --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" \
  --buildkit-addr "${BUILDKIT_ADDR}" \
  --name "${KOVA_RUNNER_NAME}" \
  prepare \
  --image "${RUNNER_IMAGE}" \
  --image-pull-policy IfNotPresent \
  --image-pull-secret ""

"${KOVA[@]}" \
  --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" \
  --buildkit-addr "${BUILDKIT_ADDR}" \
  --name "${KOVA_RUNNER_NAME}" \
  build --format oci --concurrency "${BUILD_CONCURRENCY}" --timeout 600 --fail-fast --verbose \
  --var "KOVA_IMAGE_REGISTRY=${CLUSTER_REGISTRY}" \
  < "${ROOT}/${SOURCE_ZIP}"

"${KOVA[@]}" \
  --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" \
  --name "${KOVA_RUNNER_NAME}" \
  wait --timeout 600

"${KOVA[@]}" \
  --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" \
  --name "${KOVA_RUNNER_NAME}" \
  export --result "${ROOT}/${RESULT_JSONL}" --oci

line_count=$(wc -l < "${ROOT}/${RESULT_JSONL}" | tr -d ' ')
if [[ "${line_count}" -ne "${EXAMPLE_COUNT}" ]]; then
  echo "error: ${RESULT_JSONL} has ${line_count} line(s), expected ${EXAMPLE_COUNT}" >&2
  exit 1
fi

if grep -qv '"success":true' "${ROOT}/${RESULT_JSONL}"; then
  echo "error: ${RESULT_JSONL} contains failed entries" >&2
  exit 1
fi

node_ip_count=$(sed -n 's/.*"node_ip":"\([^"]*\)".*/\1/p' "${ROOT}/${RESULT_JSONL}" | sort -u | wc -l | tr -d ' ')
if [[ "${MIN_BUILDKIT_NODE_IPS}" -gt 0 && "${node_ip_count}" -lt "${MIN_BUILDKIT_NODE_IPS}" ]]; then
  echo "error: ${RESULT_JSONL} used ${node_ip_count} BuildKit node IP(s), expected at least ${MIN_BUILDKIT_NODE_IPS}" >&2
  exit 1
fi

for i in $(seq 1 "${EXAMPLE_COUNT}"); do
  target=$(printf "concurrent-%02d" "${i}")
  docker pull "${REGISTRY_HOST}/kova-examples/${target}:dev"
done
