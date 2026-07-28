#!/usr/bin/env bash

set -euo pipefail

source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

ROOT=$(repo_root)
RUNNER_IMAGE=${RUNNER_IMAGE:-localhost:5002/kova:runner-dev}
BUILDKIT_ADDR=${BUILDKIT_ADDR:-tcp://kova.kova.svc:9094}
KIND_CLUSTER=${KIND_CLUSTER:-kova-local}
KIND_KUBECONFIG=${KIND_KUBECONFIG:-.kind/${KIND_CLUSTER}.kubeconfig}
KOVA_RUNNER_NAME=${KOVA_RUNNER_NAME:-e2e-nydus}
WORK_DIR=${WORK_DIR:-.work}
SOURCE_ZIP=${SOURCE_ZIP:-${WORK_DIR}/source.zip}
RESULT_JSONL=${RESULT_JSONL:-${WORK_DIR}/result-nydus.jsonl}
CLUSTER_REGISTRY=${CLUSTER_REGISTRY:-kind-registry:5000}
REGISTRY_HOST=${REGISTRY_HOST:-localhost:5002}
NYDUS_TEST_IMAGE=${NYDUS_TEST_IMAGE:-${CLUSTER_REGISTRY}/kova-examples/nydus-smoke:dev_nydus_v3}
DRAGONFLY_NAMESPACE=${DRAGONFLY_NAMESPACE:-dragonfly-system}
DRAGONFLY_RELEASE_NAME=${DRAGONFLY_RELEASE_NAME:-dragonfly}
NYDUS_RELEASE_NAME=${NYDUS_RELEASE_NAME:-nydus}
DRAGONFLY_SCHEDULER_ADDR=${DRAGONFLY_SCHEDULER_ADDR:-${DRAGONFLY_RELEASE_NAME}-scheduler.${DRAGONFLY_NAMESPACE}.svc.cluster.local:8002}
NYDUS_TEST_NAMESPACE=${NYDUS_TEST_NAMESPACE:-kova-nydus-test}
NYDUS_TEST_POD=${NYDUS_TEST_POD:-kova-nydus-smoke}

require_cmd docker
require_cmd kubectl

make -C "${ROOT}" kova
KOVA=("${ROOT}/bin/kova")
KUBECONFIG="${ROOT}/${KIND_KUBECONFIG}"
export KUBECONFIG

"${ROOT}/scripts/kind/deploy-kind.sh"
"${ROOT}/scripts/runtime/dragonfly-nydus-install.sh"
EXAMPLE_DIRS=nydus-smoke "${ROOT}/scripts/package/package-example.sh"

KOVA_IMAGE_PULL_SECRET='' "${KOVA[@]}" \
  --kubeconfig "${KUBECONFIG}" \
  --name "${KOVA_RUNNER_NAME}" \
  destroy || true

KOVA_DAEMON_OTEL_ENABLED=${KOVA_DAEMON_OTEL_ENABLED:-true} \
KOVA_DAEMON_OTEL_METRIC_INTERVAL=${KOVA_DAEMON_OTEL_METRIC_INTERVAL:-5s} \
KOVA_DAEMON_OTEL_SERVICE_NAME=${KOVA_DAEMON_OTEL_SERVICE_NAME:-kova-runner} \
KOVA_DAEMON_OTEL_EXPORTER_OTLP_ENDPOINT=${KOVA_DAEMON_OTEL_EXPORTER_OTLP_ENDPOINT:-host.docker.internal:${KOVA_LOCAL_OTLP_GRPC_PORT:-14317}} \
KOVA_DAEMON_OTEL_EXPORTER_OTLP_INSECURE=${KOVA_DAEMON_OTEL_EXPORTER_OTLP_INSECURE:-true} \
KOVA_DAEMON_OTEL_RESOURCE_ATTRIBUTES=${KOVA_DAEMON_OTEL_RESOURCE_ATTRIBUTES:-deployment.environment=kind} \
"${KOVA[@]}" \
  --kubeconfig "${KUBECONFIG}" \
  --buildkit-addr "${BUILDKIT_ADDR}" \
  --name "${KOVA_RUNNER_NAME}" \
  prepare \
  --image "${RUNNER_IMAGE}" \
  --image-pull-policy IfNotPresent \
  --image-pull-secret ""

"${KOVA[@]}" \
  --kubeconfig "${KUBECONFIG}" \
  --buildkit-addr "${BUILDKIT_ADDR}" \
  --name "${KOVA_RUNNER_NAME}" \
  build --concurrency 1 --timeout 600 --fail-fast --verbose \
  --var "KOVA_IMAGE_REGISTRY=${CLUSTER_REGISTRY}" \
  < "${ROOT}/${SOURCE_ZIP}"

"${KOVA[@]}" \
  --kubeconfig "${KUBECONFIG}" \
  --name "${KOVA_RUNNER_NAME}" \
  wait --timeout 600

"${KOVA[@]}" \
  --kubeconfig "${KUBECONFIG}" \
  --name "${KOVA_RUNNER_NAME}" \
  export --result "${ROOT}/${RESULT_JSONL}"

if ! grep -q '"success":true' "${ROOT}/${RESULT_JSONL}"; then
  echo "error: ${RESULT_JSONL} does not contain a successful build" >&2
  exit 1
fi
if ! grep -q '_nydus_v3' "${ROOT}/${RESULT_JSONL}"; then
  echo "error: ${RESULT_JSONL} does not contain a nydus target" >&2
  exit 1
fi

"${KOVA[@]}" \
  --kubeconfig "${KUBECONFIG}" \
  --name "${KOVA_RUNNER_NAME}" \
  preheat --dragonfly-scheduler-addr "${DRAGONFLY_SCHEDULER_ADDR}" --concurrency 1 --timeout 60 --verbose --insecure-skip-verify

for node in $(kind_worker_nodes "${KIND_CLUSTER}"); do
  docker exec "${node}" bash -lc "grep -q 'snapshotter = \"nydus\"' /etc/containerd/config.toml"
  docker exec "${node}" bash -lc "grep -q '^\\[proxy_plugins.nydus\\]\$' /etc/containerd/config.toml"
  docker exec "${node}" bash -lc "test -S /run/containerd-nydus/containerd-nydus-grpc.sock"
done

kubectl create namespace "${NYDUS_TEST_NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -
kubectl delete pod "${NYDUS_TEST_POD}" -n "${NYDUS_TEST_NAMESPACE}" --ignore-not-found >/dev/null 2>&1 || true
kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${NYDUS_TEST_POD}
  namespace: ${NYDUS_TEST_NAMESPACE}
spec:
  restartPolicy: Never
  containers:
    - name: app
      image: ${NYDUS_TEST_IMAGE}
      imagePullPolicy: Always
      command: ["/bin/sh", "-c", "cat /hello.txt && sleep 5"]
EOF

kubectl wait --for=condition=Ready pod/"${NYDUS_TEST_POD}" -n "${NYDUS_TEST_NAMESPACE}" --timeout=240s
kubectl logs pod/"${NYDUS_TEST_POD}" -n "${NYDUS_TEST_NAMESPACE}"
