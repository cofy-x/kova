#!/usr/bin/env bash

set -euo pipefail

source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

ROOT=$(repo_root)
RUNNER_IMAGE=${RUNNER_IMAGE:-localhost:5002/kova:runner-dev}
CURL_IMAGE=${CURL_IMAGE:-docker.io/curlimages/curl:8.16.0@sha256:463eaf6072688fe96ac64fa623fe73e1dbe25d8ad6c34404a669ad3ce1f104b6}
BUILDKIT_ADDR=${BUILDKIT_ADDR:-tcp://kova.kova.svc:9094}
KIND_CLUSTER=${KIND_CLUSTER:-kova-local}
KIND_KUBECONFIG=${KIND_KUBECONFIG:-.kind/${KIND_CLUSTER}.kubeconfig}
RUNNER_NAME=${RUNNER_NAME:-e2e-runtime}
WORK_DIR=${WORK_DIR:-.work}
OCI_SOURCE_ZIP=${OCI_SOURCE_ZIP:-${WORK_DIR}/source-runtime-oci.zip}
NYDUS_SOURCE_ZIP=${NYDUS_SOURCE_ZIP:-${WORK_DIR}/source-runtime-nydus.zip}
OCI_RESULT_JSONL=${OCI_RESULT_JSONL:-${WORK_DIR}/result-runtime-oci.jsonl}
NYDUS_RESULT_JSONL=${NYDUS_RESULT_JSONL:-${WORK_DIR}/result-runtime-nydus.jsonl}
CLUSTER_REGISTRY=${CLUSTER_REGISTRY:-host.docker.internal:5002}
DRAGONFLY_NAMESPACE=${DRAGONFLY_NAMESPACE:-dragonfly-system}
DRAGONFLY_RELEASE_NAME=${DRAGONFLY_RELEASE_NAME:-dragonfly}
DRAGONFLY_SCHEDULER_ADDR=${DRAGONFLY_SCHEDULER_ADDR:-${DRAGONFLY_RELEASE_NAME}-scheduler.${DRAGONFLY_NAMESPACE}.svc.cluster.local:8002}
RUNTIME_NAMESPACE=${RUNTIME_NAMESPACE:-kova-runtime-test}
OCI_SERVICE_NAME=${OCI_SERVICE_NAME:-kova-service-oci}
NYDUS_SERVICE_NAME=${NYDUS_SERVICE_NAME:-kova-service-nydus}
OCI_SERVICE_IMAGE=${OCI_SERVICE_IMAGE:-${CLUSTER_REGISTRY}/kova-examples/service-oci:dev}
NYDUS_SERVICE_IMAGE=${NYDUS_SERVICE_IMAGE:-${CLUSTER_REGISTRY}/kova-examples/service-nydus:dev_nydus_v3}

require_cmd docker
require_cmd kubectl

"${ROOT}/scripts/e2e/e2e-runtime-preflight.sh"

make -C "${ROOT}" kova
make -C "${ROOT}" image
"${ROOT}/scripts/build/build-python-smoke-base.sh"
KOVA=("${ROOT}/bin/kova")
KUBECONFIG="${ROOT}/${KIND_KUBECONFIG}"
export KUBECONFIG

dump_debug() {
  echo "---- runtime pods ----" >&2
  kubectl get pods -n "${RUNTIME_NAMESPACE}" -o wide >&2 || true
  echo "---- runtime events ----" >&2
  kubectl get events -n "${RUNTIME_NAMESPACE}" --sort-by=.lastTimestamp >&2 || true
  echo "---- runner logs ----" >&2
  "${KOVA[@]}" --kubeconfig "${KUBECONFIG}" --name "${RUNNER_NAME}" logs --tail=120 >&2 || true
}

trap 'dump_debug' ERR

"${ROOT}/scripts/kind/deploy-kind.sh"
"${ROOT}/scripts/runtime/dragonfly-nydus-install.sh"

KOVA_IMAGE_PULL_SECRET='' "${KOVA[@]}" \
  --kubeconfig "${KUBECONFIG}" \
  --name "${RUNNER_NAME}" \
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
  --name "${RUNNER_NAME}" \
  prepare \
  --image "${RUNNER_IMAGE}" \
  --image-pull-policy IfNotPresent \
  --image-pull-secret ""

SOURCE_ZIP="${OCI_SOURCE_ZIP}" EXAMPLE_DIRS=service-oci "${ROOT}/scripts/package/package-example.sh"
"${KOVA[@]}" \
  --kubeconfig "${KUBECONFIG}" \
  --buildkit-addr "${BUILDKIT_ADDR}" \
  --name "${RUNNER_NAME}" \
  build --format oci --concurrency 1 --timeout 600 --fail-fast --verbose \
  --var "KOVA_IMAGE_REGISTRY=${CLUSTER_REGISTRY}" \
  < "${ROOT}/${OCI_SOURCE_ZIP}"

"${KOVA[@]}" --kubeconfig "${KUBECONFIG}" --name "${RUNNER_NAME}" wait --timeout 600
"${KOVA[@]}" --kubeconfig "${KUBECONFIG}" --name "${RUNNER_NAME}" export --result "${ROOT}/${OCI_RESULT_JSONL}" --oci
"${KOVA[@]}" --kubeconfig "${KUBECONFIG}" --name "${RUNNER_NAME}" preheat --oci --dragonfly-scheduler-addr "${DRAGONFLY_SCHEDULER_ADDR}" --concurrency 1 --timeout 60 --verbose --insecure-skip-verify

SOURCE_ZIP="${NYDUS_SOURCE_ZIP}" EXAMPLE_DIRS=service-nydus "${ROOT}/scripts/package/package-example.sh"
"${KOVA[@]}" \
  --kubeconfig "${KUBECONFIG}" \
  --buildkit-addr "${BUILDKIT_ADDR}" \
  --name "${RUNNER_NAME}" \
  build --concurrency 1 --timeout 600 --fail-fast --verbose \
  --var "KOVA_IMAGE_REGISTRY=${CLUSTER_REGISTRY}" \
  < "${ROOT}/${NYDUS_SOURCE_ZIP}"

"${KOVA[@]}" --kubeconfig "${KUBECONFIG}" --name "${RUNNER_NAME}" wait --timeout 600
"${KOVA[@]}" --kubeconfig "${KUBECONFIG}" --name "${RUNNER_NAME}" export --result "${ROOT}/${NYDUS_RESULT_JSONL}"
"${KOVA[@]}" --kubeconfig "${KUBECONFIG}" --name "${RUNNER_NAME}" preheat --dragonfly-scheduler-addr "${DRAGONFLY_SCHEDULER_ADDR}" --concurrency 1 --timeout 60 --verbose --insecure-skip-verify

if ! grep -q '"success":true' "${ROOT}/${OCI_RESULT_JSONL}"; then
  echo "error: ${OCI_RESULT_JSONL} does not contain a successful OCI build" >&2
  exit 1
fi
if ! grep -q '"success":true' "${ROOT}/${NYDUS_RESULT_JSONL}"; then
  echo "error: ${NYDUS_RESULT_JSONL} does not contain a successful Nydus build" >&2
  exit 1
fi
if ! grep -q '_nydus_v3' "${ROOT}/${NYDUS_RESULT_JSONL}"; then
  echo "error: ${NYDUS_RESULT_JSONL} does not contain a Nydus target" >&2
  exit 1
fi

kubectl create namespace "${RUNTIME_NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

deploy_python_service() {
  local name=$1
  local image=$2
  local format=$3
  kubectl delete deployment "${name}" -n "${RUNTIME_NAMESPACE}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl delete service "${name}" -n "${RUNTIME_NAMESPACE}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${name}
  namespace: ${RUNTIME_NAMESPACE}
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: ${name}
  template:
    metadata:
      labels:
        app.kubernetes.io/name: ${name}
    spec:
      containers:
        - name: app
          image: ${image}
          imagePullPolicy: Always
          env:
            - name: KOVA_IMAGE_FORMAT
              value: ${format}
          ports:
            - name: http
              containerPort: 8080
---
apiVersion: v1
kind: Service
metadata:
  name: ${name}
  namespace: ${RUNTIME_NAMESPACE}
spec:
  selector:
    app.kubernetes.io/name: ${name}
  ports:
    - name: http
      port: 8080
      targetPort: http
EOF
  kubectl rollout status deployment/"${name}" -n "${RUNTIME_NAMESPACE}" --timeout=240s
}

probe_python_service() {
  local name=$1
  local format=$2
  local job="${name}-probe"
  kubectl delete job "${job}" -n "${RUNTIME_NAMESPACE}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl apply -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: ${job}
  namespace: ${RUNTIME_NAMESPACE}
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: probe
          image: ${CURL_IMAGE}
          imagePullPolicy: IfNotPresent
          command:
            - /bin/sh
            - -ec
            - |
              i=0
              while [ "\$i" -lt 60 ]; do
                if curl -fsS http://${name}.${RUNTIME_NAMESPACE}.svc:8080/healthz >/tmp/response 2>/tmp/error; then
                  cat /tmp/response
                  grep -q '"service":"kova-runtime-smoke"' /tmp/response
                  grep -q '"format":"${format}"' /tmp/response
                  grep -q '"ok":true' /tmp/response
                  exit 0
                fi
                cat /tmp/error || true
                sleep 2
                i=\$((i + 1))
              done
              exit 1
EOF
  kubectl wait --for=condition=Complete job/"${job}" -n "${RUNTIME_NAMESPACE}" --timeout=180s
  kubectl logs job/"${job}" -n "${RUNTIME_NAMESPACE}"
}

deploy_python_service "${OCI_SERVICE_NAME}" "${OCI_SERVICE_IMAGE}" "oci"
probe_python_service "${OCI_SERVICE_NAME}" "oci"

deploy_python_service "${NYDUS_SERVICE_NAME}" "${NYDUS_SERVICE_IMAGE}" "nydus"
probe_python_service "${NYDUS_SERVICE_NAME}" "nydus"
