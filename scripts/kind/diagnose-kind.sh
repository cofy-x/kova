#!/usr/bin/env bash

set -euo pipefail

source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

ROOT=$(repo_root)
KIND_CLUSTER=${KIND_CLUSTER:-kova-local}
KIND_KUBECONFIG=${KIND_KUBECONFIG:-.kind/${KIND_CLUSTER}.kubeconfig}
REGISTRY_NAME=${REGISTRY_NAME:-kind-registry}
REGISTRY_PORT=${REGISTRY_PORT:-5002}
NAMESPACE=${NAMESPACE:-kova}
DRAGONFLY_NAMESPACE=${DRAGONFLY_NAMESPACE:-dragonfly-system}
RUNTIME_NAMESPACE=${RUNTIME_NAMESPACE:-kova-runtime-test}

require_cmd docker
require_kind
require_cmd kubectl

KUBECONFIG="${ROOT}/${KIND_KUBECONFIG}"
export KUBECONFIG

section() {
  printf '\n== %s ==\n' "$1"
}

kubectl_maybe() {
  kubectl "$@" 2>&1 || true
}

section "Local Registry"
docker ps --filter "name=^/${REGISTRY_NAME}$" --format 'name={{.Names}} status={{.Status}} ports={{.Ports}}' || true
if wait_for_tcp 127.0.0.1 "${REGISTRY_PORT}" 2 >/dev/null 2>&1; then
  echo "registry reachable at 127.0.0.1:${REGISTRY_PORT}"
else
  echo "registry not reachable at 127.0.0.1:${REGISTRY_PORT}"
fi

section "Kind Cluster"
if kind get clusters | grep -qx "${KIND_CLUSTER}"; then
  kind get nodes --name "${KIND_CLUSTER}" || true
else
  echo "kind cluster ${KIND_CLUSTER} not found"
fi

section "Kova Pods"
kubectl_maybe get pods -n "${NAMESPACE}" -o wide
kubectl_maybe get deployment -n "${NAMESPACE}" -o wide

section "Dragonfly And Nydus"
kubectl_maybe get pods -n "${DRAGONFLY_NAMESPACE}" -o wide
kubectl_maybe get daemonset -n "${DRAGONFLY_NAMESPACE}" -o wide

section "Runtime Smoke"
kubectl_maybe get pods -n "${RUNTIME_NAMESPACE}" -o wide
kubectl_maybe get service -n "${RUNTIME_NAMESPACE}" -o wide
kubectl_maybe get jobs -n "${RUNTIME_NAMESPACE}" -o wide

section "Recent Kova Events"
kubectl_maybe get events -n "${NAMESPACE}" --sort-by=.lastTimestamp

section "Recent Runner Logs"
runner_pod=$(kubectl get pods -n "${NAMESPACE}" -l app.kubernetes.io/name=kova-runner -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
if [[ -n "${runner_pod}" ]]; then
  kubectl_maybe logs -n "${NAMESPACE}" "${runner_pod}" --tail=120
else
  echo "no kova runner pod found"
fi

section "Result Files"
shopt -s nullglob
for file in "${ROOT}"/.work/result*.jsonl; do
  total=$(grep -c '^' "${file}" || true)
  success=$(grep -c '"success":true' "${file}" || true)
  failed=$(grep -c '"success":false' "${file}" || true)
  printf '%s total=%s success=%s failed=%s\n' "$(basename "${file}")" "${total}" "${success}" "${failed}"
done
