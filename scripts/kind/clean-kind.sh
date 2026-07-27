#!/usr/bin/env bash

set -euo pipefail

source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

ROOT=$(repo_root)
KIND_CLUSTER=${KIND_CLUSTER:-kova-local}
KIND_KUBECONFIG=${KIND_KUBECONFIG:-.kind/${KIND_CLUSTER}.kubeconfig}
REGISTRY_NAME=${REGISTRY_NAME:-kind-registry}
RELEASE_NAME=${RELEASE_NAME:-kova}
WORK_DIR=${WORK_DIR:-.work}
NAMESPACE=${NAMESPACE:-kova}
RUNNER_NAME=${RUNNER_NAME:-e2e}
CONCURRENT_RUNNER_NAME=${CONCURRENT_RUNNER_NAME:-e2e-concurrent}

if [[ -f "${ROOT}/${KIND_KUBECONFIG}" ]]; then
  make -C "${ROOT}" kova-host
  "${ROOT}/bin/kova" --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" --name "${RUNNER_NAME}" destroy || true
  "${ROOT}/bin/kova" --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" --name "${CONCURRENT_RUNNER_NAME}" destroy || true
  helm uninstall "${RELEASE_NAME}" --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" --namespace "${NAMESPACE}" || true
fi

kind delete cluster --name "${KIND_CLUSTER}" || true
docker rm -f "${REGISTRY_NAME}" || true
rm -rf \
  "${ROOT:?}/.kind" \
  "${ROOT:?}/.generated" \
  "${ROOT:?}/${WORK_DIR:?}"
