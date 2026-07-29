#!/usr/bin/env bash

set -euo pipefail

source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

ROOT=$(repo_root)
CONTROLLER_IMAGE=${CONTROLLER_IMAGE:-localhost:5002/kova:controller-dev}
RUNNER_IMAGE=${RUNNER_IMAGE:-localhost:5002/kova:runner-dev}
WORKER_IMAGE=${WORKER_IMAGE:-localhost:5002/kova:worker-dev}
KIND_KUBECONFIG=${KIND_KUBECONFIG:-.kind/kova-local.kubeconfig}
RELEASE_NAME=${RELEASE_NAME:-kova}
NAMESPACE=${NAMESPACE:-kova}
KOVA_CHART=${KOVA_CHART:-${ROOT}/charts/kova}
KIND_VALUES=${KIND_VALUES:-${ROOT}/deploy/kind-values.yaml}
KIND_LOAD_IMAGES=${KIND_LOAD_IMAGES:-true}
START_OBSERVABILITY=${START_OBSERVABILITY:-true}

if [[ "${KOVA_CHART}" != /* ]]; then
  KOVA_CHART=${ROOT}/${KOVA_CHART}
fi
if [[ "${KIND_VALUES}" != /* ]]; then
  KIND_VALUES=${ROOT}/${KIND_VALUES}
fi
if [[ "${KIND_KUBECONFIG}" != /* ]]; then
  KIND_KUBECONFIG=${ROOT}/${KIND_KUBECONFIG}
fi

require_cmd helm

if [[ "${START_OBSERVABILITY}" == "true" ]]; then
  "${ROOT}/scripts/observability/local-up.sh"
fi
if [[ "${KIND_LOAD_IMAGES}" == "true" ]]; then
  "${ROOT}/scripts/kind/kind-load.sh"
else
  "${ROOT}/scripts/kind/kind-create.sh"
fi

helm upgrade --install "${RELEASE_NAME}" "${KOVA_CHART}" \
  --kubeconfig "${KIND_KUBECONFIG}" \
  --namespace "${NAMESPACE}" \
  --create-namespace \
  --wait \
  --timeout 180s \
  -f "${KIND_VALUES}" \
  --set-string "images.controller.repository=${CONTROLLER_IMAGE%:*}" \
  --set-string "images.controller.tag=${CONTROLLER_IMAGE##*:}" \
  --set-string "images.runner.repository=${RUNNER_IMAGE%:*}" \
  --set-string "images.runner.tag=${RUNNER_IMAGE##*:}" \
  --set-string "images.worker.repository=${WORKER_IMAGE%:*}" \
  --set-string "images.worker.tag=${WORKER_IMAGE##*:}"
