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

require_cmd helm

"${ROOT}/scripts/observability/local-up.sh"
"${ROOT}/scripts/kind/kind-load.sh"

helm upgrade --install "${RELEASE_NAME}" "${ROOT}/charts/kova" \
  --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" \
  --namespace "${NAMESPACE}" \
  --create-namespace \
  --wait \
  --timeout 180s \
  -f "${ROOT}/deploy/kind-values.yaml" \
  --set-string "images.controller.repository=${CONTROLLER_IMAGE%:*}" \
  --set-string "images.controller.tag=${CONTROLLER_IMAGE##*:}" \
  --set-string "images.runner.repository=${RUNNER_IMAGE%:*}" \
  --set-string "images.runner.tag=${RUNNER_IMAGE##*:}" \
  --set-string "images.worker.repository=${WORKER_IMAGE%:*}" \
  --set-string "images.worker.tag=${WORKER_IMAGE##*:}"
