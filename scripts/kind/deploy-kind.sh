#!/usr/bin/env bash

set -euo pipefail

source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

ROOT=$(repo_root)
IMAGE=${IMAGE:-localhost:5002/kova:dev}
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
  --set "imageOverride=${IMAGE}"
