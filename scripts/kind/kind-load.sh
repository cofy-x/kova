#!/usr/bin/env bash

set -euo pipefail

source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

ROOT=$(repo_root)
CONTROLLER_IMAGE=${CONTROLLER_IMAGE:-localhost:5002/kova:controller-dev}
RUNNER_IMAGE=${RUNNER_IMAGE:-localhost:5002/kova:runner-dev}
WORKER_IMAGE=${WORKER_IMAGE:-localhost:5002/kova:worker-dev}
KIND_CLUSTER=${KIND_CLUSTER:-kova-local}

require_cmd docker
require_kind

"${ROOT}/scripts/kind/kind-create.sh"

for image in "${CONTROLLER_IMAGE}" "${RUNNER_IMAGE}" "${WORKER_IMAGE}"; do
  docker push "${image}"
  kind load docker-image "${image}" --name "${KIND_CLUSTER}"
done
