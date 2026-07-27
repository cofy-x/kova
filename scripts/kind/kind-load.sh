#!/usr/bin/env bash

set -euo pipefail

source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

ROOT=$(repo_root)
IMAGE=${IMAGE:-localhost:5002/kova:dev}
KIND_CLUSTER=${KIND_CLUSTER:-kova-local}

require_cmd docker
require_cmd kind

"${ROOT}/scripts/kind/kind-create.sh"

docker push "${IMAGE}"
kind load docker-image "${IMAGE}" --name "${KIND_CLUSTER}"
