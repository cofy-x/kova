#!/usr/bin/env bash

set -euo pipefail

source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

ROOT=$(repo_root)
REGISTRY_PORT=${REGISTRY_PORT:-5002}

require_cmd docker
require_cmd helm
require_kind
require_cmd kubectl

if ! docker info >/dev/null 2>&1; then
  echo "error: Docker daemon is not reachable" >&2
  exit 1
fi

"${ROOT}/scripts/kind/kind-registry.sh"
wait_for_tcp 127.0.0.1 "${REGISTRY_PORT}" 20

echo "runtime e2e preflight passed"
