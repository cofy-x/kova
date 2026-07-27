#!/usr/bin/env bash

set -euo pipefail

source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

REGISTRY_NAME=${REGISTRY_NAME:-kind-registry}
REGISTRY_IMAGE=${REGISTRY_IMAGE:-registry:2}
REGISTRY_PORT=${REGISTRY_PORT:-5002}

require_cmd docker

if ! docker inspect "${REGISTRY_NAME}" >/dev/null 2>&1; then
  docker run -d --restart=always \
    -p "127.0.0.1:${REGISTRY_PORT}:5000" \
    --name "${REGISTRY_NAME}" \
    "${REGISTRY_IMAGE}"
else
  state=$(docker inspect -f '{{.State.Running}}' "${REGISTRY_NAME}")
  if [[ "${state}" != "true" ]]; then
    docker start "${REGISTRY_NAME}" >/dev/null
  fi
fi

wait_for_tcp 127.0.0.1 "${REGISTRY_PORT}" 20
