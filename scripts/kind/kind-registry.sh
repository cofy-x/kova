#!/usr/bin/env bash

set -euo pipefail

source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

REGISTRY_NAME=${REGISTRY_NAME:-kind-registry}
REGISTRY_IMAGE=${REGISTRY_IMAGE:-registry:2@sha256:a3d8aaa63ed8681a604f1dea0aa03f100d5895b6a58ace528858a7b332415373}
REGISTRY_PORT=${REGISTRY_PORT:-5002}

require_cmd docker

if ! docker inspect "${REGISTRY_NAME}" >/dev/null 2>&1; then
  docker run -d --restart=always \
    -p "127.0.0.1:${REGISTRY_PORT}:5000" \
    --name "${REGISTRY_NAME}" \
    "${REGISTRY_IMAGE}"
else
  existing_image=$(docker inspect -f '{{.Config.Image}}' "${REGISTRY_NAME}")
  if [[ "${existing_image}" != "${REGISTRY_IMAGE}" ]]; then
    echo "error: registry ${REGISTRY_NAME} uses ${existing_image}; expected ${REGISTRY_IMAGE}" >&2
    echo "Run 'make clean-kind' before recreating the local environment." >&2
    exit 1
  fi
  state=$(docker inspect -f '{{.State.Running}}' "${REGISTRY_NAME}")
  if [[ "${state}" != "true" ]]; then
    docker start "${REGISTRY_NAME}" >/dev/null
  fi
fi

wait_for_tcp 127.0.0.1 "${REGISTRY_PORT}" 20
