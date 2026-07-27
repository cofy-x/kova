#!/usr/bin/env bash

set -euo pipefail

source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

ROOT=$(repo_root)
REGISTRY_HOST=${REGISTRY_HOST:-localhost:5002}
PYTHON_SMOKE_BASE_IMAGE=${PYTHON_SMOKE_BASE_IMAGE:-${REGISTRY_HOST}/kova-examples/python-smoke-base:dev}
IMAGE_PLATFORM=${IMAGE_PLATFORM:-linux/$(docker_arch)}
NO_PROXY_DEFAULT=${NO_PROXY:-localhost,127.0.0.1,kind-registry,kind-registry:5000,*.svc,*.cluster.local}

require_cmd docker

build_args=(
  --platform "${IMAGE_PLATFORM}"
  --build-arg "NO_PROXY=${NO_PROXY_DEFAULT}"
  --build-arg "no_proxy=${NO_PROXY_DEFAULT}"
  -t "${PYTHON_SMOKE_BASE_IMAGE}"
  -f "${ROOT}/docker/python-smoke-base.Dockerfile"
)

proxy_url=${HTTP_PROXY:-${http_proxy:-}}
if [[ -z "${proxy_url}" ]]; then
  proxy_url=$(detect_host_http_proxy || true)
fi

if [[ -n "${proxy_url}" ]]; then
  proxy_url=${proxy_url/127.0.0.1/host.docker.internal}
  proxy_url=${proxy_url/localhost/host.docker.internal}
  echo "Using Docker build proxy: ${proxy_url}" >&2
  build_args+=(
    --build-arg "HTTP_PROXY=${proxy_url}"
    --build-arg "HTTPS_PROXY=${proxy_url}"
    --build-arg "http_proxy=${proxy_url}"
    --build-arg "https_proxy=${proxy_url}"
  )
fi

docker build "${build_args[@]}" "${ROOT}"
docker push "${PYTHON_SMOKE_BASE_IMAGE}"
