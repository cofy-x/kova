#!/usr/bin/env bash

set -euo pipefail

source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

ROOT=$(repo_root)
IMAGE=${IMAGE:-localhost:5002/kova:dev}
IMAGE_PLATFORM=${IMAGE_PLATFORM:-linux/$(docker_arch)}
VERSION=${VERSION:-dev}
COMMIT=${COMMIT:-$(git -C "${ROOT}" rev-parse --short=12 HEAD 2>/dev/null || printf unknown)}
BUILD_DATE=${BUILD_DATE:-unknown}
NO_PROXY_DEFAULT=${NO_PROXY:-${no_proxy:-localhost,127.0.0.1,kind-registry,kind-registry:5000,*.svc,*.cluster.local}}

require_cmd docker

build_args=(
  --platform "${IMAGE_PLATFORM}"
  --build-arg "NO_PROXY=${NO_PROXY_DEFAULT}"
  --build-arg "no_proxy=${NO_PROXY_DEFAULT}"
  --build-arg "VERSION=${VERSION}"
  --build-arg "COMMIT=${COMMIT}"
  --build-arg "BUILD_DATE=${BUILD_DATE}"
  -t "${IMAGE}"
  -f "${ROOT}/docker/Dockerfile"
)

optional_build_args=(
  UBUNTU_IMAGE
  BUILDKIT_DOWNLOAD_BASE_URL
  GRPCURL_DOWNLOAD_BASE_URL
  NYDUS_DOWNLOAD_BASE_URL
  GO_DOWNLOAD_BASE_URL
  GOPROXY
)
for name in "${optional_build_args[@]}"; do
  if [[ -n "${!name:-}" ]]; then
    build_args+=(--build-arg "${name}=${!name}")
  fi
done

http_proxy_url=${HTTP_PROXY:-${http_proxy:-}}
https_proxy_url=${HTTPS_PROXY:-${https_proxy:-}}
if [[ -z "${http_proxy_url}" && -z "${https_proxy_url}" ]]; then
  detected_proxy=$(detect_host_http_proxy || true)
  http_proxy_url=${detected_proxy}
  https_proxy_url=${detected_proxy}
fi

if [[ -n "${http_proxy_url}" ]]; then
  http_proxy_url=${http_proxy_url/127.0.0.1/host.docker.internal}
  http_proxy_url=${http_proxy_url/localhost/host.docker.internal}
  echo "Using Docker HTTP build proxy: ${http_proxy_url}" >&2
  build_args+=(
    --build-arg "HTTP_PROXY=${http_proxy_url}"
    --build-arg "http_proxy=${http_proxy_url}"
  )
fi

if [[ -n "${https_proxy_url}" ]]; then
  https_proxy_url=${https_proxy_url/127.0.0.1/host.docker.internal}
  https_proxy_url=${https_proxy_url/localhost/host.docker.internal}
  echo "Using Docker HTTPS build proxy: ${https_proxy_url}" >&2
  build_args+=(
    --build-arg "HTTPS_PROXY=${https_proxy_url}"
    --build-arg "https_proxy=${https_proxy_url}"
  )
fi

docker build "${build_args[@]}" "${ROOT}"
