#!/usr/bin/env bash

set -euo pipefail

source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

ROOT=$(repo_root)
IMAGE=${IMAGE:-localhost:5002/kova:dev}
BUILDKIT_VERSION=${BUILDKIT_VERSION:-v0.30.0}
GRPCURL_VERSION=${GRPCURL_VERSION:-v1.9.3}
NYDUS_VERSION=${NYDUS_VERSION:-v2.4.3}
GO_VERSION=${GO_VERSION:-1.25.5}
IMAGE_PLATFORM=${IMAGE_PLATFORM:-linux/$(docker_arch)}
NO_PROXY_DEFAULT=${NO_PROXY:-localhost,127.0.0.1,kind-registry,kind-registry:5000,*.svc,*.cluster.local}

require_cmd docker

build_args=(
  --platform "${IMAGE_PLATFORM}"
  --build-arg "BUILDKIT_VERSION=${BUILDKIT_VERSION}"
  --build-arg "GRPCURL_VERSION=${GRPCURL_VERSION}"
  --build-arg "NYDUS_VERSION=${NYDUS_VERSION}"
  --build-arg "GO_VERSION=${GO_VERSION}"
  --build-arg "NO_PROXY=${NO_PROXY_DEFAULT}"
  --build-arg "no_proxy=${NO_PROXY_DEFAULT}"
  -t "${IMAGE}"
  -f "${ROOT}/docker/Dockerfile"
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
