#!/usr/bin/env bash

set -euo pipefail

source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

ROOT=$(repo_root)
KOVA_GOOS=${KOVA_GOOS:-linux}
KOVA_GOARCH=${KOVA_GOARCH:-$(docker info --format '{{.Architecture}}' 2>/dev/null | sed -e 's/aarch64/arm64/' -e 's/x86_64/amd64/')}
KOVA_GOARCH=${KOVA_GOARCH:-arm64}
BINARY=${BINARY:-bin/kova}
NO_PROXY_DEFAULT=${NO_PROXY:-localhost,127.0.0.1,kind-registry,kind-registry:5000,*.svc,*.cluster.local}

require_cmd docker

build_args=(
  --platform "${KOVA_GOOS}/${KOVA_GOARCH}"
  --build-arg "TARGETOS=${KOVA_GOOS}"
  --build-arg "TARGETARCH=${KOVA_GOARCH}"
  --build-arg "NO_PROXY=${NO_PROXY_DEFAULT}"
  --build-arg "no_proxy=${NO_PROXY_DEFAULT}"
  --target kova-artifact
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

tmpdir=$(mktemp -d)
trap 'rm -rf "${tmpdir}"' EXIT

docker build \
  "${build_args[@]}" \
  --output "type=local,dest=${tmpdir}" \
  -f "${ROOT}/docker/Dockerfile" \
  "${ROOT}"

mkdir -p "${ROOT}/$(dirname "${BINARY}")"
cp "${tmpdir}/kova" "${ROOT}/${BINARY}"
chmod +x "${ROOT}/${BINARY}"
