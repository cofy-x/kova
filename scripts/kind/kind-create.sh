#!/usr/bin/env bash

set -euo pipefail

source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

ROOT=$(repo_root)
KIND_CLUSTER=${KIND_CLUSTER:-kova-local}
KIND_NODE_IMAGE=${KIND_NODE_IMAGE:-kindest/node:v1.36.1@sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5}
KIND_CONFIG=${KIND_CONFIG:-deploy/kind-cluster.yaml}
KIND_KUBECONFIG=${KIND_KUBECONFIG:-.kind/${KIND_CLUSTER}.kubeconfig}
KIND_WORKERS=${KIND_WORKERS:-3}
REGISTRY_NAME=${REGISTRY_NAME:-kind-registry}

require_cmd docker
require_kind

"${ROOT}/scripts/kind/kind-registry.sh"

mkdir -p "${ROOT}/$(dirname "${KIND_KUBECONFIG}")"

if ! kind get clusters | grep -qx "${KIND_CLUSTER}"; then
  proxy_url=${HTTP_PROXY:-${http_proxy:-}}
  if [[ -z "${proxy_url}" ]]; then
    proxy_url=$(detect_host_http_proxy || true)
  fi
  if [[ -n "${proxy_url}" ]]; then
    proxy_url=${proxy_url/127.0.0.1/host.docker.internal}
    proxy_url=${proxy_url/localhost/host.docker.internal}
    export HTTP_PROXY="${proxy_url}"
    export HTTPS_PROXY="${proxy_url}"
    export http_proxy="${proxy_url}"
    export https_proxy="${proxy_url}"
    export NO_PROXY="${NO_PROXY:-localhost,127.0.0.1,kind-registry,kind-registry:5000,*.svc,*.cluster.local}"
    export no_proxy="${no_proxy:-${NO_PROXY}}"
    echo "Using kind node proxy: ${proxy_url}" >&2
  fi
  KUBECONFIG="${ROOT}/${KIND_KUBECONFIG}" kind create cluster \
    --name "${KIND_CLUSTER}" \
    --image "${KIND_NODE_IMAGE}" \
    --config "${ROOT}/${KIND_CONFIG}"
fi

worker_count=$(kind get nodes --name "${KIND_CLUSTER}" | grep -Ec -- "-worker[0-9]*$")
if [[ "${worker_count}" -lt "${KIND_WORKERS}" ]]; then
  echo "error: kind cluster ${KIND_CLUSTER} has ${worker_count} worker node(s), expected at least ${KIND_WORKERS}." >&2
  echo "Run 'make clean-kind kind-create' to recreate it with deploy/kind-cluster.yaml." >&2
  exit 1
fi

docker network connect kind "${REGISTRY_NAME}" >/dev/null 2>&1 || true

kind get kubeconfig --name "${KIND_CLUSTER}" > "${ROOT}/${KIND_KUBECONFIG}"
