#!/usr/bin/env bash

set -euo pipefail

repo_root() {
  local script_dir
  script_dir=$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
  CDPATH='' cd -- "${script_dir}/.." && pwd
}

detect_host_http_proxy() {
  local port=${LOCAL_PROXY_PORT:-7890}
  if command -v nc >/dev/null 2>&1 && nc -z 127.0.0.1 "${port}" >/dev/null 2>&1; then
    printf 'http://host.docker.internal:%s\n' "${port}"
    return
  fi
  if command -v bash >/dev/null 2>&1 && command -v timeout >/dev/null 2>&1 && timeout 1 bash -c ":</dev/tcp/127.0.0.1/${port}" >/dev/null 2>&1; then
    printf 'http://host.docker.internal:%s\n' "${port}"
  fi
}

docker_arch() {
  docker info --format '{{.Architecture}}' 2>/dev/null | sed -e 's/aarch64/arm64/' -e 's/x86_64/amd64/'
}

kova_platform() {
  local platform=${KOVA_PLATFORM:-}
  local arch
  if [[ -z "${platform}" ]]; then
    arch=$(docker_arch || true)
    if [[ -z "${arch}" ]]; then
      arch=$(uname -m | sed -e 's/aarch64/arm64/' -e 's/arm64/arm64/' -e 's/x86_64/amd64/')
    fi
    platform=linux/${arch}
  fi
  case ${platform} in
    linux/amd64|linux/arm64) printf '%s\n' "${platform}" ;;
    *)
      echo "error: unsupported KOVA_PLATFORM ${platform}; expected linux/amd64 or linux/arm64" >&2
      return 2
      ;;
  esac
}

kind_worker_nodes() {
  local cluster=$1
  kind get nodes --name "${cluster}" | grep -v 'control-plane'
}

kind_use_overlayfs_snapshotter() {
  local cluster=$1
  local node
  for node in $(kind_worker_nodes "${cluster}"); do
    docker exec "${node}" bash -lc '
set -euo pipefail
conf=/etc/containerd/config.toml
if grep -q '\''snapshotter = "nydus"'\'' "${conf}"; then
  sed -i '\''s/snapshotter = "nydus"/snapshotter = "overlayfs"/g'\'' "${conf}"
  systemctl restart containerd
fi
'
  done
}

require_cmd() {
  local cmd=$1
  command -v "${cmd}" >/dev/null 2>&1 || {
    echo "missing required command: ${cmd}" >&2
    exit 1
  }
}

require_kind() {
  local version
  local major
  local minor

  require_cmd kind
  version=$(kind version | awk '{print $2}')
  if [[ ! ${version} =~ ^v?([0-9]+)\.([0-9]+)\.([0-9]+) ]]; then
    echo "error: unable to parse kind version: ${version}" >&2
    exit 1
  fi

  major=${BASH_REMATCH[1]}
  minor=${BASH_REMATCH[2]}
  if (( major == 0 && minor < 32 )); then
    echo "error: kind v0.32.0 or newer is required; found ${version}" >&2
    exit 1
  fi
}

wait_for_tcp() {
  local host=$1
  local port=$2
  local timeout_seconds=${3:-15}
  local deadline=$((SECONDS + timeout_seconds))

  while (( SECONDS < deadline )); do
    if command -v nc >/dev/null 2>&1 && nc -z "${host}" "${port}" >/dev/null 2>&1; then
      return 0
    fi
    if command -v bash >/dev/null 2>&1 && command -v timeout >/dev/null 2>&1 && timeout 1 bash -c ":</dev/tcp/${host}/${port}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  echo "error: timed out waiting for ${host}:${port}" >&2
  return 1
}
