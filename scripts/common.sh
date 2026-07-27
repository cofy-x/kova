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

kind_worker_nodes() {
  local cluster=$1
  kind get nodes --name "${cluster}" | grep -v 'control-plane'
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
