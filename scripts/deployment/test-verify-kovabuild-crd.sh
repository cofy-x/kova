#!/usr/bin/env bash

set -euo pipefail

ROOT=$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
CHECK="${ROOT}/scripts/deployment/verify-kovabuild-crd.sh"

kubectl() {
  case "${1:-}" in
    wait)
      [[ "$*" == 'wait --for=condition=Established crd/kovabuilds.kova.cofy.dev --timeout=60s' ]] || return 2
      [[ "${MOCK_WAIT_FAILURE:-}" != 1 ]]
      ;;
    get)
      [[ "${2:-}" == crd && "${3:-}" == kovabuilds.kova.cofy.dev && "${4:-}" == -o && "${5:-}" == jsonpath=* ]] || return 2
      [[ "${5}" == *'pollFailureSince.type'* && "${5}" == *'pollFailureSince.format'* && \
        "${5}" == *'pollFailureCount.type'* && "${5}" == *'pollFailureCount.format'* ]] || return 2
      [[ "${MOCK_GET_FAILURE:-}" != 1 ]] || return 1
      printf '%s' "${MOCK_SCHEMA:-}"
      ;;
    *) return 2 ;;
  esac
}
export -f kubectl

assert_pass() {
  local label=$1 schema=$2 mode=$3 output
  if ! output=$(MOCK_SCHEMA="${schema}" KUBECTL=kubectl "${CHECK}" "${mode}" 2>&1); then
    echo "error: ${label} unexpectedly failed: ${output}" >&2
    exit 1
  fi
}

assert_blocked() {
  local label=$1 schema=$2 wait_failure=$3 get_failure=$4 output
  if output=$(MOCK_SCHEMA="${schema}" MOCK_WAIT_FAILURE="${wait_failure}" \
    MOCK_GET_FAILURE="${get_failure}" KUBECTL=kubectl "${CHECK}" 2>&1); then
    echo "error: ${label} unexpectedly passed: ${output}" >&2
    exit 1
  fi
  if [[ "${output}" != *'block the controller upgrade'* ]]; then
    echo "error: ${label} failed without an upgrade-block reason: ${output}" >&2
    exit 1
  fi
}

assert_pass 'new CRD schema' 'string|date-time|integer|int32' current
assert_pass 'legacy CRD schema' '|||' --expect-legacy
assert_blocked 'old CRD schema' '|||' 0 0
assert_blocked 'wrong retry timestamp format' 'string||integer|int32' 0 0
assert_blocked 'wrong retry count type' 'string|date-time|string|int32' 0 0
assert_blocked 'CRD not Established' 'string|date-time|integer|int32' 1 0
assert_blocked 'CRD read failed' 'string|date-time|integer|int32' 0 1
if output=$(MOCK_SCHEMA='string|date-time|integer|int32' KUBECTL=kubectl \
  "${CHECK}" --expect-legacy 2>&1); then
  echo "error: new CRD unexpectedly passed legacy check: ${output}" >&2
  exit 1
fi

echo 'CRD upgrade gate: current and legacy schemas distinguished; incompatible/unavailable CRD blocked'
