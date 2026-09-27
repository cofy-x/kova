#!/usr/bin/env bash

set -euo pipefail

ROOT=$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
PROBE=${ROOT}/scripts/deployment/probe-kovabuild-status.sh
call_log=$(mktemp "${TMPDIR:-/tmp}/kova-crd-probe-test.XXXXXX")
trap 'rm -f -- "${call_log}"' EXIT

kubectl() {
  printf '%s\n' "$*" >>"${MOCK_CALL_LOG}"
  case "${1:-}" in
    get)
      [[ "${2:-}" == namespace ]] || return 2
      return 1
      ;;
    create)
      [[ "${2:-}" == namespace ]] || return 2
      ;;
    delete)
      [[ "${2:-}" == namespace && "${4:-}" == --wait=true && "${5:-}" == --timeout=60s ]] || return 2
      ;;
    -n)
      case "${3:-}" in
        create) [[ "${4:-}" == -f && "${5:-}" == - ]] || return 2 ;;
        patch) [[ "${4:-}" == kovabuild && "${5:-}" == retry-status-probe && "$*" == *'--subresource=status'* ]] || return 2 ;;
        get)
          [[ "${4:-}" == kovabuild && "${5:-}" == retry-status-probe && "${6:-}" == -o && "${7:-}" == json ]] || return 2
          printf '%s\n' "${MOCK_STATUS_JSON}"
          ;;
        *) return 2 ;;
      esac
      ;;
    *) return 2 ;;
  esac
}
export -f kubectl

assert_probe() {
  local label=$1 mode=$2 status_json=$3 expect_success=$4 output
  : >"${call_log}"
  if output=$(MOCK_CALL_LOG="${call_log}" MOCK_STATUS_JSON="${status_json}" \
    PROBE_ATTEMPTS=1 KUBECTL=kubectl "${PROBE}" "${mode}" 2>&1); then
    if [[ "${expect_success}" != true ]]; then
      echo "error: ${label} unexpectedly passed: ${output}" >&2
      exit 1
    fi
  elif [[ "${expect_success}" == true ]]; then
    echo "error: ${label} unexpectedly failed: ${output}" >&2
    exit 1
  fi
  if ! grep -Fq 'delete namespace kova-crd-upgrade-probe-' "${call_log}"; then
    echo "error: ${label} did not clean its test namespace" >&2
    exit 1
  fi
}

pruned='{"status":{}}'
persisted='{"status":{"phase":"FailedVerifying","reason":"BuildFailed","pollFailureSince":"2026-01-02T03:04:05Z","pollFailureCount":3,"verificationStartedAt":"2026-01-02T03:04:05Z","verificationDeadlineAt":"2026-01-02T03:09:05Z","verificationNextAttemptAt":"2026-01-02T03:04:06Z","verificationAttempts":2,"verificationLastError":"temporary registry error","verificationResults":[{"pushedDigest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","state":"pending"}]}}'
assert_probe 'legacy pruning' --expect-pruned "${pruned}" true
assert_probe 'legacy unexpectedly persisted' --expect-pruned "${persisted}" false
assert_probe 'current persistence' --expect-persisted "${persisted}" true
assert_probe 'current unexpectedly pruned' --expect-persisted "${pruned}" false

echo 'status probe: legacy pruning and current persistence classified; namespace cleaned'
