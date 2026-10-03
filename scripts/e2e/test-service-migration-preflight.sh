#!/usr/bin/env bash

# Cluster-free checks: unsafe migration inputs must fail before any Kind write.
set -euo pipefail

root=$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
service_e2e=${root}/scripts/e2e/e2e-service.sh

assert_rejected() {
  local label=$1 expected=$2 output status
  shift 2
  status=0
  output=$(env -i PATH="${PATH}" KOVA_PLATFORM=linux/amd64 "$@" "${service_e2e}" 2>&1) || status=$?
  if [[ "${status}" != 2 || "${output}" != *"${expected}"* ]]; then
    echo "error: ${label} did not fail closed in preflight (exit ${status}): ${output}" >&2
    exit 1
  fi
}

assert_rejected baseline-without-fresh-namespace \
  'baseline Service migration requires a fresh runner namespace' \
  BASELINE_CHART=/tmp/kova-migration-test-baseline.tgz \
  NAMESPACE=kova SERVICE_RUNNER_NAMESPACE=kova
assert_rejected legacy-without-baseline \
  'REQUIRE_LEGACY_CRD requires BASELINE_CHART' \
  REQUIRE_LEGACY_CRD=true
assert_rejected invalid-legacy-mode \
  'REQUIRE_LEGACY_CRD must be true or false' \
  REQUIRE_LEGACY_CRD=maybe
assert_rejected same-namespace-fresh-install \
  'Service E2E requires a separate, never-used runner namespace' \
  NAMESPACE=kova SERVICE_RUNNER_NAMESPACE=kova

echo 'Service migration preflight rejects in-place baseline and invalid legacy modes'
