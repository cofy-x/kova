#!/usr/bin/env bash

set -euo pipefail

ROOT=$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
GATE=${ROOT}/scripts/deployment/verify-kovabuild-drained.sh

kubectl() {
  [[ "${1:-}" == -n && "${3:-}" == get ]] || return 2
  case "${4:-}" in
    kovabuilds.kova.cofy.dev)
      [[ "${2:-}" == "${MOCK_EXPECT_RUNNER_NAMESPACE:-kova}" ]] || return 2
      [[ "${5:-}" == -o && "${6:-}" == json ]] || return 2
      [[ "${MOCK_ERROR:-}" != builds ]] || return 1
      printf '%s\n' "${MOCK_BUILDS}"
      ;;
    pods)
      if [[ "${5:-}" == -o && "${6:-}" == json ]]; then
        [[ "${2:-}" == "${MOCK_EXPECT_RUNNER_NAMESPACE:-kova}" ]] || return 2
        [[ "${MOCK_ERROR:-}" != pods ]] || return 1
        printf '%s\n' "${MOCK_PODS}"
      elif [[ "${5:-}" == -l && "${6:-}" == 'app.kubernetes.io/instance=kova,app.kubernetes.io/component=service' && "${7:-}" == -o && "${8:-}" == json ]]; then
        [[ "${2:-}" == kova ]] || return 2
        [[ "${MOCK_ERROR:-}" != service-pods ]] || return 1
        printf '%s\n' "${MOCK_SERVICE_PODS}"
      else
        return 2
      fi
      ;;
    deployments)
      [[ "${2:-}" == kova ]] || return 2
      [[ "${5:-}" == -l && "${6:-}" == 'app.kubernetes.io/instance=kova,app.kubernetes.io/component=service' && "${7:-}" == -o && "${8:-}" == json ]] || return 2
      [[ "${MOCK_ERROR:-}" != deployments ]] || return 1
      printf '%s\n' "${MOCK_DEPLOYMENTS}"
      ;;
    *) return 2 ;;
  esac
}
export -f kubectl

assert_gate() {
  local label=$1 builds=$2 pods=$3 service_pods=$4 deployments=$5 error=$6 want_pass=$7 runner_ns=${8:-kova} output
  if output=$(MOCK_BUILDS="${builds}" MOCK_PODS="${pods}" MOCK_DEPLOYMENTS="${deployments}" \
    MOCK_SERVICE_PODS="${service_pods}" MOCK_ERROR="${error}" \
    MOCK_EXPECT_RUNNER_NAMESPACE="${runner_ns}" RUNNER_NAMESPACE="${runner_ns}" KUBECTL=kubectl "${GATE}" 2>&1); then
    if [[ "${want_pass}" != true ]]; then
      echo "error: ${label} unexpectedly passed: ${output}" >&2
      exit 1
    fi
  elif [[ "${want_pass}" == true || "${output}" != *'block the controller upgrade'* ]]; then
    echo "error: ${label} failed incorrectly: ${output}" >&2
    exit 1
  fi
}

empty='{"items":[]}'
terminal='{"items":[{"metadata":{"name":"done"},"status":{"phase":"Succeeded"}}]}'
starting='{"items":[{"metadata":{"name":"legacy"},"status":{"phase":"Starting"}}]}'
verifying='{"items":[{"metadata":{"name":"verifying"},"status":{"phase":"Verifying"}}]}'
failed_verifying='{"items":[{"metadata":{"name":"failed-verifying"},"status":{"phase":"FailedVerifying"}}]}'
unknown='{"items":[{"metadata":{"name":"unknown"},"status":{}}]}'
runner='{"items":[{"metadata":{"name":"legacy-runner","labels":{"app.kubernetes.io/name":"kova-runner"}}}]}'
service='{"items":[{"metadata":{"name":"kova-service"},"spec":{"replicas":1},"status":{"readyReplicas":1}}]}'
service_pod='{"items":[{"metadata":{"name":"kova-service-old"}}]}'
stopped='{"items":[{"metadata":{"name":"kova-service"},"spec":{"replicas":0},"status":{"replicas":0}}]}'
wrong_deployment='{"items":[{"metadata":{"name":"different-service"},"spec":{"replicas":0},"status":{"replicas":0}}]}'

assert_gate empty-deployment "${empty}" "${empty}" "${empty}" "${empty}" '' false
assert_gate wrong-deployment "${empty}" "${empty}" "${empty}" "${wrong_deployment}" '' false
assert_gate drained "${empty}" "${empty}" "${empty}" "${stopped}" '' true
assert_gate terminal "${terminal}" "${empty}" "${empty}" "${stopped}" '' false
assert_gate separate-runner-namespace "${empty}" "${empty}" "${empty}" "${stopped}" '' true jobs
assert_gate starting "${starting}" "${empty}" "${empty}" "${stopped}" '' false
assert_gate verifying "${verifying}" "${empty}" "${empty}" "${stopped}" '' false
assert_gate failed-verifying "${failed_verifying}" "${empty}" "${empty}" "${stopped}" '' false
assert_gate unknown-phase "${unknown}" "${empty}" "${empty}" "${stopped}" '' false
assert_gate runner-pod "${empty}" "${runner}" "${empty}" "${stopped}" '' false
assert_gate service-pod "${empty}" "${empty}" "${service_pod}" "${stopped}" '' false
assert_gate running-service "${empty}" "${empty}" "${empty}" "${service}" '' false
assert_gate unreadable-builds "${empty}" "${empty}" "${empty}" "${stopped}" builds false
assert_gate unreadable-service-pods "${empty}" "${empty}" "${empty}" "${stopped}" service-pods false
assert_gate malformed-list '{}' "${empty}" "${empty}" "${stopped}" '' false

echo 'quiescence gate: empty build set and stopped named Service accepted; remaining, unknown, and unreadable states blocked'
