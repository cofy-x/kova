#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${root}"

assert_budget() {
  local values="$1" expected="$2" rendered
  rendered="$(helm template kova ./charts/kova --show-only templates/configmap.yaml -f "${values}" -f scripts/chart/genesis-test-values.yaml)"
  if [[ "${rendered}" != *"maxUsedSpace = \"${expected}GB\""* ]] ||
     [[ "${rendered}" != *'gc = true'* ]]; then
    echo "Missing bounded BuildKit GC policy in ${values}" >&2
    exit 1
  fi
}

assert_rejected() {
  local case_name="$1"
  shift
  if helm template kova ./charts/kova "$@" >/dev/null 2>&1; then
    echo "Unsafe worker cache configuration was accepted: ${case_name}" >&2
    exit 1
  fi
}

assert_budget ./charts/kova/values.yaml 8
assert_budget ./deploy/production-values.yaml 8
assert_budget ./deploy/kind-values.yaml 3
assert_budget ./deploy/quickstart-kind-values.yaml 3

assert_rejected reserved-at-max --set worker.cache.reservedSpaceGB=8
assert_rejected cache-near-volume --set worker.cache.maxUsedSpaceGB=17
assert_rejected cache-near-ephemeral-limit --set worker.resources.limits.ephemeral-storage=8Gi
assert_rejected missing-cache-volume --set worker.volumes[1].name=other-cache
assert_rejected unbounded-cache-volume --set worker.volumes[1].emptyDir.sizeLimit=null
assert_rejected missing-ephemeral-limit --set worker.resources.limits.ephemeral-storage=null
assert_rejected raw-worker-override --set-string 'buildkitdConfig=[worker.oci]'
assert_rejected cache-overflow --set worker.cache.maxUsedSpaceGB=9223372036854775807
assert_rejected cache-overflow-without-schema --skip-schema-validation --set worker.cache.maxUsedSpaceGB=9223372036854775807
assert_rejected volume-overflow --set worker.volumes[1].emptyDir.sizeLimit=999999999999999999999Gi
assert_rejected ephemeral-overflow --set worker.resources.limits.ephemeral-storage=999999999999999999999Gi

echo "BuildKit worker cache budgets render and reject unsafe overrides"
