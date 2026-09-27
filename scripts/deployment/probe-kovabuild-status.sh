#!/usr/bin/env bash

# Test the API server's status-field pruning without involving a Kova controller.
set -euo pipefail

KUBECTL=${KUBECTL:-kubectl}
MODE=${1:-}
PROBE_ATTEMPTS=${PROBE_ATTEMPTS:-15}
if [[ ! "${PROBE_ATTEMPTS}" =~ ^([1-9]|1[0-5])$ ]]; then
  echo 'error: PROBE_ATTEMPTS must be between 1 and 15' >&2
  exit 2
fi
case ${MODE} in
  --expect-pruned) probe_namespace=kova-crd-upgrade-probe-legacy ;;
  --expect-persisted) probe_namespace=kova-crd-upgrade-probe-current ;;
  *) echo "usage: $0 --expect-pruned|--expect-persisted" >&2; exit 2 ;;
esac

if "${KUBECTL}" get namespace "${probe_namespace}" >/dev/null 2>&1; then
  echo "error: probe namespace ${probe_namespace} already exists; refusing to reuse it" >&2
  exit 1
fi

created=false
cleanup() {
  local status=$?
  trap - EXIT
  if [[ "${created}" == true ]]; then
    if ! "${KUBECTL}" delete namespace "${probe_namespace}" --wait=true --timeout=60s >&2; then
      echo "error: failed to remove probe namespace ${probe_namespace}" >&2
      status=1
    fi
  fi
  exit "${status}"
}
trap cleanup EXIT

"${KUBECTL}" create namespace "${probe_namespace}" >/dev/null
created=true
"${KUBECTL}" -n "${probe_namespace}" create -f - >/dev/null <<'EOF'
apiVersion: kova.cofy.dev/v1alpha1
kind: KovaBuild
metadata:
  name: retry-status-probe
spec:
  requester:
    username: crd-upgrade-probe
  targets:
    - target: registry.invalid/example:probe
      platform: linux/amd64
  source:
    uri: oci://registry.invalid/source@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    digest: sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
  build:
    format: oci
    concurrency: 1
EOF

patch='{"status":{"pollFailureSince":"2026-01-02T03:04:05Z","pollFailureCount":3}}'
if [[ "${MODE}" == '--expect-pruned' ]]; then
  "${KUBECTL}" -n "${probe_namespace}" patch kovabuild retry-status-probe \
    --type=merge --subresource=status -p "${patch}" >/dev/null
  observed=$("${KUBECTL}" -n "${probe_namespace}" get kovabuild retry-status-probe -o json)
  if ! jq -e '(.status.pollFailureSince == null) and (.status.pollFailureCount == null)' \
    <<<"${observed}" >/dev/null; then
    echo 'error: legacy CRD unexpectedly persisted retry status fields' >&2
    exit 1
  fi
  echo 'legacy CRD pruned retry status fields as expected'
else
  # CRD schema updates can take a short time to reach the serving API path.
  for ((attempt = 1; attempt <= PROBE_ATTEMPTS; attempt++)); do
    if "${KUBECTL}" -n "${probe_namespace}" patch kovabuild retry-status-probe \
      --type=merge --subresource=status -p "${patch}" >/dev/null 2>&1; then
      if observed=$("${KUBECTL}" -n "${probe_namespace}" get kovabuild retry-status-probe -o json); then
        if jq -e '.status.pollFailureSince == "2026-01-02T03:04:05Z" and .status.pollFailureCount == 3' \
          <<<"${observed}" >/dev/null; then
          echo 'current CRD persisted retry status fields'
          exit 0
        fi
      fi
    fi
    if (( attempt < PROBE_ATTEMPTS )); then
      sleep 2
    fi
  done
  echo 'error: current CRD pruned retry status fields or status API remained unavailable' >&2
  exit 1
fi
