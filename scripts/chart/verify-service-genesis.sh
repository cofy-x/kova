#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${root}"

for extra in absent partial; do
  args=(--set serviceDaemon.enabled=true)
  if [[ ${extra} == partial ]]; then
    args+=(--set serviceDaemon.admissionGenesis.enabled=true --set serviceDaemon.runnerNamespace=jobs)
  fi
  if helm template kova ./charts/kova "${args[@]}" >/dev/null 2>&1; then
    echo "Service chart accepted ${extra} Genesis authority" >&2
    exit 1
  fi
done

rendered=$(helm template kova ./charts/kova --set serviceDaemon.enabled=true \
  -f scripts/chart/genesis-test-values.yaml --show-only templates/service-daemon-rbac.yaml)
if ! awk '
  /resourceNames: \["kova-service-admission", "kova-service-queue-admission"\]/ {
    getline
    if ($0 !~ /verbs: \["get", "patch"\]/) exit 1
    found++
  }
  END { if (found != 1) exit 1 }
' <<<"${rendered}"; then
  echo 'Genesis ledger RBAC must grant named GET and conditional PATCH' >&2
  exit 1
fi
echo 'Service requires external Genesis authority and named ledger PATCH permission'
