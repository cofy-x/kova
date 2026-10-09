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
for invalid in 'serviceDaemon.workerPoolID=' 'images.runner.digest=' 'images.runner.digest=sha256:bad'; do
  if helm template kova ./charts/kova --set serviceDaemon.enabled=true \
    -f scripts/chart/genesis-test-values.yaml --set-string "${invalid}" >/dev/null 2>&1; then
    echo "Service chart accepted missing or invalid execution identity: ${invalid}" >&2
    exit 1
  fi
done
deployment=$(helm template kova ./charts/kova --set serviceDaemon.enabled=true \
  -f scripts/chart/genesis-test-values.yaml --show-only templates/service-daemon-deployment.yaml)
if [[ ${deployment} != *'--worker-pool-id=render-test-pool'* ||
      ${deployment} != *'--runner-image=ghcr.io/cofy-x/kova@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'* ]]; then
  echo 'Service chart lost its worker pool or manifest digest pin' >&2
  exit 1
fi

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
if ! awk -v runner='kova-render-test-jobs' -v receipts='kova-render-test-receipts' '
  BEGIN { RS = "---"; FS = "\n" }
  {
    is_role = 0; namespace = ""
    for (i = 1; i <= NF; i++) {
      if ($i == "kind: Role") is_role = 1
      if ($i ~ /^  namespace: / && namespace == "") { split($i, bits, " "); namespace = bits[2] }
    }
    if (!is_role) next
    for (i = 1; i <= NF; i++) {
      if ($i !~ /resources: \["configmaps"\]/) continue
      named = (i < NF && $(i + 1) ~ /resourceNames:/)
      verbs = $(i + (named ? 2 : 1))
      if (namespace == runner && !named && verbs ~ /"(get|delete)"/) bad_runner = 1
      if (namespace == receipts && !named && verbs ~ /"create"/ && verbs ~ /"get"/ && verbs ~ /"delete"/) receipt_rule++
    }
  }
  END { if (bad_runner || receipt_rule != 1) exit 1 }
' <<<"${rendered}"; then
  echo 'Genesis RBAC must confine dynamic receipt GET/DELETE to the separate receipt namespace' >&2
  exit 1
fi
for collision in kova-render-test-jobs default; do
  if helm template kova ./charts/kova --set serviceDaemon.enabled=true \
    -f scripts/chart/genesis-test-values.yaml \
    --set "serviceDaemon.admissionGenesis.recoveryReceiptNamespace=${collision}" >/dev/null 2>&1; then
    echo "Service chart accepted receipt namespace collision with ${collision}" >&2
    exit 1
  fi
done
echo 'Service requires external Genesis authority, named ledger PATCH, and separate receipt-only DELETE permission'
