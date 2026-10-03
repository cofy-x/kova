#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${root}"

helm template kova ./charts/kova --set serviceDaemon.enabled=true \
  -f scripts/chart/genesis-test-values.yaml \
  --set-string serviceDaemon.metricsBindAddress=127.0.0.1:8081 >/dev/null

for address in 0.0.0.0:8081 127.0.0.1:8080 127.0.0.1:65536; do
  if helm template kova ./charts/kova --set serviceDaemon.enabled=true \
    -f scripts/chart/genesis-test-values.yaml \
    --set-string "serviceDaemon.metricsBindAddress=${address}" >/dev/null 2>&1; then
    echo "Unsafe Service metrics address accepted: ${address}" >&2
    exit 1
  fi
done

echo "Service diagnostic metrics render only on a distinct loopback port"
