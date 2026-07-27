#!/usr/bin/env bash

set -euo pipefail

source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

ROOT=$(repo_root)
PROMETHEUS_URL=${KOVA_LOCAL_PROMETHEUS_URL:-http://127.0.0.1:${KOVA_LOCAL_PROMETHEUS_PORT:-19090}}

require_cmd curl
require_cmd python3

"${ROOT}/scripts/observability/local-up.sh"
"${ROOT}/scripts/e2e/e2e.sh"

deadline=$((SECONDS + 90))
while (( SECONDS < deadline )); do
  result=$(curl -fsS --get --data-urlencode 'query=sum(kova_batch_builds_total)' "${PROMETHEUS_URL}/api/v1/query" 2>/dev/null || true)
  if python3 -c 'import json,sys; d=json.load(sys.stdin); raise SystemExit(not d.get("data", {}).get("result"))' <<<"${result}" 2>/dev/null; then
    echo "observability telemetry received"
    exit 0
  fi
  sleep 5
done

echo "local LGTM Prometheus did not receive kova-runner build telemetry" >&2
exit 1
