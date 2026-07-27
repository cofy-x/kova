#!/usr/bin/env bash

set -euo pipefail

source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

ROOT=$(repo_root)
COMPOSE_FILE=${KOVA_OBSERVABILITY_COMPOSE_FILE:-${ROOT}/deploy/local/compose-observability.yaml}
GRAFANA_PORT=${KOVA_LOCAL_GRAFANA_PORT:-30301}
GRAFANA_URL="http://127.0.0.1:${GRAFANA_PORT}"

require_cmd curl
require_cmd docker

if ! docker compose -f "${COMPOSE_FILE}" ps --status running --services | grep -Fxq otel-lgtm; then
  echo "error: Kova local LGTM Compose service is not running; run make observability-up" >&2
  exit 1
fi

if ! curl -fsS "${GRAFANA_URL}/api/health" >/dev/null; then
  echo "error: Kova local Grafana is not healthy at ${GRAFANA_URL}" >&2
  exit 1
fi

echo "Kova local Grafana/LGTM is available at ${GRAFANA_URL}"
