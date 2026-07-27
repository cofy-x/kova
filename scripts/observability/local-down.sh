#!/usr/bin/env bash

set -euo pipefail

source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

ROOT=$(repo_root)
COMPOSE_FILE=${KOVA_OBSERVABILITY_COMPOSE_FILE:-${ROOT}/deploy/local/compose-observability.yaml}

require_cmd docker

docker compose -f "${COMPOSE_FILE}" down --remove-orphans
