#!/usr/bin/env bash

set -euo pipefail

source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

ROOT=$(repo_root)
PYTHON_VERSION=${KOVA_PYTHON_VERSION:-3.10}
WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/kova-sdk-examples.XXXXXX")
trap 'rm -rf "${WORK_DIR}"' EXIT

require_cmd go
require_cmd uv

uv sync --python "${PYTHON_VERSION}" --directory "${ROOT}/sdk/python" --locked
go build -trimpath -o "${WORK_DIR}/go-service-sdk-example" "${ROOT}/examples/service-sdk/go"
uv run --python "${PYTHON_VERSION}" --directory "${ROOT}/sdk/python" --locked \
  python "${ROOT}/scripts/ci/test-sdk-examples.py" \
  --go-example "${WORK_DIR}/go-service-sdk-example" \
  --python-example "${ROOT}/examples/service-sdk/python/main.py" \
  --receipt-example "${ROOT}/docs/examples/seed-build-receipt-v1.json"
