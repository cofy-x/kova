#!/usr/bin/env bash

set -euo pipefail

source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

ROOT=$(repo_root)
WORK_DIR=${WORK_DIR:-.work}
SOURCE_ZIP=${SOURCE_ZIP:-${WORK_DIR}/source.zip}
EXAMPLE_DIRS=${EXAMPLE_DIRS:-simple}

require_cmd zip

output="${ROOT}/${SOURCE_ZIP}"
mkdir -p "$(dirname "${output}")"
rm -f "${output}"
(
  cd "${ROOT}/examples"
  # shellcheck disable=SC2086
  zip -qr "${output}" ${EXAMPLE_DIRS}
)
