#!/usr/bin/env bash

set -euo pipefail

source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

ROOT=$(repo_root)
WORK_DIR=${WORK_DIR:-.work}
SOURCE_ZIP=${SOURCE_ZIP:-${WORK_DIR}/source.zip}
EXAMPLE_DIRS=${EXAMPLE_DIRS:-simple}
KOVA_PLATFORM=$(kova_platform)

require_cmd zip
require_cmd jq

staging=$(mktemp -d "${TMPDIR:-/tmp}/kova-package-example.XXXXXX")
trap 'rm -rf "${staging}"' EXIT
for example in ${EXAMPLE_DIRS}; do
  cp -R "${ROOT}/examples/${example}" "${staging}/${example}"
  metadata=${staging}/${example}/metadata.json
  updated=${metadata}.tmp
  jq --arg platform "${KOVA_PLATFORM}" '.platform = $platform' "${metadata}" >"${updated}"
  mv "${updated}" "${metadata}"
done

output="${ROOT}/${SOURCE_ZIP}"
mkdir -p "$(dirname "${output}")"
rm -f "${output}"
(
  cd "${staging}"
  # shellcheck disable=SC2086
  zip -qr "${output}" ${EXAMPLE_DIRS}
)
