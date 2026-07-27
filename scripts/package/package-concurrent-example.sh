#!/usr/bin/env bash

set -euo pipefail

source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

ROOT=$(repo_root)
WORK_DIR=${WORK_DIR:-.work}
SOURCE_ZIP=${SOURCE_ZIP:-${WORK_DIR}/source-concurrent.zip}
EXAMPLE_COUNT=${EXAMPLE_COUNT:-12}
GENERATED_WORK_DIR=${GENERATED_WORK_DIR:-.generated/concurrent-examples}

require_cmd zip

if ! [[ "${EXAMPLE_COUNT}" =~ ^[0-9]+$ ]] || [[ "${EXAMPLE_COUNT}" -lt 1 ]]; then
  echo "error: EXAMPLE_COUNT must be a positive integer" >&2
  exit 1
fi

rm -rf "${ROOT:?}/${GENERATED_WORK_DIR:?}"
mkdir -p "${ROOT}/${GENERATED_WORK_DIR}"

for i in $(seq 1 "${EXAMPLE_COUNT}"); do
  name=$(printf "image-%02d" "${i}")
  target=$(printf "concurrent-%02d" "${i}")
  image_dir="${ROOT}/${GENERATED_WORK_DIR}/${name}"
  mkdir -p "${image_dir}"
  cat > "${image_dir}/Dockerfile" <<EOF
FROM scratch
COPY hello.txt /hello.txt
EOF
  cat > "${image_dir}/metadata.json" <<EOF
{
  "target": "\$KOVA_IMAGE_REGISTRY/kova-examples/${target}:dev"
}
EOF
  printf "hello from concurrent build %02d\n" "${i}" > "${image_dir}/hello.txt"
done

output="${ROOT}/${SOURCE_ZIP}"
mkdir -p "$(dirname "${output}")"
rm -f "${output}"
(
  cd "${ROOT}/${GENERATED_WORK_DIR}"
  zip -qr "${output}" .
)
