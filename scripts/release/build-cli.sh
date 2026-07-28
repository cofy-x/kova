#!/usr/bin/env bash

set -euo pipefail

source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

ROOT=$(repo_root)
VERSION=${1:?usage: build-cli.sh <version>}
COMMIT=${COMMIT:-$(git -C "${ROOT}" rev-parse HEAD)}
BUILD_DATE=${BUILD_DATE:-$(git -C "${ROOT}" show -s --format=%cI HEAD)}
DIST="${ROOT}/dist"
VERSION_PACKAGE=github.com/cofy-x/kova/internal/version
LDFLAGS="-s -w -X ${VERSION_PACKAGE}.Version=${VERSION} -X ${VERSION_PACKAGE}.Commit=${COMMIT} -X ${VERSION_PACKAGE}.BuildDate=${BUILD_DATE}"

require_cmd go
require_cmd tar
require_cmd zip

rm -rf "${DIST}"
mkdir -p "${DIST}"
tmpdir=$(mktemp -d)
trap 'rm -rf "${tmpdir}"' EXIT

targets=(
  linux/amd64
  linux/arm64
  darwin/amd64
  darwin/arm64
  windows/amd64
  windows/arm64
)

MAX_PARALLEL=${KOVA_RELEASE_BUILD_JOBS:-2}
if ! [[ "${MAX_PARALLEL}" =~ ^[1-9][0-9]*$ ]]; then
  echo "KOVA_RELEASE_BUILD_JOBS must be a positive integer" >&2
  exit 2
fi

pids=()
failed=0
for target in "${targets[@]}"; do
  (
  goos=${target%/*}
  goarch=${target#*/}
  archive_version=${VERSION#v}
  workdir="${tmpdir}/${goos}-${goarch}"
  mkdir -p "${workdir}"

  binary=kova
  if [[ "${goos}" == windows ]]; then
    binary=kova.exe
  fi

  CGO_ENABLED=0 GOOS="${goos}" GOARCH="${goarch}" \
    go build -trimpath -tags 'netgo,osusergo' -ldflags "${LDFLAGS}" \
    -o "${workdir}/${binary}" "${ROOT}/cmd/kova"
  cp "${ROOT}/LICENSE" "${workdir}/LICENSE"

  if [[ "${goos}" == windows ]]; then
    (cd "${workdir}" && zip -q "${DIST}/kova_${archive_version}_${goos}_${goarch}.zip" "${binary}" LICENSE)
  else
    tar -C "${workdir}" -czf "${DIST}/kova_${archive_version}_${goos}_${goarch}.tar.gz" "${binary}" LICENSE
  fi
  ) &
  pids+=("$!")
  if (( ${#pids[@]} == MAX_PARALLEL )); then
    for pid in "${pids[@]}"; do
      wait "${pid}" || failed=1
    done
    pids=()
  fi
done

for pid in "${pids[@]}"; do
  wait "${pid}" || failed=1
done
exit "${failed}"
