#!/usr/bin/env bash
set -euo pipefail
image=${1:?usage: guard-image-version.sh IMAGE EXPECTED_DIGEST}
expected=${2:?usage: guard-image-version.sh IMAGE EXPECTED_DIGEST}
[[ "${expected}" =~ ^sha256:[0-9a-f]{64}$ ]] || exit 1
error_file=$(mktemp)
trap 'rm -f "${error_file}"' EXIT
if descriptor=$(docker buildx imagetools inspect "${image}" --format '{{json .Manifest}}' 2>"${error_file}"); then
  actual=$(jq -er .digest <<<"${descriptor}")
  if [[ "${actual}" != "${expected}" ]]; then
    echo 'version image already exists with a different digest; refusing overwrite' >&2
    exit 1
  fi
elif ! grep -Fxq "ERROR: ${image}: not found" "${error_file}" && ! grep -Fxq "${image}: not found" "${error_file}"; then
  echo 'cannot establish existing image identity; refusing publication' >&2
  exit 1
fi
