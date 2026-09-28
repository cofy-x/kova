#!/usr/bin/env bash
set -euo pipefail

metadata=${1:?usage: capture-image-digests.sh BUILD_METADATA_JSON}
# Validate every role before emitting any job outputs. A missing bake target or
# a config digest must never be substituted for the top-level image index.
for role in controller runner worker; do
  digest=$(jq -er --arg role "${role}" '.[$role]["containerimage.digest"]' "${metadata}")
  if [[ ! "${digest}" =~ ^sha256:[0-9a-f]{64}$ ]]; then
    echo "invalid ${role} candidate image digest" >&2
    exit 1
  fi
done
for role in controller runner worker; do
  digest=$(jq -er --arg role "${role}" '.[$role]["containerimage.digest"]' "${metadata}")
  printf '%s_digest=%s\n' "${role}" "${digest}"
done
