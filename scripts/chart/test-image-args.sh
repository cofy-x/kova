#!/usr/bin/env bash
set -euo pipefail
source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
helm_image_args=()
append_helm_role_image_args runner "localhost:5002/kova@${digest}"
expected=(--set-string images.runner.repository=localhost:5002/kova --set-string images.runner.tag= --set-string "images.runner.digest=${digest}")
[[ "${helm_image_args[*]}" == "${expected[*]}" ]]

helm_image_args=()
append_helm_role_image_args controller localhost:5002/kova:controller-dev
expected=(--set-string images.controller.repository=localhost:5002/kova --set-string images.controller.tag=controller-dev --set-string images.controller.digest=)
[[ "${helm_image_args[*]}" == "${expected[*]}" ]]

helm_image_args=()
append_helm_role_image_args worker "ghcr.io/cofy-x/kova@${digest}" true
expected=(--set-string images.worker.repository=ghcr.io/cofy-x/kova@sha256 --set-string "images.worker.tag=${digest#sha256:}")
[[ "${helm_image_args[*]}" == "${expected[*]}" ]]

for invalid in localhost:5002/kova localhost:5002/kova: localhost:5002/kova@sha256:bad; do
  if append_helm_role_image_args runner "${invalid}" 2>/dev/null; then
    echo 'image arguments accepted an unpinned or malformed reference' >&2
    exit 1
  fi
done
echo 'Helm image arguments preserve digest and tag references'
