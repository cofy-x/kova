#!/usr/bin/env bash

# Exercise the real Helm packaging path without a cluster or image registry.
set -euo pipefail

root=$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
test_dir=$(mktemp -d)
trap 'rm -rf "${test_dir}"' EXIT

for tag in v1.2.3 v1.2.3-rc.1; do
  if ! DIST_DIR="${test_dir}/${tag}" bash "${root}/scripts/release/package-chart.sh" "${tag}" >"${test_dir}/package.log" 2>&1; then
    cat "${test_dir}/package.log" >&2
    echo "chart packaging failed for ${tag}" >&2
    exit 1
  fi
  archive=${test_dir}/${tag}/kova-${tag#v}.tgz
  test -f "${archive}"
  helm show chart "${archive}" | grep -Fx "version: ${tag#v}" >/dev/null
  helm show chart "${archive}" | grep -Fx "appVersion: ${tag}" >/dev/null
done

# Mutate only the copied chart. Helm must render it successfully, and the real
# packaging assertion must reject a different digest, mutable tag, or trailing text.
fixture=${test_dir}/fixture
mkdir -p "${fixture}/scripts/release" "${fixture}/scripts/chart" "${fixture}/charts"
cp "${root}/scripts/common.sh" "${fixture}/scripts/common.sh"
cp "${root}/scripts/release/package-chart.sh" "${fixture}/scripts/release/package-chart.sh"
cp "${root}/scripts/chart/genesis-test-values.yaml" "${fixture}/scripts/chart/genesis-test-values.yaml"
cp -R "${root}/charts/kova" "${fixture}/charts/kova"

for image in \
  'ghcr.io/cofy-x/kova@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc' \
  'ghcr.io/cofy-x/kova:runner-{{ .Chart.AppVersion }}' \
  'ghcr.io/cofy-x/kova@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb garbage'; do
  awk -v image="${image}" '
    /^            - --runner-image=/ { $0 = "            - --runner-image=" image }
    { print }
  ' "${root}/charts/kova/templates/service-daemon-deployment.yaml" >"${fixture}/charts/kova/templates/service-daemon-deployment.yaml"
  if DIST_DIR="${test_dir}/rejected" bash "${fixture}/scripts/release/package-chart.sh" v1.2.3 >"${test_dir}/rejected.log" 2>&1; then
    echo "chart packaging accepted incorrect runner image: ${image}" >&2
    exit 1
  fi
  if ! grep -F 'error: packaged Service runner image must match render fixture ' "${test_dir}/rejected.log" >/dev/null; then
    cat "${test_dir}/rejected.log" >&2
    echo 'chart packaging did not reach the runner image assertion' >&2
    exit 1
  fi
done

echo 'chart packaging verifies release versions and exact runner manifest digest rendering'
