#!/usr/bin/env bash

set -euo pipefail

source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

ROOT=$(repo_root)
TAG=${1:?usage: package-chart.sh vX.Y.Z[-prerelease]}
DIST_DIR=${DIST_DIR:-${ROOT}/dist}

require_cmd helm

if [[ ! "${TAG}" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z.-]+)?$ ]]; then
  echo "error: chart release tag must be a semantic version prefixed with v" >&2
  exit 1
fi

VERSION=${TAG#v}
ARCHIVE=${DIST_DIR}/kova-${VERSION}.tgz
rendered=$(mktemp "${TMPDIR:-/tmp}/kova-chart.XXXXXX")
cross_namespace=$(mktemp "${TMPDIR:-/tmp}/kova-chart-cross-namespace.XXXXXX")
trap 'rm -f "${rendered}" "${cross_namespace}"' EXIT

mkdir -p "${DIST_DIR}"
rm -f "${ARCHIVE}"

helm lint "${ROOT}/charts/kova"
helm package "${ROOT}/charts/kova" \
  --version "${VERSION}" \
  --app-version "${TAG}" \
  --destination "${DIST_DIR}" >/dev/null

test -f "${ARCHIVE}"
helm show chart "${ARCHIVE}" | grep -Fx "version: ${VERSION}" >/dev/null
helm show chart "${ARCHIVE}" | grep -Fx "appVersion: ${TAG}" >/dev/null

helm template kova "${ARCHIVE}" \
  --namespace kova \
  --set serviceDaemon.enabled=true \
  --set artifactStore.filesystem.pvc.create=true >"${rendered}"

grep -F "image: \"ghcr.io/cofy-x/kova:controller-${TAG}\"" "${rendered}" >/dev/null
grep -F "image: \"ghcr.io/cofy-x/kova:worker-${TAG}\"" "${rendered}" >/dev/null
grep -F -- "--runner-image=ghcr.io/cofy-x/kova:runner-${TAG}" "${rendered}" >/dev/null

helm template kova "${ARCHIVE}" \
  --namespace release \
  --set serviceDaemon.enabled=true \
  --set serviceDaemon.runnerNamespace=jobs \
  --set artifactStore.filesystem.pvc.create=true \
  --set imagePullSecrets.create=true \
  --set imagePullSecrets.name=kova-registry \
  --set imageRegistries[0].name=registry.example.com \
  --set imageRegistries[0].auth=dGVzdDp0ZXN0 >"${cross_namespace}"

secret_namespaces=$(awk '
  /^kind: Secret$/ { in_secret = 1; next }
  in_secret && /^  namespace:/ { print $2; in_secret = 0 }
' "${cross_namespace}" | sort)
if [[ "${secret_namespaces}" != $'jobs\nrelease' ]]; then
  echo "error: registry Secrets must be limited to release and runner namespaces" >&2
  exit 1
fi

printf '%s\n' "${ARCHIVE}"
