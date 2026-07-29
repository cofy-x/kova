#!/usr/bin/env bash

set -euo pipefail

source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

ROOT=$(repo_root)
VERSION=${1:-${KOVA_VERSION:-}}
REPOSITORY=${KOVA_REPOSITORY:-cofy-x/kova}

if [[ ! "${VERSION}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
  echo "usage: $0 vX.Y.Z[-prerelease]" >&2
  exit 2
fi

require_cmd curl
require_cmd helm
require_cmd tar

case "$(uname -s)" in
  Darwin) host_os=darwin; require_cmd shasum; checksum=(shasum -a 256) ;;
  Linux) host_os=linux; require_cmd sha256sum; checksum=(sha256sum) ;;
  *) echo "error: unsupported operating system: $(uname -s)" >&2; exit 2 ;;
esac
case "$(uname -m)" in
  x86_64 | amd64) host_arch=amd64 ;;
  arm64 | aarch64) host_arch=arm64 ;;
  *) echo "error: unsupported architecture: $(uname -m)" >&2; exit 2 ;;
esac

work_dir=$(mktemp -d "${TMPDIR:-/tmp}/kova-release-smoke.XXXXXX")
trap 'rm -rf "${work_dir}"' EXIT

release_base="https://github.com/${REPOSITORY}/releases/download/${VERSION}"
archive="kova_${VERSION#v}_${host_os}_${host_arch}.tar.gz"
curl --fail --location --retry 5 --retry-all-errors \
  --output "${work_dir}/${archive}" "${release_base}/${archive}"
curl --fail --location --retry 5 --retry-all-errors \
  --output "${work_dir}/checksums.txt" "${release_base}/checksums.txt"

expected=$(awk -v file="./${archive}" '$2 == file {print $1}' "${work_dir}/checksums.txt")
if [[ ! "${expected}" =~ ^[0-9a-f]{64}$ ]]; then
  echo "error: release checksum is missing for ${archive}" >&2
  exit 1
fi
actual=$("${checksum[@]}" "${work_dir}/${archive}" | awk '{print $1}')
if [[ "${actual}" != "${expected}" ]]; then
  echo "error: release checksum mismatch for ${archive}" >&2
  exit 1
fi

mkdir "${work_dir}/cli" "${work_dir}/chart"
tar -xzf "${work_dir}/${archive}" -C "${work_dir}/cli" kova LICENSE
"${work_dir}/cli/kova" version | grep -F "${VERSION}"
HELM_REGISTRY_CONFIG="${work_dir}/registry.json" \
  helm pull "oci://ghcr.io/${REPOSITORY%/*}/charts/kova" \
  --version "${VERSION#v}" --destination "${work_dir}/chart"

CONTROLLER_IMAGE="ghcr.io/${REPOSITORY}:controller-${VERSION}" \
RUNNER_IMAGE="ghcr.io/${REPOSITORY}:runner-${VERSION}" \
WORKER_IMAGE="ghcr.io/${REPOSITORY}:worker-${VERSION}" \
KOVA_CHART="${work_dir}/chart/kova-${VERSION#v}.tgz" \
KOVA_CLI="${work_dir}/cli/kova" \
E2E_SERVICE_BUILD_CLI=false \
E2E_SERVICE_BUILD_IMAGE=false \
KIND_LOAD_IMAGES=false \
START_OBSERVABILITY=false \
ARTIFACT_DRIVER=s3 \
KIND_CLUSTER=kova-released-artifact-smoke \
KIND_CONFIG=deploy/quickstart-kind-cluster.yaml \
KIND_KUBECONFIG=.kind/kova-released-artifact-smoke.kubeconfig \
KIND_WORKERS=1 \
KIND_VALUES=deploy/quickstart-kind-values.yaml \
KOVA_VALUES="${ROOT}/deploy/quickstart-kind-values.yaml" \
SOURCE_ZIP=.work/source-released-artifact.zip \
RESULT_JSONL=.work/result-released-artifact.jsonl \
"${ROOT}/scripts/e2e/e2e-service.sh"
