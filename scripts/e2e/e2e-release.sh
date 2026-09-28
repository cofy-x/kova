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
require_cmd cmp

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

verify_release_checksum() {
  local file=$1 expected actual
  expected=$(awk -v file="./${file}" '$2 == file {print $1}' "${work_dir}/checksums.txt")
  if [[ ! "${expected}" =~ ^[0-9a-f]{64}$ ]]; then
    echo "error: release checksum is missing or ambiguous for ${file}" >&2
    return 1
  fi
  actual=$("${checksum[@]}" "${work_dir}/${file}" | awk '{print $1}')
  if [[ "${actual}" != "${expected}" ]]; then
    echo "error: release checksum mismatch for ${file}" >&2
    return 1
  fi
}

verify_release_checksum "${archive}"

metadata=candidate-images.json
if awk -v file="./${metadata}" '$2 == file {found=1} END {exit !found}' "${work_dir}/checksums.txt"; then
  curl --fail --location --retry 5 --retry-all-errors \
    --output "${work_dir}/${metadata}" "${release_base}/${metadata}"
  verify_release_checksum "${metadata}"
  require_cmd jq
  # Validate all three top-level bake digests before consuming any role. The
  # config digest is not the multi-platform index used by the release smoke.
  bash "${ROOT}/scripts/release/capture-image-digests.sh" "${work_dir}/${metadata}" >/dev/null
  CONTROLLER_IMAGE="ghcr.io/${REPOSITORY}@$(jq -er '.controller["containerimage.digest"]' "${work_dir}/${metadata}")"
  RUNNER_IMAGE="ghcr.io/${REPOSITORY}@$(jq -er '.runner["containerimage.digest"]' "${work_dir}/${metadata}")"
  WORKER_IMAGE="ghcr.io/${REPOSITORY}@$(jq -er '.worker["containerimage.digest"]' "${work_dir}/${metadata}")"
elif [[ "${VERSION}" =~ ^v0\.1\.0-rc\.[1-9]$ ]]; then
  # These already-published releases predate captured-image metadata. Do not
  # silently use this compatibility path for a new or unknown release.
  echo "historical release ${VERSION} has no captured image metadata; using its versioned role tags" >&2
  CONTROLLER_IMAGE="ghcr.io/${REPOSITORY}:controller-${VERSION}"
  RUNNER_IMAGE="ghcr.io/${REPOSITORY}:runner-${VERSION}"
  WORKER_IMAGE="ghcr.io/${REPOSITORY}:worker-${VERSION}"
else
  echo "error: release checksum is missing for required ${metadata}" >&2
  exit 1
fi

chart="kova-${VERSION#v}.tgz"
curl --fail --location --retry 5 --retry-all-errors \
  --output "${work_dir}/${chart}" "${release_base}/${chart}"
verify_release_checksum "${chart}"

mkdir "${work_dir}/cli" "${work_dir}/chart"
tar -xzf "${work_dir}/${archive}" -C "${work_dir}/cli" kova LICENSE
"${work_dir}/cli/kova" version | grep -F "${VERSION}"
HELM_REGISTRY_CONFIG="${work_dir}/registry.json" \
  helm pull "oci://ghcr.io/${REPOSITORY%/*}/charts/kova" \
  --version "${VERSION#v}" --destination "${work_dir}/chart"
if ! cmp "${work_dir}/${chart}" "${work_dir}/chart/${chart}"; then
  echo "error: anonymously pulled chart differs from checksummed release chart ${chart}" >&2
  exit 1
fi

# Historical releases may predate retry status fields; this smoke never upgrades to this checkout's controller.
CONTROLLER_IMAGE="${CONTROLLER_IMAGE}" \
RUNNER_IMAGE="${RUNNER_IMAGE}" \
WORKER_IMAGE="${WORKER_IMAGE}" \
KOVA_CHART="${work_dir}/chart/${chart}" \
KOVA_CLI="${work_dir}/cli/kova" \
E2E_SERVICE_BUILD_CLI=false \
E2E_SERVICE_BUILD_IMAGE=false \
KIND_LOAD_IMAGES=false \
START_OBSERVABILITY=false \
VERIFY_RETRY_CRD_SCHEMA=false \
KIND_CLUSTER=kova-release-smoke \
KIND_CONFIG=deploy/quickstart-kind-cluster.yaml \
KIND_KUBECONFIG=.kind/kova-release-smoke.kubeconfig \
KIND_WORKERS=1 \
KIND_VALUES=deploy/quickstart-kind-values.yaml \
KOVA_VALUES="${ROOT}/deploy/quickstart-kind-values.yaml" \
"${ROOT}/scripts/e2e/e2e-service.sh"
