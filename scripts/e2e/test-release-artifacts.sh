#!/usr/bin/env bash

# Isolated, cluster-free regression for public artifact identity. Only the
# copied smoke entrypoint and real digest validator run; curl/Helm/Service are
# mocks and can never contact a registry, compile a binary, or create Kind.
set -euo pipefail

root=$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
test_dir=$(mktemp -d)
trap 'rm -rf "${test_dir}"' EXIT
fixture=${test_dir}/fixture
mkdir -p "${fixture}/scripts/e2e" "${fixture}/scripts/release" "${test_dir}/bin" "${test_dir}/cli" "${test_dir}/tmp"
cp "${root}/scripts/common.sh" "${fixture}/scripts/common.sh"
cp "${root}/scripts/e2e/e2e-release.sh" "${fixture}/scripts/e2e/e2e-release.sh"
cp "${root}/scripts/release/capture-image-digests.sh" "${fixture}/scripts/release/capture-image-digests.sh"

cat >"${test_dir}/bin/curl" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
output=''
while (( $# )); do
  case $1 in
    --fail|--location|--retry-all-errors) shift ;;
    --retry) shift 2 ;;
    --output) output=$2; shift 2 ;;
    https://github.com/cofy-x/kova/releases/download/*) url=$1; shift ;;
    *) echo "unexpected curl argument: $1" >&2; exit 90 ;;
  esac
done
test -n "${output}" && test -n "${url:-}"
printf '%s\n' "${url}" >>"${MOCK_FETCH_LOG}"
cp "${MOCK_ASSET_DIR}/${url##*/}" "${output}"
MOCK
cat >"${test_dir}/bin/helm" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
test "$1" = pull
test "$2" = oci://ghcr.io/cofy-x/charts/kova
test "$3" = --version
test "$4" = "${MOCK_VERSION#v}"
test "$5" = --destination
test $# = 6
# A new registry config, rather than any caller's authenticated config, is
# required for this public pull. No existing registry credential is read.
test -n "${HELM_REGISTRY_CONFIG}" && test ! -e "${HELM_REGISTRY_CONFIG}"
cp "${MOCK_OCI_CHART}" "$6/kova-${MOCK_VERSION#v}.tgz"
MOCK
cat >"${fixture}/scripts/e2e/e2e-service.sh" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "${CONTROLLER_IMAGE}" "${RUNNER_IMAGE}" "${WORKER_IMAGE}" >"${MOCK_RESULT}"
test "${E2E_SERVICE_BUILD_CLI}" = false
test "${E2E_SERVICE_BUILD_IMAGE}" = false
test "${KIND_LOAD_IMAGES}" = false
test "${VERIFY_RETRY_CRD_SCHEMA}" = false
cmp "${KOVA_CHART}" "${MOCK_ASSET_DIR}/kova-${MOCK_VERSION#v}.tgz"
test "$("${KOVA_CLI}" version)" = "Kova ${MOCK_VERSION}"
MOCK
cat >"${test_dir}/cli/kova" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
test "$1" = version
printf 'Kova %s\n' "${MOCK_VERSION}"
MOCK
printf 'test license\n' >"${test_dir}/cli/LICENSE"
chmod +x "${test_dir}/bin/curl" "${test_dir}/bin/helm" "${fixture}/scripts/e2e/e2e-service.sh" "${test_dir}/cli/kova"

case "$(uname -s)" in
  Darwin) host_os=darwin; checksum=(shasum -a 256) ;;
  Linux) host_os=linux; checksum=(sha256sum) ;;
  *) echo 'unsupported regression host OS' >&2; exit 2 ;;
esac
case "$(uname -m)" in
  x86_64|amd64) host_arch=amd64 ;;
  arm64|aarch64) host_arch=arm64 ;;
  *) echo 'unsupported regression host architecture' >&2; exit 2 ;;
esac
controller_digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
runner_digest=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
worker_digest=sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc

write_checksums() {
  local file
  : >"${assets}/checksums.txt"
  for file in "${archive}" "${chart}" candidate-images.json; do
    if [[ -f "${assets}/${file}" ]]; then
      printf '%s  ./%s\n' "$("${checksum[@]}" "${assets}/${file}" | awk '{print $1}')" "${file}" >>"${assets}/checksums.txt"
    fi
  done
}

prepare_case() {
  local name=$1
  version=${2:-v0.1.0-rc.10}
  case_dir=${test_dir}/${name}
  assets=${case_dir}/assets
  archive=kova_${version#v}_${host_os}_${host_arch}.tar.gz
  chart=kova-${version#v}.tgz
  mkdir -p "${assets}"
  tar -czf "${assets}/${archive}" -C "${test_dir}/cli" kova LICENSE
  printf 'checksummed chart %s\n' "${version}" >"${assets}/${chart}"
  jq -n --arg controller "${controller_digest}" --arg runner "${runner_digest}" --arg worker "${worker_digest}" \
    '{controller:{"containerimage.digest":$controller,"containerimage.config.digest":$worker},
      runner:{"containerimage.digest":$runner,"containerimage.config.digest":$controller},
      worker:{"containerimage.digest":$worker,"containerimage.config.digest":$runner}}' >"${assets}/candidate-images.json"
  write_checksums
  oci_chart=${assets}/${chart}
}

run_case() {
  local expected=$1 status=0
  env -i PATH="${test_dir}/bin:${PATH}" TMPDIR="${test_dir}/tmp" \
    MOCK_ASSET_DIR="${assets}" MOCK_OCI_CHART="${oci_chart}" MOCK_VERSION="${version}" \
    MOCK_RESULT="${case_dir}/service.result" MOCK_FETCH_LOG="${case_dir}/fetch.log" \
    CONTROLLER_IMAGE=mutable.invalid/controller:latest RUNNER_IMAGE=mutable.invalid/runner:latest \
    WORKER_IMAGE=mutable.invalid/worker:latest \
    bash "${fixture}/scripts/e2e/e2e-release.sh" "${version}" >"${case_dir}/output" 2>&1 || status=$?
  if [[ "${expected}" == pass ]]; then
    if (( status != 0 )); then
      echo "release artifact mock failed (${case_dir##*/}): $(<"${case_dir}/output")" >&2
      exit 1
    fi
    test -f "${case_dir}/service.result"
  else
    if (( status == 0 )) || [[ -e "${case_dir}/service.result" ]]; then
      echo "unsafe release artifact reached Service (${case_dir##*/})" >&2
      exit 1
    fi
  fi
}

prepare_case captured-digests
run_case pass
printf 'ghcr.io/cofy-x/kova@%s\n' "${controller_digest}" "${runner_digest}" "${worker_digest}" >"${case_dir}/expected"
cmp "${case_dir}/expected" "${case_dir}/service.result"

prepare_case historical-fallback v0.1.0-rc.9
rm "${assets}/candidate-images.json"
write_checksums
run_case pass
printf 'ghcr.io/cofy-x/kova:%s-%s\n' controller "${version}" runner "${version}" worker "${version}" >"${case_dir}/expected"
cmp "${case_dir}/expected" "${case_dir}/service.result"
grep -F 'historical release v0.1.0-rc.9 has no captured image metadata' "${case_dir}/output" >/dev/null

for tag in v0.1.0-rc.10 v0.1.0 v0.2.0-rc.1; do
  prepare_case "missing-metadata-${tag}" "${tag}"
  rm "${assets}/candidate-images.json"
  write_checksums
  run_case fail
  grep -F 'missing for required candidate-images.json' "${case_dir}/output" >/dev/null
done

for expression in 'del(.runner)' '.worker["containerimage.digest"]="sha256:no"' 'del(.controller["containerimage.digest"])'; do
  prepare_case "invalid-metadata-${RANDOM}"
  jq "${expression}" "${assets}/candidate-images.json" >"${case_dir}/changed.json"
  mv "${case_dir}/changed.json" "${assets}/candidate-images.json"
  write_checksums
  run_case fail
done

for asset in metadata chart cli; do
  prepare_case "checksum-mismatch-${asset}"
  case ${asset} in
    metadata) printf 'tampered\n' >>"${assets}/candidate-images.json" ;;
    chart) printf 'tampered\n' >>"${assets}/${chart}" ;;
    cli) printf 'tampered\n' >>"${assets}/${archive}" ;;
  esac
  run_case fail
  grep -F 'release checksum mismatch' "${case_dir}/output" >/dev/null
done

prepare_case unsigned-chart
awk -v file="./${chart}" '$2 != file' "${assets}/checksums.txt" >"${case_dir}/changed-checksums.txt"
mv "${case_dir}/changed-checksums.txt" "${assets}/checksums.txt"
run_case fail

prepare_case duplicate-metadata-checksum
duplicate_checksum=$(awk '$2 == "./candidate-images.json" {print}' "${assets}/checksums.txt")
printf '%s\n' "${duplicate_checksum}" >>"${assets}/checksums.txt"
run_case fail

prepare_case anonymous-chart-mismatch
oci_chart=${case_dir}/different-chart.tgz
printf 'different OCI chart\n' >"${oci_chart}"
run_case fail
grep -F 'anonymously pulled chart differs' "${case_dir}/output" >/dev/null

prepare_case missing-public-metadata
rm "${assets}/candidate-images.json"
run_case fail

for tag in 'v0.1.0/../../other' 'v0.1.0-rc.10?other=tag' 'v0.1.0;false'; do
  prepare_case "invalid-tag-${RANDOM}"
  version=${tag}
  run_case fail
  test ! -e "${case_dir}/fetch.log"
  grep -F 'usage:' "${case_dir}/output" >/dev/null
done

echo 'release artifact mocks verify captured image digests, public chart identity, legacy fallback, and fail-closed inputs'
