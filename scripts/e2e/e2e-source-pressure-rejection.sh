#!/usr/bin/env bash

# Companion to e2e-source-capacity.sh: reject four bounded invalid source
# archives before publication. It never creates a cluster or submits a build.
set -euo pipefail

# shellcheck source=scripts/common.sh
source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

root=$(repo_root)
cluster=kova-source-capacity
namespace=kova
repository=kova-sources/source-pressure-rejection
registry=localhost:5002
kubeconfig=${root}/.kind/${cluster}.kubeconfig
mode=${SOURCE_PRESSURE_E2E_MODE:-check}
ack=${SOURCE_PRESSURE_E2E_ACK:-}
run_dir=
current_fixture=
success=false
active_pid=

die() { echo "error: $*" >&2; exit 1; }
note() { echo "source-pressure-e2e: $*" >&2; }
preflight() {
  SOURCE_CAPACITY_E2E_MODE=check KIND_CLUSTER=${cluster} \
    KIND_KUBECONFIG=.kind/${cluster}.kubeconfig \
    "${root}/scripts/e2e/e2e-source-capacity.sh"
}
tag_status() {
  local tag=$1 stage=$2 headers error_file code
  headers=${run_dir}/${stage}.headers
  error_file=${run_dir}/${stage}.curl.err
  if ! code=$(curl --noproxy '*' --connect-timeout 3 --max-time 10 -sSI \
    -H 'Accept: application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json' \
    -o "${headers}" -w '%{http_code}' \
    "http://${registry}/v2/${repository}/manifests/${tag}" 2>"${error_file}"); then
    die "registry lookup failed for exact ${repository}:${tag}; preserve evidence"
  fi
  printf '%s\n' "${code}" >"${run_dir}/${stage}.status"
  [[ ${code} == 404 ]] || die "exact ${repository}:${tag} was not absent (HTTP ${code}); preserve it for inspection"
}
headroom() {
  local workspace_kib temp_kib docker_kib memory_kib docker_root
  workspace_kib=$(df -Pk "${root}" | awk 'NR == 2 {print $4}')
  temp_kib=$(df -Pk "${run_dir}/tmp" | awk 'NR == 2 {print $4}')
  docker_root=$(docker info --format '{{.DockerRootDir}}')
  [[ -d ${docker_root} ]] || die "Docker data root is unavailable"
  docker_kib=$(df -Pk "${docker_root}" | awk 'NR == 2 {print $4}')
  memory_kib=$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)
  for value in "${workspace_kib}" "${temp_kib}" "${docker_kib}" "${memory_kib}"; do
    [[ ${value} =~ ^[0-9]+$ ]] || die "cannot determine disk or memory headroom"
  done
  (( workspace_kib >= 20 * 1024 * 1024 && docker_kib >= 20 * 1024 * 1024 &&
     temp_kib >= 2 * 1024 * 1024 && memory_kib >= 8 * 1024 * 1024 )) ||
    die "need 20 GiB free workspace/Docker disk, 2 GiB temporary disk, and 8 GiB available memory"
  printf 'workspace_kib=%s\ntemp_kib=%s\ndocker_kib=%s\nmemory_kib=%s\n' \
    "${workspace_kib}" "${temp_kib}" "${docker_kib}" "${memory_kib}"
}
check_identity() {
  local current_nodes current_registry current_kubeconfig current_binary
  current_nodes=$(kubectl --kubeconfig "${kubeconfig}" --request-timeout=15s get nodes -o json |
    jq -r '.items[].metadata.uid' | sort)
  current_registry=$(docker inspect kind-registry --format '{{.Id}}')
  current_kubeconfig=$(sha256sum "${kubeconfig}" | awk '{print $1}')
  current_binary=$(sha256sum "${root}/bin/kova" | awk '{print $1}')
  [[ ${current_nodes} == "${node_uids}" && ${current_registry} == "${registry_id}" &&
     ${current_kubeconfig} == "${kubeconfig_sha}" && ${current_binary} == "${binary_sha}" ]] ||
    die "Kind node, local registry, kubeconfig, or Kova CLI identity changed during run"
}
finish() {
  local status=$?
  trap - EXIT HUP INT TERM
  trap '' HUP INT TERM
  if [[ -n ${active_pid} ]]; then
    kill "${active_pid}" 2>/dev/null || true
    wait "${active_pid}" 2>/dev/null || true
  fi
  if [[ -n ${run_dir} ]]; then
    if [[ ${success} == true && ${status} == 0 ]]; then
      note "PASS: all rejection receipts at ${run_dir}; no test source tags or KovaBuilds created"
    else
      note "FAILED: evidence at ${run_dir}; retained current fixture ${current_fixture:-none}; inspect any exact run tag before cleanup"
    fi
  fi
  exit "${status}"
}
wait_active() {
  local status=0
  wait "${active_pid}" || status=$?
  active_pid=
  return "${status}"
}
trap finish EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

[[ ${mode} == check || ${mode} == run ]] || die "SOURCE_PRESSURE_E2E_MODE must be check or run"
require_cmd python3
require_cmd timeout
require_cmd sha256sum
require_cmd jq
require_cmd curl
require_cmd openssl
[[ -x /usr/bin/time ]] || die "GNU /usr/bin/time is required for peak RSS receipts"
preflight
head_sha=$(git -C "${root}" rev-parse HEAD)
[[ -z $(git -C "${root}" status --porcelain --untracked-files=normal) ]] || die "candidate checkout must be clean"
cli_version=$("${root}/bin/kova" version)
[[ ${cli_version} == *"(commit ${head_sha:0:12}, "* ]] || die "Kova CLI is not built from exact candidate commit ${head_sha}"
note "read-only preflight passed: exact Kind, registry, empty workloads and candidate CLI ${head_sha}"
if [[ ${mode} == check ]]; then
  note "no registry or Kubernetes writes; set SOURCE_PRESSURE_E2E_MODE=run and exact SOURCE_PRESSURE_E2E_ACK to execute"
  exit 0
fi
[[ ${ack} == "${cluster}/${namespace}/${repository}" ]] ||
  die "acknowledgement must be ${cluster}/${namespace}/${repository}"

umask 077
mkdir -p -- "${root}/.work/source-pressure-rejection"
run_id="source-pressure-$(date -u +%Y%m%dt%H%M%Sz)-$(openssl rand -hex 4)"
run_dir=${root}/.work/source-pressure-rejection/${run_id}
mkdir -- "${run_dir}"
mkdir -- "${run_dir}/tmp"
export TMPDIR=${run_dir}/tmp
node_uids=$(kubectl --kubeconfig "${kubeconfig}" --request-timeout=15s get nodes -o json |
  jq -r '.items[].metadata.uid' | sort)
registry_id=$(docker inspect kind-registry --format '{{.Id}}')
kubeconfig_sha=$(sha256sum "${kubeconfig}" | awk '{print $1}')
binary_sha=$(sha256sum "${root}/bin/kova" | awk '{print $1}')
printf 'candidate_commit=%s\ncli_version=%s\ncli_sha256=%s\nkubeconfig_sha256=%s\nregistry_id=%s\nnode_uids=%s\n' \
  "${head_sha}" "${cli_version}" "${binary_sha}" "${kubeconfig_sha}" "${registry_id}" "${node_uids}" \
  >"${run_dir}/identity.txt"

for case_name in symlink-parent dockerfile-limit expanded-limit compressed-limit; do
  preflight >"${run_dir}/${case_name}.preflight.log" 2>&1 || die "preflight failed before ${case_name}"
  check_identity
  headroom >"${run_dir}/${case_name}.headroom.txt"
  tag=${run_id}-${case_name}
  tag_status "${tag}" "${case_name}.before"
  current_fixture=${run_dir}/${case_name}.zip
  note "generating bounded ${case_name} fixture"
  case ${case_name} in
    symlink-parent|dockerfile-limit) file_limit_kib=4096 ;;
    expanded-limit) file_limit_kib=65536 ;;
    compressed-limit) file_limit_kib=526336 ;;
  esac
  (
    ulimit -f "${file_limit_kib}"
    exec timeout --signal=TERM --kill-after=5s 180s /usr/bin/time -v \
      -o "${run_dir}/${case_name}.generate.time.txt" \
      python3 "${root}/scripts/e2e/source-pressure-fixtures.py" \
        --case "${case_name}" --output "${current_fixture}" --tag "${tag}"
  ) >"${run_dir}/${case_name}.generate.stdout" 2>"${run_dir}/${case_name}.generate.stderr" &
  active_pid=$!
  if wait_active; then
    :
  else
    die "fixture generation failed or exceeded 180s for ${case_name}"
  fi
  generation_rss_kib=$(awk -F: '/Maximum resident set size \(kbytes\)/ {gsub(/[[:space:]]/, "", $2); print $2}' \
    "${run_dir}/${case_name}.generate.time.txt")
  [[ ${generation_rss_kib} =~ ^[0-9]+$ ]] || die "${case_name} generator peak RSS is unavailable"
  (( generation_rss_kib <= 256 * 1024 )) || die "${case_name} generator exceeded 256 MiB peak RSS"
  fixture_bytes=$(stat -c '%s' "${current_fixture}")
  fixture_disk_kib=$(du -Pk "${current_fixture}" | awk '{print $1}')
  [[ ${fixture_bytes} =~ ^[0-9]+$ && ${fixture_disk_kib} =~ ^[0-9]+$ ]] ||
    die "cannot measure ${case_name} fixture size"
  printf 'fixture_bytes=%s\nfixture_disk_kib=%s\ngenerator_max_rss_kib=%s\n' \
    "${fixture_bytes}" "${fixture_disk_kib}" "${generation_rss_kib}" \
    >"${run_dir}/${case_name}.fixture.txt"
  case ${case_name} in
    symlink-parent|dockerfile-limit) (( fixture_bytes <= 4 * 1024 * 1024 && fixture_disk_kib <= 4 * 1024 )) ;;
    expanded-limit) (( fixture_bytes <= 64 * 1024 * 1024 && fixture_disk_kib <= 64 * 1024 )) ;;
    compressed-limit) (( fixture_bytes == 512 * 1024 * 1024 + 1 && fixture_disk_kib <= 16 * 1024 )) ;;
  esac || die "${case_name} fixture exceeded its size budget"
  sha256sum "${current_fixture}" >>"${run_dir}/${case_name}.fixture.txt"

  # The source destination is hard-coded to the local Kind registry. A buggy
  # client may unexpectedly create the tag, so verify absence even on failure.
  (
    ulimit -f 540672
    exec timeout --signal=TERM --kill-after=5s 90s /usr/bin/time -v \
      -o "${run_dir}/${case_name}.time.txt" \
      "${root}/bin/kova" source push \
        --repository "${registry}/${repository}:${tag}" --registry-plain-http "${registry}" \
        "${current_fixture}"
  ) >"${run_dir}/${case_name}.push.stdout" 2>"${run_dir}/${case_name}.push.stderr" &
  active_pid=$!
  if wait_active; then
    code=0
  else
    code=$?
  fi
  printf '%s\n' "${code}" >"${run_dir}/${case_name}.push.exit"
  tag_status "${tag}" "${case_name}.after"
  [[ ${code} != 0 && ${code} != 124 && ${code} != 137 && ${code} != 143 ]] ||
    die "${case_name} source push succeeded, timed out, or was interrupted"
  case ${case_name} in
    symlink-parent) expected='symlink parent' ;;
    dockerfile-limit) expected='Dockerfile exceeds 1 MiB limit' ;;
    expanded-limit) expected='source archive exceeds 2 GiB expanded size limit' ;;
    compressed-limit) expected='source archive exceeds 512 MiB compressed size limit' ;;
  esac
  grep -F -- "${expected}" "${run_dir}/${case_name}.push.stderr" >/dev/null ||
    die "${case_name} failed for an unexpected reason; inspect the retained fixture and stderr"
  rss_kib=$(awk -F: '/Maximum resident set size \(kbytes\)/ {gsub(/[[:space:]]/, "", $2); print $2}' \
    "${run_dir}/${case_name}.time.txt")
  [[ ${rss_kib} =~ ^[0-9]+$ ]] || die "${case_name} peak RSS is unavailable"
  printf 'max_rss_kib=%s\n' "${rss_kib}" >>"${run_dir}/${case_name}.fixture.txt"
  (( rss_kib <= 512 * 1024 )) || die "${case_name} CLI exceeded 512 MiB peak RSS"
  preflight >"${run_dir}/${case_name}.after-preflight.log" 2>&1 ||
    die "cluster or Kova workload state changed after ${case_name}"
  check_identity
  headroom >"${run_dir}/${case_name}.after-headroom.txt"
  [[ -z $(find "${run_dir}/tmp" -mindepth 1 -maxdepth 1 -print -quit) ]] ||
    die "${case_name} left a source push snapshot in the run-scoped temp directory"
  rm -f -- "${current_fixture}"
  current_fixture=
  note "${case_name} rejected as expected with exact tag absent and ${rss_kib} KiB peak RSS"
done

rmdir -- "${run_dir}/tmp"
printf 'status=passed\nfinished_at=%s\ncase_count=4\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >"${run_dir}/summary.txt"
success=true
