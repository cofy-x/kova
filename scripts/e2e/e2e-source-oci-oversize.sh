#!/usr/bin/env bash

# Service-side #43 acceptance. Reuses the source-capacity Kind and exact-UID
# guard, but publishes an oversized immutable source outside the Kova client.
# Default mode is read-only; live mode retains every source tag and receipt.
set -euo pipefail

# shellcheck source=scripts/common.sh
source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

root=$(repo_root)
cluster=kova-source-capacity
namespace=kova
registry=localhost:5002
source_repo=kova-sources/source-capacity
output_repo=kova-examples/source-capacity
kubeconfig=${root}/.kind/${cluster}.kubeconfig
guard=${root}/scripts/e2e/source-capacity-guard.py
publisher=${root}/scripts/e2e/source-oci-oversize.py
evidence=${root}/scripts/e2e/source-oci-oversize-evidence.py
log_capture=${root}/scripts/e2e/capture-bounded-logs.py
mode=${SOURCE_OCI_OVERSIZE_MODE:-check}
ack=${SOURCE_OCI_OVERSIZE_ACK:-}
run_dir=
submission_possible=false
active_pid=
port_forward_pid=
sampler_pid=
log_follower_pid=
token=
test_secret_uid=

die() { echo "error: $*" >&2; exit 1; }
note() { echo "source-oci-oversize: $*" >&2; }
kctl() { kubectl --kubeconfig "${kubeconfig}" --request-timeout=15s "$@"; }
preflight() {
  SOURCE_CAPACITY_E2E_MODE=check KIND_CLUSTER=${cluster} \
    KIND_KUBECONFIG=.kind/${cluster}.kubeconfig \
    "${root}/scripts/e2e/e2e-source-capacity.sh"
}
run_supervised() {
  local status=0
  "$@" &
  active_pid=$!
  wait "${active_pid}" || status=$?
  active_pid=
  return "${status}"
}
tag_status() {
  local repo=$1 stage=$2 expected=$3 headers code
  headers=${run_dir}/${stage}.headers
  code=$(curl --noproxy '*' --connect-timeout 3 --max-time 10 -sSI \
    -H 'Accept: application/vnd.oci.image.manifest.v1+json' \
    -D "${headers}" -o /dev/null -w '%{http_code}' \
    "http://${registry}/v2/${repo}/manifests/${run_id}") ||
    die "exact ${repo}:${run_id} registry status is unknown"
  printf '%s\n' "${code}" >"${run_dir}/${stage}.status"
  [[ ${code} == "${expected}" ]] || die "exact ${repo}:${run_id} returned HTTP ${code}, expected ${expected}"
}
admission_empty() {
  jq -e '.data["reservations.json"] | fromjson | .active == {}' \
    <<<"$(kctl -n "${namespace}" get configmap kova-service-admission -o json)" >/dev/null ||
    die "active admission ledger is not empty"
  jq -e '.data["queue.json"] | fromjson | .intents == {}' \
    <<<"$(kctl -n "${namespace}" get configmap kova-service-queue-admission -o json)" >/dev/null ||
    die "queue admission ledger is not empty"
}
check_test_auth() {
  local service secret observed_uid observed_token
  service=$(kctl -n "${namespace}" get deployment kova-service -o json)
  jq -e '.spec.template.spec.containers |
    [.[] | select(.name == "kova-service") | .env[]? |
     select(.name == "KOVA_SERVICE_AUTH_TOKEN") | .valueFrom.secretKeyRef] ==
    [{"name":"kova-e2e-token","key":"token"}]' <<<"${service}" >/dev/null ||
    die "Service credential reference changed"
  secret=$(kctl -n "${namespace}" get secret kova-e2e-token -o json)
  jq -e '.metadata.name == "kova-e2e-token" and .metadata.namespace == "kova" and
    .type == "Opaque" and (.metadata.uid | type == "string" and length > 0) and
    (.data.token | type == "string" and length > 0)' <<<"${secret}" >/dev/null ||
    die "disposable Kind Secret identity or data differs"
  observed_uid=$(jq -r '.metadata.uid' <<<"${secret}")
  observed_token=$(jq -r '.data.token | @base64d' <<<"${secret}") ||
    die "cannot decode the disposable Kind credential"
  [[ ${observed_token} == service-e2e-token ]] || die "disposable Kind credential changed"
  if [[ -n ${test_secret_uid} && ${observed_uid} != "${test_secret_uid}" ]]; then
    die "disposable Kind Secret UID changed during the run"
  fi
  test_secret_uid=${observed_uid}
  token=${observed_token}
}
headroom() {
  local stage=$1 workspace_kib temp_kib docker_kib memory_kib docker_root
  workspace_kib=$(df -Pk "${root}" | awk 'NR == 2 {print $4}')
  temp_kib=$(df -Pk "${TMPDIR:-/tmp}" | awk 'NR == 2 {print $4}')
  docker_root=$(docker info --format '{{.DockerRootDir}}')
  [[ -d ${docker_root} ]] || die "Docker data root is unavailable"
  docker_kib=$(df -Pk "${docker_root}" | awk 'NR == 2 {print $4}')
  memory_kib=$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)
  for amount in "${workspace_kib}" "${temp_kib}" "${docker_kib}" "${memory_kib}"; do
    [[ ${amount} =~ ^[0-9]+$ ]] || die "disk or memory headroom is unknown"
  done
  (( workspace_kib >= 20 * 1024 * 1024 && docker_kib >= 20 * 1024 * 1024 &&
     temp_kib >= 2 * 1024 * 1024 && memory_kib >= 8 * 1024 * 1024 )) ||
    die "need 20 GiB workspace/Docker disk, 2 GiB temporary disk, and 8 GiB available memory"
  printf 'workspace_kib=%s\ntemp_kib=%s\ndocker_kib=%s\nmemory_kib=%s\n' \
    "${workspace_kib}" "${temp_kib}" "${docker_kib}" "${memory_kib}" >"${run_dir}/${stage}.headroom.txt"
}
finish() {
  local status=$?
  trap - EXIT
  trap '' HUP INT TERM
  for pid in "${active_pid}" "${sampler_pid}" "${log_follower_pid}" "${port_forward_pid}"; do
    if [[ -n ${pid} ]]; then kill "${pid}" 2>/dev/null || true; wait "${pid}" 2>/dev/null || true; fi
  done
  if [[ ${submission_possible} == true ]]; then
    (( status != 0 )) || status=1
    note "submission may have created ${expected_job_id}; exact UID stop required"
    if ! timeout -k 10s 5m python3 "${guard}" stop "${run_dir}" >"${run_dir}/stop.log" 2>"${run_dir}/stop.err"; then
      note "exact stop did not prove convergence; inspect the saved run before another test"
    else
      note "exact KovaBuild UID stop and admission cleanup verified"
    fi
  fi
  if [[ -n ${run_dir} ]]; then
    note "all source tags and evidence retained at ${run_dir}"
    if (( status != 0 )); then note "FAILED; inspect exact source/output tags and stop receipt"; fi
  fi
  exit "${status}"
}
trap finish EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

[[ ${mode} == check || ${mode} == run ]] || die "SOURCE_OCI_OVERSIZE_MODE must be check or run"
[[ $(uname -s) == Linux ]] || die "only an isolated Linux Kind host may run this test"
for command in docker kubectl jq curl openssl timeout python3 sha256sum; do require_cmd "${command}"; done
preflight
note "read-only preflight passed for the exact empty ${cluster} Kind"
if [[ ${mode} == check ]]; then
  note "set SOURCE_OCI_OVERSIZE_MODE=run and exact SOURCE_OCI_OVERSIZE_ACK to publish one 512 MiB + 1 byte source"
  exit 0
fi
[[ ${ack} == "${cluster}/${namespace}/${source_repo}" ]] ||
  die "acknowledgement must be ${cluster}/${namespace}/${source_repo}"
[[ ! ${SERVICE_AUTH_TOKEN+x} && ! ${KOVA_SERVICE_TOKEN+x} ]] ||
  die "do not pass a Service token in the environment"

umask 077
mkdir -p -- "${root}/.work/source-oci-oversize"
run_id="source-capacity-$(date -u +%Y%m%dt%H%M%Sz)-$(openssl rand -hex 4)"
run_dir=${root}/.work/source-oci-oversize/${run_id}
mkdir -- "${run_dir}"
headroom before
python3 "${guard}" check >"${run_dir}/candidate-images.json" || die "candidate image identity changed"
check_test_auth
expected_job_id=$(printf '%s\0%s' 'kova:e2e' "${run_id}" | sha256sum | awk '{print "idem-" substr($1, 1, 20)}')
printf '%s\n' "${expected_job_id}" >"${run_dir}/expected-job-id.txt"
target="kind-registry:5000/${output_repo}:${run_id}"
source_repository="${registry}/${source_repo}:${run_id}"
jq -n --arg id "${run_id}" --arg cluster "${cluster}" --arg target "${target}" \
  --arg source_repository "${source_repository}" --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  '{run_id:$id,cluster:$cluster,target:$target,source_repository:$source_repository,started_at:$at}' \
  >"${run_dir}/run.json"
tag_status "${source_repo}" source-before 404
tag_status "${output_repo}" output-before 404

note "creating sparse 512 MiB + 1 byte ZIP and streaming its OCI layer to the exact local registry"
run_supervised timeout -k 10s 12m python3 "${publisher}" \
  --run-id "${run_id}" --run-dir "${run_dir}" \
  >"${run_dir}/publish.stdout" 2>"${run_dir}/publish.stderr" ||
  die "source publication failed; inspect exact tag and retained evidence"
source_uri=$(jq -r '.uri // empty' "${run_dir}/source-receipt.json")
source_digest=$(jq -r '.digest // empty' "${run_dir}/source-receipt.json")
source_manifest_digest=${source_uri##*@}
[[ ${source_uri} =~ ^oci://localhost:5002/kova-sources/source-capacity@sha256:[0-9a-f]{64}$ &&
   ${source_digest} =~ ^sha256:[0-9a-f]{64}$ ]] || die "source publisher receipt is not the exact repository and digests"
[[ $(jq -r '.archive_bytes' "${run_dir}/source-publication.json") == 536870913 ]] ||
  die "source publication size is not 512 MiB + 1 byte"
tag_status "${source_repo}" source-after 200
observed_source_manifest=$(awk 'tolower($1) == "docker-content-digest:" {gsub("\r", "", $2); print $2}' "${run_dir}/source-after.headers" | tail -1)
[[ ${observed_source_manifest} == "${source_manifest_digest}" ]] || die "source manifest tag digest drifted"
tag_status "${output_repo}" output-after-publication 404
preflight >"${run_dir}/after-publication-preflight.log" 2>&1 || die "cluster state changed during source publication"
python3 "${guard}" check "${run_dir}/candidate-images.json" >/dev/null || die "candidate/Kind identity changed during source publication"
check_test_auth
headroom after-publication

source_uri="oci://kind-registry:5000/${source_repo}@${source_manifest_digest}"
jq -n --arg run_id "${run_id}" --arg expected_job_id "${expected_job_id}" \
  --arg source_uri "${source_uri}" --arg source_digest "${source_digest}" \
  --arg source_manifest_digest "${source_manifest_digest}" --arg target "${target}" \
  '{run_id:$run_id,expected_job_id:$expected_job_id,source_uri:$source_uri,
   source_digest:$source_digest,source_manifest_digest:$source_manifest_digest,target:$target}' \
  >"${run_dir}/source-contract.json"
kubectl --kubeconfig "${kubeconfig}" -n "${namespace}" port-forward --address 127.0.0.1 \
  svc/kova-service :8080 >"${run_dir}/port-forward.log" 2>&1 &
port_forward_pid=$!
local_port=
deadline=$((SECONDS + 30))
while (( SECONDS < deadline )); do
  kill -0 "${port_forward_pid}" 2>/dev/null || die "Service port-forward exited"
  local_port=$(sed -nE 's/^Forwarding from 127\.0\.0\.1:([0-9]+) -> 8080$/\1/p' "${run_dir}/port-forward.log" | head -1)
  if [[ ${local_port} =~ ^[0-9]+$ ]] && curl --noproxy '*' --connect-timeout 2 --max-time 2 -fsS "http://127.0.0.1:${local_port}/healthz" >/dev/null 2>&1; then break; fi
  sleep 1
done
[[ ${local_port} =~ ^[0-9]+$ ]] || die "Service port-forward did not become healthy"
base="http://127.0.0.1:${local_port}"
check_test_auth

# A timed-out Create can still have committed. From this point the EXIT trap
# invokes the existing full-contract, API-server UID-precondition stop guard.
submission_possible=true
KOVA_SERVICE_TOKEN=${token} run_supervised timeout -k 10s 30s "${root}/bin/kova" --service-url "${base}" \
  job submit --source-digest "${source_digest}" --target "${target}" \
  --platform linux/amd64 --format oci --timeout 900 --oom-cooldown 2m --var KOVA_MARKER=capacity \
  --idempotency-key "${run_id}" "${source_uri}" >"${run_dir}/job.json" 2>"${run_dir}/submit.err" ||
  die "Service submit failed or timed out; exact UID stop will inspect it"
job_id=$(jq -r '.id // empty' "${run_dir}/job.json")
[[ ${job_id} == "${expected_job_id}" ]] || die "Service returned a different deterministic build ID"

controller_pid=$BASHPID
sampler_fail() {
  printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$1" >>"${run_dir}/sampler-errors.txt"
  kill -TERM "${controller_pid}" 2>/dev/null || true
  return 1
}
sample() {
  local now node_json pod_json docker_json
  now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  node_json=$(kctl get nodes -o json) || sampler_fail "node collection failed"
  jq -c --arg at "${now}" '{at:$at,nodes:[.items[] | {name:.metadata.name,
    conditions:[.status.conditions[]? | select(.type == "Ready" or .type == "DiskPressure" or .type == "MemoryPressure" or .type == "PIDPressure") | {type,status}]}]}' \
    <<<"${node_json}" >>"${run_dir}/node-health.jsonl" || sampler_fail "node sample parse failed"
  jq -e --arg control "${cluster}-control-plane" --arg worker "${cluster}-worker" '
    def has($type; $status): [.conditions[] | select(.type == $type and .status == $status)] | length == 1;
    (.nodes | length == 2) and ([.nodes[].name] | sort) == ([$control,$worker] | sort) and
    all(.nodes[]; (.conditions | length == 4) and has("Ready";"True") and
      has("DiskPressure";"False") and has("MemoryPressure";"False") and has("PIDPressure";"False"))
  ' <<<"$(tail -1 "${run_dir}/node-health.jsonl")" >/dev/null || sampler_fail "node pressure or unready state"
  pod_json=$(kctl -n "${namespace}" get pod "kova-job-${job_id}" --ignore-not-found -o json) ||
    sampler_fail "runner collection failed"
  if [[ -n ${pod_json} ]]; then
    printf '%s\n' "${pod_json}" >"${run_dir}/runner-pod-last.json"
    [[ -e ${run_dir}/runner-pod-first.json ]] || printf '%s\n' "${pod_json}" >"${run_dir}/runner-pod-first.json"
    if jq -e 'any((.status.initContainerStatuses // [])[];
      .name == "source-fetch" and (.imageID | type == "string" and length > 0))' \
      <<<"${pod_json}" >/dev/null; then
      printf '%s\n' "${pod_json}" >"${run_dir}/runner-pod-image.json"
    fi
    jq -c --arg at "${now}" '{at:$at,name:.metadata.name,uid:.metadata.uid,node:.spec.nodeName,
      phase:.status.phase,reason:.status.reason,
      initContainerStatuses:[.status.initContainerStatuses[]? | {name,image,imageID,restartCount,state}],
      containerStatuses:[.status.containerStatuses[]? | {name,image,imageID,restartCount,state}]}' \
      <<<"${pod_json}" >>"${run_dir}/runner-pod-samples.jsonl" || sampler_fail "runner sample parse failed"
    jq -e '.status.reason != "Evicted" and .status.phase != "Unknown" and
      (if .status.phase == "Failed" then
        any((.status.initContainerStatuses // [])[];
          .name == "source-fetch" and (.state.terminated.exitCode // 0) > 0 and
          .state.terminated.reason != "OOMKilled")
       else true end) and
      all((.status.initContainerStatuses // [])[];
        .restartCount == 0 and .state.terminated.reason != "OOMKilled") and
      all((.status.containerStatuses // [])[]; .restartCount == 0 and .state.running == null and .state.terminated == null)' \
      <<<"${pod_json}" >/dev/null || sampler_fail "runner restarted, was evicted, or main container started"
  else
    jq -cn --arg at "${now}" '{at:$at,missing:true}' >>"${run_dir}/runner-pod-samples.jsonl"
  fi
  docker_json=$(timeout -k 5s 20s docker stats --no-stream --format '{{json .}}' \
    "${cluster}-control-plane" "${cluster}-worker") || sampler_fail "Docker stats collection failed"
  jq -s -e --arg control "${cluster}-control-plane" --arg worker "${cluster}-worker" '
    length == 2 and ([.[].Name] | sort) == ([$control,$worker] | sort)
  ' <<<"${docker_json}" >/dev/null || sampler_fail "Docker stats omitted a Kind node"
  jq -c --arg at "${now}" '{at:$at,name:.Name,cpu_percent:.CPUPerc,
    memory_usage:.MemUsage,memory_percent:.MemPerc,pids:.PIDs,block_io:.BlockIO}' \
    <<<"${docker_json}" >>"${run_dir}/node-docker-stats.jsonl" || sampler_fail "Docker stats parse failed"
}
(
  while [[ ! -e ${run_dir}/sampler.stop ]]; do sample; sleep 2; done
) &
sampler_pid=$!
(
  log_deadline=$((SECONDS + 120))
  until kctl -n "${namespace}" get pod "kova-job-${job_id}" >/dev/null 2>&1; do
    if (( SECONDS >= log_deadline )); then echo 'source-fetch runner Pod did not appear' >&2; exit 1; fi
    sleep 1
  done
  KOVA_E2E_REDACT_TOKEN=${token} exec python3 "${log_capture}" \
    --stdout "${run_dir}/source-fetch.log" --stderr "${run_dir}/source-fetch.err" \
    --receipt "${run_dir}/source-fetch.capture.json" -- \
    kubectl --kubeconfig "${kubeconfig}" -n "${namespace}" logs -f "pod/kova-job-${job_id}" \
      -c source-fetch --timestamps --pod-running-timeout=120s
) &
log_follower_pid=$!

deadline=$((SECONDS + 600))
while (( SECONDS < deadline )); do
  KOVA_SERVICE_TOKEN=${token} run_supervised timeout -k 5s 20s "${root}/bin/kova" --service-url "${base}" \
    job get "${job_id}" >"${run_dir}/terminal.json" 2>"${run_dir}/get.err" ||
    die "Service job status unavailable during source fetch"
  jq -c --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" '{at:$at,id,status,failure_code,error}' \
    "${run_dir}/terminal.json" >>"${run_dir}/job-status.jsonl"
  status=$(jq -r '.status // empty' "${run_dir}/terminal.json")
  [[ ${status} != succeeded && ${status} != cancelled ]] || die "oversized source unexpectedly reached ${status}"
  [[ ${status} != failed ]] || break
  sleep 2
done
[[ ${status:-} == failed ]] || die "oversized source did not reach a failed terminal state within 600s"
kctl -n "${namespace}" get kovabuild "${job_id}" -o json >"${run_dir}/kovabuild.json" ||
  die "exact terminal KovaBuild expired before its receipt was captured"
terminal_pod=$(kctl -n "${namespace}" get pod "kova-job-${job_id}" --ignore-not-found -o json) ||
  die "terminal runner Pod observation failed"
touch "${run_dir}/sampler.stop"
wait "${sampler_pid}" || die "resource sampler failed"
sampler_pid=
if [[ -n ${terminal_pod} ]]; then
  printf '%s\n' "${terminal_pod}" >"${run_dir}/runner-pod-last.json"
  [[ -e ${run_dir}/runner-pod-first.json ]] ||
    printf '%s\n' "${terminal_pod}" >"${run_dir}/runner-pod-first.json"
  if jq -e 'any((.status.initContainerStatuses // [])[];
    .name == "source-fetch" and (.imageID | type == "string" and length > 0))' \
    <<<"${terminal_pod}" >/dev/null; then
    printf '%s\n' "${terminal_pod}" >"${run_dir}/runner-pod-image.json"
  fi
fi
if [[ -n ${log_follower_pid} ]]; then
  for _ in 1 2 3; do
    if ! kill -0 "${log_follower_pid}" 2>/dev/null; then break; fi
    sleep 1
  done
  if kill -0 "${log_follower_pid}" 2>/dev/null; then kill "${log_follower_pid}" 2>/dev/null || true; fi
  wait "${log_follower_pid}" || true
  log_follower_pid=
fi
# Preserve the terminal CR and init failure first, then stop by exact UID while
# the short quickstart TTL still leaves the CR available for identity checks.
run_supervised timeout -k 10s 5m python3 "${guard}" stop "${run_dir}" \
  >"${run_dir}/stop.log" 2>"${run_dir}/stop.err" ||
  die "exact UID stop failed after expected InvalidSource"
submission_possible=false
kctl -n "${namespace}" get events --field-selector "involvedObject.name=kova-job-${job_id}" -o json \
  >"${run_dir}/runner-events.json" || die "runner events are unavailable"
kctl get nodes -o json >"${run_dir}/nodes-final.json" || die "final node health is unavailable"
tag_status "${output_repo}" output-final 404
tag_status "${source_repo}" source-final 200
observed_source_manifest=$(awk 'tolower($1) == "docker-content-digest:" {gsub("\r", "", $2); print $2}' "${run_dir}/source-final.headers" | tail -1)
[[ ${observed_source_manifest} == "${source_manifest_digest}" ]] || die "source tag changed during Service fetch"
python3 "${evidence}" "${run_dir}" >"${run_dir}/evidence.json" ||
  die "Service-side InvalidSource evidence is incomplete or unsafe"
preflight >"${run_dir}/final-preflight.log" 2>&1 || die "cluster did not return to healthy empty state"
tag_status "${output_repo}" output-after-stop 404
jq -n --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --arg id "${job_id}" \
  --arg source "${source_digest}" --arg manifest "${source_manifest_digest}" \
  '{status:"passed",finished_at:$at,job_id:$id,source_digest:$source,source_manifest_digest:$manifest,
   failure_code:"invalid_source",output_tag_absent:true,exact_uid_stopped:true}' >"${run_dir}/summary.json"
note "PASS: Service runner rejected 512 MiB + 1 byte immutable source as InvalidSource; exact UID stopped"
