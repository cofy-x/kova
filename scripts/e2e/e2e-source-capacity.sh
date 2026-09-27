#!/usr/bin/env bash

# #43 acceptance against an existing, isolated Kind quickstart cluster. The
# default mode is read-only. Live mode creates one run-scoped source and output;
# it never installs, resets, deletes, or switches a Kubernetes cluster.
set -euo pipefail

# shellcheck source=scripts/common.sh
source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

root=$(repo_root)
mode=${SOURCE_CAPACITY_E2E_MODE:-check}
cluster=kova-source-capacity
namespace=kova
release=kova
kubeconfig=${root}/.kind/${cluster}.kubeconfig
registry_name=kind-registry
registry_host=localhost:5002
cluster_registry=kind-registry:5000
registry_image=registry:2@sha256:a3d8aaa63ed8681a604f1dea0aa03f100d5895b6a58ace528858a7b332415373
token=
test_secret_uid=
expected_kubeconfig=
port_forward_pid=
sampler_pid=
log_follower_pid=
supervised_pid=
run_dir=
job_id=
submission_possible=false
guard=${root}/scripts/e2e/source-capacity-guard.py
log_capture=${root}/scripts/e2e/capture-bounded-logs.py
evidence_guard=${root}/scripts/e2e/source-capacity-evidence.py

die() { echo "error: $*" >&2; exit 1; }
note() { echo "source-capacity-e2e: $*" >&2; }
kctl() { kubectl --kubeconfig "${kubeconfig}" --request-timeout=15s "$@"; }
run_supervised() {
  local outcome=0
  "$@" &
  supervised_pid=$!
  wait "${supervised_pid}" || outcome=$?
  supervised_pid=
  return "${outcome}"
}
admission_empty() {
  local active queue
  active=$(kctl -n "${namespace}" get configmap kova-service-admission -o json)
  queue=$(kctl -n "${namespace}" get configmap kova-service-queue-admission -o json)
  jq -e '.data["reservations.json"] | fromjson | .active == {}' <<<"${active}" >/dev/null || die "active admission ledger is not empty"
  jq -e '.data["queue.json"] | fromjson | .intents == {}' <<<"${queue}" >/dev/null || die "queue admission ledger is not empty"
}
check_test_auth() {
  local secret observed_uid observed_token
  jq -e '.spec.template.spec.containers |
    [.[] | select(.name == "kova-service") | .env[]? |
      select(.name == "KOVA_SERVICE_AUTH_TOKEN") | .valueFrom.secretKeyRef] ==
    [{"name":"kova-e2e-token","key":"token"}]' <<<"${service}" >/dev/null ||
    die "Service is not bound to the exact disposable Kind token Secret"
  secret=$(kctl -n "${namespace}" get secret kova-e2e-token -o json)
  jq -e '.metadata.name == "kova-e2e-token" and .metadata.namespace == "kova" and
    .type == "Opaque" and (.metadata.uid | type == "string" and length > 0) and
    (.data.token | type == "string" and length > 0)' <<<"${secret}" >/dev/null ||
    die "disposable Kind token Secret identity or data differs"
  observed_uid=$(jq -r '.metadata.uid' <<<"${secret}")
  observed_token=$(jq -r '.data.token | @base64d' <<<"${secret}") ||
    die "disposable Kind token cannot be decoded"
  [[ ${observed_token} == service-e2e-token ]] ||
    die "disposable Kind token value differs from the fixed test credential"
  if [[ -n ${test_secret_uid} && ${observed_uid} != "${test_secret_uid}" ]]; then
    die "disposable Kind token Secret UID changed during the run"
  fi
  test_secret_uid=${observed_uid}
  token=${observed_token}
}

cleanup() {
  local status=$?
  trap - EXIT
  trap '' HUP INT TERM
  if [[ -n ${supervised_pid} ]]; then kill "${supervised_pid}" 2>/dev/null || true; wait "${supervised_pid}" 2>/dev/null || true; fi
  if [[ -n ${sampler_pid} ]]; then kill "${sampler_pid}" 2>/dev/null || true; wait "${sampler_pid}" 2>/dev/null || true; fi
  if [[ -n ${log_follower_pid} ]]; then kill "${log_follower_pid}" 2>/dev/null || true; wait "${log_follower_pid}" 2>/dev/null || true; fi
  if [[ -n ${port_forward_pid} ]]; then kill "${port_forward_pid}" 2>/dev/null || true; wait "${port_forward_pid}" 2>/dev/null || true; fi
  if [[ -n ${expected_kubeconfig} ]]; then rm -f -- "${expected_kubeconfig}"; fi
  if [[ ${submission_possible} == true ]]; then
    (( status != 0 )) || status=1
    note "submission may have created ${expected_job_id}; attempting only exact-ID/UID supervised stop"
    if ! timeout -k 10s 5m python3 "${guard}" stop "${run_dir}" >"${run_dir}/stop.log" 2>"${run_dir}/stop.err"; then
      note "exact stop did not prove convergence; inspect stop.err and exact run ID before another test"
    else
      note "exact KovaBuild stop and admission cleanup verified; registry tags and evidence retained"
    fi
  fi
  if [[ -n ${run_dir} ]]; then
    note "evidence and run-scoped registry tags retained at ${run_dir}"
    if (( status != 0 )); then note "FAILED; inspect the exact run before recovery or tag cleanup"; fi
  fi
  exit "${status}"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

[[ ${mode} == check || ${mode} == run ]] || die "SOURCE_CAPACITY_E2E_MODE must be check or run"
[[ $(uname -s) == Linux ]] || die "this acceptance test runs only on an isolated Linux Kind host"
[[ ${KIND_CLUSTER:-${cluster}} == "${cluster}" ]] || die "KIND_CLUSTER must be ${cluster}"
[[ ${KIND_KUBECONFIG:-.kind/${cluster}.kubeconfig} == ".kind/${cluster}.kubeconfig" ]] || die "KIND_KUBECONFIG must be .kind/${cluster}.kubeconfig"
[[ ${NAMESPACE:-${namespace}} == "${namespace}" && ${RELEASE_NAME:-${release}} == "${release}" ]] || die "the test owns only the kova release in the kova namespace"
[[ ${REGISTRY_HOST:-${registry_host}} == "${registry_host}" && ${CLUSTER_REGISTRY:-${cluster_registry}} == "${cluster_registry}" && ${REGISTRY_NAME:-${registry_name}} == "${registry_name}" && ${REGISTRY_PORT:-5002} == 5002 ]] || die "registry overrides do not match the isolated quickstart registry"
require_cmd docker
require_cmd kind
require_cmd kubectl
require_cmd helm
require_cmd jq
require_cmd curl
require_cmd openssl
require_cmd timeout
require_cmd sha256sum
require_cmd python3
[[ -f ${kubeconfig} && ! -L ${kubeconfig} ]] || die "missing regular dedicated kubeconfig: ${kubeconfig}"
[[ -x ${root}/bin/kova ]] || die "build the Linux CLI with make kova before this test"

# Refuse a stale or cloud kubeconfig even if its current-context was renamed.
[[ $(kind get clusters) == "${cluster}" ]] || die "the host must contain exactly one Kind cluster named ${cluster}"
expected_kubeconfig=$(mktemp "${TMPDIR:-/tmp}/kova-source-capacity-kubeconfig.XXXXXX")
kind get kubeconfig --name "${cluster}" >"${expected_kubeconfig}"
context=$(kubectl --kubeconfig "${kubeconfig}" config current-context)
[[ ${context} == "kind-${cluster}" ]] || die "unexpected kubeconfig context: ${context}"
identity() {
  kubectl --kubeconfig "$1" config view --raw --minify -o json |
    jq -Sc '{context:."current-context",cluster:.clusters[0].cluster,user:.users[0].user}' |
    sha256sum | awk '{print $1}'
}
[[ $(identity "${kubeconfig}") == "$(identity "${expected_kubeconfig}")" ]] || die "dedicated kubeconfig differs from the current Kind cluster credentials/server"

nodes=$(kctl get nodes -o json)
jq -e '.items | length == 2 and all(.[];
  ([.status.conditions[]? | select(.type == "Ready" and .status == "True")] | length) == 1 and
  ([.status.conditions[]? | select((.type == "DiskPressure" or .type == "MemoryPressure" or .type == "PIDPressure") and .status != "False")] | length) == 0)' <<<"${nodes}" >/dev/null || die "the dedicated control-plane and worker must both be Ready without pressure"
[[ $(docker_arch) == amd64 ]] || die "this fixed-size acceptance recipe currently requires a Linux/amd64 Docker host"
registry=$(docker inspect "${registry_name}" --format '{{json .}}') || die "dedicated local registry is missing"
jq -e --arg image "${registry_image}" '
  .Config.Image == $image and .State.Running == true and
  .NetworkSettings.Networks.kind != null and
  ([.NetworkSettings.Ports["5000/tcp"][]? | select(.HostIp == "127.0.0.1" and .HostPort == "5002")] | length) == 1
' <<<"${registry}" >/dev/null || die "kind-registry image, port binding, network, or running state differs from the quickstart contract"
curl --noproxy '*' --connect-timeout 3 --max-time 5 -fsS "http://${registry_host}/v2/" >/dev/null || die "the local registry API is unhealthy"

helm status "${release}" --kubeconfig "${kubeconfig}" -n "${namespace}" -o json |
  jq -e '.info.status == "deployed"' >/dev/null || die "the kova Helm release is not deployed"
for deployment in "${release}" "${release}-service"; do
  observed=$(kctl -n "${namespace}" get deployment "${deployment}" -o json)
  jq -e '.spec.replicas >= 1 and .status.readyReplicas == .spec.replicas and .status.updatedReplicas == .spec.replicas and .status.observedGeneration == .metadata.generation' <<<"${observed}" >/dev/null || die "deployment ${deployment} is not fully Ready"
done
service=$(kctl -n "${namespace}" get deployment "${release}-service" -o json)
jq -e --arg namespace "${namespace}" --arg registry "${cluster_registry}" '
  [.spec.template.spec.containers[] | select(.name == "kova-service") | .args[]] as $args |
  ["--namespace=" + $namespace, "--auth-mode=static", "--auth-static-principal=kova:e2e", "--registry-plain-http=" + $registry] |
  all(. as $arg | ($args | index($arg)) != null)
' <<<"${service}" >/dev/null || die "Service is not the isolated authenticated quickstart configuration"
[[ ! ${SERVICE_AUTH_TOKEN+x} && ! ${KOVA_SERVICE_TOKEN+x} ]] ||
  die "do not pass a Service token in the environment; the test reads its exact disposable Kind Secret"
check_test_auth
jq -e '.items | length == 0' <<<"$(kctl get kovabuilds -A -o json)" >/dev/null || die "another KovaBuild exists; do not overlap tests"
jq -e '.items | length == 0' <<<"$(kctl get pods -A -l app.kubernetes.io/name=kova-runner -o json)" >/dev/null || die "a runner Pod exists; do not overlap tests"
admission_empty
node_uids=$(jq -r '.items[].metadata.uid' <<<"${nodes}" | sort)
registry_id=$(jq -r '.Id' <<<"${registry}")

note "read-only preflight passed: ${cluster}, 2/2 Ready nodes, empty KovaBuild/runner set, exact local registry"
python3 "${guard}" check >/dev/null || die "candidate checkout/CLI/image/Pod CRI identity preflight failed"
if [[ ${mode} == check ]]; then
  note "candidate revision and all role Pod/CRI identities passed; no cluster writes performed"
  note "set SOURCE_CAPACITY_E2E_MODE=run to execute with the exact disposable Kind Secret"
  exit 0
fi
free_kib=$(df -Pk "${root}" | awk 'NR == 2 {print $4}')
available_mem_kib=$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)
tmp_free_kib=$(df -Pk "${TMPDIR:-/tmp}" | awk 'NR == 2 {print $4}')
docker_root=$(docker info --format '{{.DockerRootDir}}')
[[ -d ${docker_root} ]] || die "cannot locate Docker's data root"
docker_free_kib=$(df -Pk "${docker_root}" | awk 'NR == 2 {print $4}')
[[ ${free_kib} =~ ^[0-9]+$ && ${tmp_free_kib} =~ ^[0-9]+$ && ${docker_free_kib} =~ ^[0-9]+$ && ${available_mem_kib} =~ ^[0-9]+$ ]] || die "cannot determine host disk or memory headroom"
(( free_kib >= 20 * 1024 * 1024 && docker_free_kib >= 20 * 1024 * 1024 && available_mem_kib >= 8 * 1024 * 1024 )) || die "need at least 20 GiB free workspace/Docker disk and 8 GiB available memory before the 128 MiB source test"
(( tmp_free_kib >= 1024 * 1024 )) || die "need at least 1 GiB free temporary storage for source packaging"

umask 077
mkdir -p "${root}/.work/source-capacity"
run_id="source-capacity-$(date -u +%Y%m%dt%H%M%Sz)-$(openssl rand -hex 4)"
run_dir=${root}/.work/source-capacity/${run_id}
mkdir -- "${run_dir}" || die "run directory already exists"
mkdir -- "${run_dir}/source"
python3 "${guard}" check >"${run_dir}/candidate-images.json" || die "candidate role image identity changed before live mode"
expected_job_id=$(printf '%s\0%s' 'kova:e2e' "${run_id}" | sha256sum | awk '{print "idem-" substr($1, 1, 20)}')
printf '%s\n' "${expected_job_id}" >"${run_dir}/expected-job-id.txt"
source_repository="${registry_host}/kova-sources/source-capacity:${run_id}"
target="${cluster_registry}/kova-examples/source-capacity:${run_id}"
pull_target="${registry_host}/kova-examples/source-capacity:${run_id}"
jq -n --arg id "${run_id}" --arg cluster "${cluster}" --arg context "${context}" \
  --arg source_repository "${source_repository}" --arg target "${target}" --arg pull_target "${pull_target}" \
  --arg started "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --argjson disk_free_kib "${free_kib}" \
  --argjson memory_available_kib "${available_mem_kib}" --argjson tmp_free_kib "${tmp_free_kib}" \
  --argjson docker_free_kib "${docker_free_kib}" \
  '{run_id:$id,started_at:$started,cluster:$cluster,context:$context,source_repository:$source_repository,
   target:$target,pull_target:$pull_target,disk_free_kib:$disk_free_kib,tmp_free_kib:$tmp_free_kib,
   docker_free_kib:$docker_free_kib,memory_available_kib:$memory_available_kib}' >"${run_dir}/run.json"
# A collision or registry error must stop before either the source or output
# repository can be overwritten. Tags are random, but absence is verified.
for repository in kova-sources/source-capacity kova-examples/source-capacity; do
  tag_status=$(curl --noproxy '*' --connect-timeout 3 --max-time 10 -sSI -o /dev/null -w '%{http_code}' \
    -H 'Accept: application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json' \
    "http://${registry_host}/v2/${repository}/manifests/${run_id}") || die "cannot check existing ${repository}:${run_id} tag"
  [[ ${tag_status} == 404 ]] || die "${repository}:${run_id} is not confirmed absent (HTTP ${tag_status})"
done
# shellcheck disable=SC2016 # The placeholder must remain literal for Kova's Dockerfile variable substitution.
printf 'FROM scratch\nCOPY payload /payload\nLABEL kova-test="${KOVA_MARKER}"\n' >"${run_dir}/source/Dockerfile"
note "creating a bounded 128 MiB incompressible source payload in ${run_dir}"
dd if=/dev/urandom of="${run_dir}/source/payload" bs=1M count=128 status=none
sha256sum "${run_dir}/source/Dockerfile" "${run_dir}/source/payload" >"${run_dir}/source-sha256.txt"

run_supervised timeout -k 10s 10m "${root}/bin/kova" source push --target "${target}" --platform linux/amd64 \
  --repository "${source_repository}" --registry-plain-http "${registry_host}" \
  "${run_dir}/source" >"${run_dir}/source-receipt.json"
source_uri=$(jq -r '.uri // empty' "${run_dir}/source-receipt.json")
source_digest=$(jq -r '.digest // empty' "${run_dir}/source-receipt.json")
[[ ${source_uri} =~ ^oci://localhost:5002/kova-sources/source-capacity@sha256:[a-f0-9]{64}$ && ${source_digest} =~ ^sha256:[a-f0-9]{64}$ ]] || die "source push did not return the exact repository and two digest receipts"
source_manifest_digest=${source_uri##*@}
curl --noproxy '*' --connect-timeout 3 --max-time 10 -fsSI \
  -H 'Accept: application/vnd.oci.image.manifest.v1+json' \
  -D "${run_dir}/source-tag.headers" \
  "http://${registry_host}/v2/kova-sources/source-capacity/manifests/${run_id}" >/dev/null || die "source tag is not readable from the exact local registry"
observed_source_manifest=$(awk 'tolower($1) == "docker-content-digest:" {gsub("\r", "", $2); print $2}' "${run_dir}/source-tag.headers" | tail -1)
[[ ${observed_source_manifest} == "${source_manifest_digest}" ]] || die "source tag digest drifted after push"
source_uri="oci://${cluster_registry}/${source_uri#oci://"${registry_host}"/}"

# Refuse late overlap or replacement while the large source was prepared and
# pushed. This is a dedicated cluster, never a shared production namespace.
[[ $(kind get clusters) == "${cluster}" ]] || die "Kind cluster identity changed during source publication"
kind get kubeconfig --name "${cluster}" >"${expected_kubeconfig}"
[[ $(identity "${kubeconfig}") == "$(identity "${expected_kubeconfig}")" ]] || die "Kind kubeconfig changed during source publication"
[[ $(jq -r '.Id' <<<"$(docker inspect "${registry_name}" --format '{{json .}}')") == "${registry_id}" ]] || die "local registry container changed during source publication"
late_nodes=$(kctl get nodes -o json)
[[ $(jq -r '.items[].metadata.uid' <<<"${late_nodes}" | sort) == "${node_uids}" ]] || die "Kind nodes changed during source publication"
jq -e '.items | length == 2 and all(.[];
  ([.status.conditions[]? | select(.type == "Ready" and .status == "True")] | length) == 1 and
  ([.status.conditions[]? | select((.type == "DiskPressure" or .type == "MemoryPressure" or .type == "PIDPressure") and .status != "False")] | length) == 0)' <<<"${late_nodes}" >/dev/null || die "Kind nodes became unhealthy during source publication"
jq -e '.items | length == 0' <<<"$(kctl get kovabuilds -A -o json)" >/dev/null || die "another KovaBuild started while the source was prepared"
jq -e '.items | length == 0' <<<"$(kctl get pods -A -l app.kubernetes.io/name=kova-runner -o json)" >/dev/null || die "a runner Pod started while the source was prepared"
admission_empty
service=$(kctl -n "${namespace}" get deployment "${release}-service" -o json)
check_test_auth
python3 "${guard}" check "${run_dir}/candidate-images.json" >/dev/null || die "candidate role image/Pod CRI identity changed during source publication"

# The dynamic local port avoids colliding with unrelated development services.
kubectl --kubeconfig "${kubeconfig}" -n "${namespace}" port-forward --address 127.0.0.1 \
  "svc/${release}-service" :8080 >"${run_dir}/port-forward.log" 2>&1 &
port_forward_pid=$!
local_port=
deadline=$((SECONDS + 30))
while (( SECONDS < deadline )); do
  kill -0 "${port_forward_pid}" 2>/dev/null || die "Service port-forward exited"
  local_port=$(sed -nE 's/^Forwarding from 127\.0\.0\.1:([0-9]+) -> 8080$/\1/p' "${run_dir}/port-forward.log" | head -1)
  if [[ ${local_port} =~ ^[0-9]+$ ]] && curl --noproxy '*' --connect-timeout 2 --max-time 2 -fsS "http://127.0.0.1:${local_port}/healthz" >/dev/null 2>&1; then break; fi
  sleep 1
done
[[ ${local_port} =~ ^[0-9]+$ ]] || die "Service port-forward did not select a local port"
curl --noproxy '*' --connect-timeout 2 --max-time 2 -fsS "http://127.0.0.1:${local_port}/healthz" >/dev/null || die "Service port-forward did not become healthy"
base="http://127.0.0.1:${local_port}"

jq -n --arg run_id "${run_id}" --arg expected_job_id "${expected_job_id}" \
  --arg source_uri "${source_uri}" --arg source_digest "${source_digest}" \
  --arg source_manifest_digest "${source_manifest_digest}" --arg target "${target}" \
  '{run_id:$run_id,expected_job_id:$expected_job_id,source_uri:$source_uri,
   source_digest:$source_digest,source_manifest_digest:$source_manifest_digest,target:$target}' \
  >"${run_dir}/source-contract.json"
# From here on, a timeout/signal/non-JSON response does not prove the Service
# did not create its deterministic ID. The EXIT trap supervises exact recovery.
submission_possible=true
KOVA_SERVICE_TOKEN=${token} run_supervised timeout -k 10s 30s "${root}/bin/kova" --service-url "${base}" \
  job submit --source-digest "${source_digest}" --target "${target}" \
  --platform linux/amd64 --format oci --timeout 900 --oom-cooldown 2m --var KOVA_MARKER=capacity \
  --idempotency-key "${run_id}" "${source_uri}" >"${run_dir}/job.json"
job_id=$(jq -r '.id // empty' "${run_dir}/job.json")
[[ ${job_id} == "${expected_job_id}" ]] || die "Service did not return the expected idempotent build ID"
printf '%s\n' "${job_id}" >"${run_dir}/job-id.txt"
note "submitted ${job_id}; sampling Kind state and verifying runner image identities"

controller_pid=$BASHPID
[[ ! -e ${run_dir}/sampler.stop && ! -L ${run_dir}/sampler.stop ]] || die "sampler stop marker already exists"
sampler_fail() {
  printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$1" >>"${run_dir}/sampler-errors.txt"
  kill -TERM "${controller_pid}" 2>/dev/null || true
  return 1
}
sample() {
  local now node_json node_sample runner_json runner_sample docker_json
  now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  if ! node_json=$(kctl get nodes -o json 2>/dev/null); then
    sampler_fail "node collection failed"
  fi
  if ! node_sample=$(jq -c --arg at "${now}" '{at:$at,nodes:[.items[] | {name:.metadata.name,
      conditions:[.status.conditions[]? | select(.type == "Ready" or .type == "DiskPressure" or .type == "MemoryPressure" or .type == "PIDPressure") | {type,status,reason}]}]}' \
    <<<"${node_json}"); then
    sampler_fail "node sample parse failed"
  fi
  printf '%s\n' "${node_sample}" >>"${run_dir}/node-health.jsonl"
  if ! jq -e --arg control "${cluster}-control-plane" --arg worker "${cluster}-worker" '
    def has($type; $status): [.conditions[] | select(.type == $type and .status == $status)] | length == 1;
    (.nodes | length == 2) and
    ([.nodes[].name] | sort) == ([$control, $worker] | sort) and
    all(.nodes[]; (.conditions | length == 4) and has("Ready"; "True") and
      has("DiskPressure"; "False") and has("MemoryPressure"; "False") and
      has("PIDPressure"; "False"))
  ' <<<"${node_sample}" >/dev/null; then
    sampler_fail "node became unready or pressured during the build"
  fi
  if ! runner_json=$(kctl -n "${namespace}" get pod "kova-job-${job_id}" --ignore-not-found -o json 2>/dev/null); then
    sampler_fail "runner collection failed"
  elif [[ -z ${runner_json} ]]; then
    jq -cn --arg at "${now}" '{at:$at,missing:true}' >>"${run_dir}/runner-pod-samples.jsonl"
  elif ! runner_sample=$(jq -c --arg at "${now}" '{at:$at,name:.metadata.name,uid:.metadata.uid,node:.spec.nodeName,
      phase:.status.phase,reason:.status.reason,
      initContainerStatuses:[.status.initContainerStatuses[]? | {name,ready,restartCount,state}],
      containerStatuses:[.status.containerStatuses[]? | {name,ready,restartCount,state}]}' \
      <<<"${runner_json}"); then
    sampler_fail "runner sample parse failed"
  else
    printf '%s\n' "${runner_sample}" >>"${run_dir}/runner-pod-samples.jsonl"
    if ! jq -e '
      .phase != "Failed" and .phase != "Unknown" and .reason != "Evicted" and
      all((.initContainerStatuses + .containerStatuses)[];
        .restartCount == 0 and (.state.terminated.exitCode // 0) == 0 and
        .state.terminated.reason != "OOMKilled" and
        (.state.waiting.reason as $reason |
          ["CrashLoopBackOff", "ImagePullBackOff", "ErrImagePull", "CreateContainerError",
           "RunContainerError", "InvalidImageName", "OOMKilled"] | index($reason) == null))
    ' <<<"${runner_sample}" >/dev/null; then
      sampler_fail "runner failed or restarted during the build"
    fi
  fi
  if ! docker_json=$(timeout -k 5s 20s docker stats --no-stream --format '{{json .}}' \
    "${cluster}-control-plane" "${cluster}-worker" 2>/dev/null); then
    sampler_fail "Docker stats collection failed"
  fi
  if ! jq -s -e --arg control "${cluster}-control-plane" --arg worker "${cluster}-worker" '
    length == 2 and ([.[].Name] | sort) == ([$control, $worker] | sort)
  ' <<<"${docker_json}" >/dev/null; then
    sampler_fail "Docker stats omitted a Kind node"
  fi
  if ! jq -c --arg at "${now}" '{at:$at,name:.Name,cpu_percent:.CPUPerc,
      memory_usage:.MemUsage,memory_percent:.MemPerc,pids:.PIDs,block_io:.BlockIO}' \
    <<<"${docker_json}" >>"${run_dir}/node-docker-stats.jsonl"; then
    sampler_fail "Docker stats parse failed"
  fi
}
(
  while [[ ! -e ${run_dir}/sampler.stop ]]; do sample; sleep 5; done
) &
sampler_pid=$!

# Terminal Service logs intentionally return 410. Follow the exact runner Pod
# while it exists so the ephemeral build output is still retained in evidence.
(
  log_deadline=$((SECONDS + 300))
  until kctl -n "${namespace}" get pod "kova-job-${job_id}" >/dev/null 2>&1; do
    if (( SECONDS >= log_deadline )); then echo 'runner Pod did not appear within 5 minutes' >&2; exit 1; fi
    sleep 1
  done
  KOVA_E2E_REDACT_TOKEN=${token} exec python3 "${log_capture}" \
    --stdout "${run_dir}/runner-follow.log" --stderr "${run_dir}/runner-follow.err" \
    --receipt "${run_dir}/runner-follow.capture.json" -- \
    kubectl --kubeconfig "${kubeconfig}" -n "${namespace}" logs -f "pod/kova-job-${job_id}" \
      -c runner --timestamps --pod-running-timeout=120s
) &
log_follower_pid=$!
run_supervised timeout -k 10s 4m python3 "${guard}" runner "${run_dir}" >/dev/null || die "runner source-fetch/main image identity was not proven"

wait_ok=true
KOVA_SERVICE_TOKEN=${token} run_supervised timeout -k 10s 21m "${root}/bin/kova" --service-url "${base}" \
  job wait --timeout 20m "${job_id}" >"${run_dir}/terminal.json" 2>"${run_dir}/wait.err" || wait_ok=false
results_ok=true
if [[ ${wait_ok} == true ]]; then
  KOVA_SERVICE_TOKEN=${token} run_supervised timeout -k 10s 30s "${root}/bin/kova" --service-url "${base}" \
    job results "${job_id}" >"${run_dir}/results.json" 2>"${run_dir}/results.err" || results_ok=false
fi
KOVA_SERVICE_TOKEN=${token} KOVA_E2E_REDACT_TOKEN=${token} \
  run_supervised timeout -k 10s 30s python3 "${log_capture}" \
    --stdout "${run_dir}/job-logs.txt" --stderr "${run_dir}/job-logs.err" \
    --receipt "${run_dir}/job-logs.capture.json" -- \
    "${root}/bin/kova" --service-url "${base}" job logs --tail 2000 "${job_id}" || true
kctl -n "${namespace}" get kovabuild "${job_id}" -o json >"${run_dir}/kovabuild.json" 2>"${run_dir}/kovabuild.err" || true
kctl -n "${namespace}" get pod "kova-job-${job_id}" -o json >"${run_dir}/runner-pod.json" 2>"${run_dir}/runner-pod.err" || true
kctl -n "${namespace}" get events --field-selector "involvedObject.name=kova-job-${job_id}" -o json >"${run_dir}/runner-events.json" 2>"${run_dir}/runner-events.err" || true
KOVA_E2E_REDACT_TOKEN=${token} python3 "${log_capture}" \
  --stdout "${run_dir}/runner-logs.txt" --stderr "${run_dir}/runner-logs.err" \
  --receipt "${run_dir}/runner-logs.capture.json" -- \
  kubectl --kubeconfig "${kubeconfig}" --request-timeout=15s -n "${namespace}" \
    logs "pod/kova-job-${job_id}" --all-containers --timestamps || true
if [[ -n ${log_follower_pid} ]]; then
  kill "${log_follower_pid}" 2>/dev/null || true
  wait "${log_follower_pid}" 2>/dev/null || true
  log_follower_pid=
fi
touch "${run_dir}/sampler.stop"
wait "${sampler_pid}" || die "resource sampler stopped unexpectedly"
sampler_pid=
kctl get nodes -o json >"${run_dir}/nodes-final.json" || die "cannot read final Kind node health"
if [[ ${wait_ok} != true ]]; then die "job wait did not return a terminal receipt; inspect ${run_dir}"; fi
if [[ ${results_ok} != true ]]; then die "job results did not return a durable output receipt; inspect ${run_dir}"; fi
jq -e '.status == "succeeded"' "${run_dir}/terminal.json" >/dev/null || die "source-capacity build did not succeed"
observed_output_digest=$(python3 "${evidence_guard}" result "${run_dir}") ||
  die "result is missing the exact verified OCI target and immutable reference"
jq -e '.items | length == 2 and all(.[];
  ([.status.conditions[]? | select(.type == "Ready" and .status == "True")] | length) == 1 and
  ([.status.conditions[]? | select((.type == "DiskPressure" or .type == "MemoryPressure" or .type == "PIDPressure") and .status != "False")] | length) == 0)' "${run_dir}/nodes-final.json" >/dev/null || die "Kind nodes were unhealthy after the build"
python3 "${evidence_guard}" monitoring "${run_dir}" >"${run_dir}/monitoring-evidence.json" ||
  die "build monitoring or bounded runner log evidence is incomplete or unhealthy"
jq -e --arg id "${job_id}" --arg ns "${namespace}" '
  all(.items[]; .metadata.name == $id and .metadata.namespace == $ns)
' <<<"$(kctl get kovabuilds -A -o json)" >/dev/null || die "an unrelated KovaBuild appeared during this isolated run"

curl --noproxy '*' --connect-timeout 3 --max-time 10 -fsSI \
  -H 'Accept: application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json' \
  -D "${run_dir}/output-tag.headers" \
  "http://${registry_host}/v2/kova-examples/source-capacity/manifests/${run_id}" >/dev/null || die "output tag is not readable from the exact local registry"
registry_output_digest=$(awk 'tolower($1) == "docker-content-digest:" {gsub("\r", "", $2); print $2}' "${run_dir}/output-tag.headers" | tail -1)
[[ ${registry_output_digest} == "${observed_output_digest}" ]] || die "output tag digest drifted from the verified Service result"
run_supervised timeout -k 10s 5m docker pull "${pull_target}" >"${run_dir}/pull.log" 2>&1 || die "host pull of exact output tag failed"
docker image inspect "${pull_target}" --format '{{json .RepoDigests}}' >"${run_dir}/pull-digests.json"
jq -e --arg suffix "@${observed_output_digest}" 'any(.[]; endswith($suffix))' "${run_dir}/pull-digests.json" >/dev/null || die "host-pulled output digest differs from the verified Service result"

# The quickstart uses a short job TTL. Wait for normal controller cleanup, but
# never delete a CR or Pod here: persistence is itself part of the acceptance.
cleanup_deadline=$((SECONDS + 120))
while (( SECONDS < cleanup_deadline )); do
  if ! kctl -n "${namespace}" get kovabuild "${job_id}" >/dev/null 2>&1 &&
     ! kctl -n "${namespace}" get pod "kova-job-${job_id}" >/dev/null 2>&1; then break; fi
  sleep 5
done
[[ ! $(kctl -n "${namespace}" get kovabuild "${job_id}" --ignore-not-found -o name) ]] || die "exact KovaBuild did not expire after its TTL"
[[ ! $(kctl -n "${namespace}" get pod "kova-job-${job_id}" --ignore-not-found -o name) ]] || die "exact runner Pod remains after terminal cleanup"
jq -e '.items | length == 0' <<<"$(kctl get kovabuilds -A -o json)" >/dev/null || die "unexpected KovaBuild remains after terminal cleanup"
jq -e '.items | length == 0' <<<"$(kctl get pods -A -l app.kubernetes.io/name=kova-runner -o json)" >/dev/null || die "runner Pod remains after terminal cleanup"
active_ledger=$(kctl -n "${namespace}" get configmap kova-service-admission -o json)
queue_ledger=$(kctl -n "${namespace}" get configmap kova-service-queue-admission -o json)
jq -e '.data["reservations.json"] | fromjson | (.active | length) == 0' <<<"${active_ledger}" >/dev/null || die "active admission grant remains after terminal cleanup"
jq -e '.data["queue.json"] | fromjson | (.intents | length) == 0' <<<"${queue_ledger}" >/dev/null || die "queue admission intent remains after terminal cleanup"
submission_possible=false
jq -n --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --arg id "${job_id}" \
  --arg source "${source_digest}" --arg source_manifest "${source_manifest_digest}" \
  --arg output "${observed_output_digest}" \
  '{finished_at:$at,status:"passed",job_id:$id,source_digest:$source,source_manifest_digest:$source_manifest,output_manifest_digest:$output}' \
  >"${run_dir}/summary.json"
note "PASS: 128 MiB source built and host-pulled with verified ${observed_output_digest}"
note "No KovaBuild, source tag, output tag, Kind cluster, or Docker image was automatically removed"
