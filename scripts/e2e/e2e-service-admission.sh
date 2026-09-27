#!/usr/bin/env bash

# #44 acceptance against an existing, dedicated Kind cluster. The default
# mode is read-only. Live mode creates only idempotent, run-scoped KovaBuilds;
# it never installs, scales, resets, or deletes a cluster or admission ledger.
set -euo pipefail

# shellcheck source=scripts/common.sh
source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

root=$(repo_root)
mode=${ADMISSION_E2E_MODE:-check}
cluster=${KIND_CLUSTER:-}
namespace=${NAMESPACE:-kova}
release=${RELEASE_NAME:-kova}
port_a=${ADMISSION_E2E_PORT_A:-18081}
port_b=${ADMISSION_E2E_PORT_B:-18082}
token=${SERVICE_AUTH_TOKEN:-}

die() { echo "error: $*" >&2; exit 1; }
note() { echo "admission-e2e: $*" >&2; }
kctl() { kubectl --kubeconfig "${kubeconfig}" "$@"; }

[[ ${mode} == check || ${mode} == run ]] || die "ADMISSION_E2E_MODE must be check or run"
[[ ${cluster} == kova-admission-* ]] || die "set KIND_CLUSTER to an existing dedicated kova-admission-* Kind cluster"
[[ ${port_a} =~ ^[0-9]+$ && ${port_b} =~ ^[0-9]+$ && ${port_a} != "${port_b}" ]] || die "choose two distinct local TCP ports"
require_cmd kind
require_cmd kubectl
require_cmd jq
require_cmd curl
require_cmd openssl

kubeconfig=${KIND_KUBECONFIG:-.kind/${cluster}.kubeconfig}
if [[ ${kubeconfig} != /* ]]; then
  kubeconfig=${root}/${kubeconfig}
fi
[[ -f ${kubeconfig} ]] || die "Kind kubeconfig does not exist: ${kubeconfig}"
kind get clusters | awk -v expected="${cluster}" '$0 == expected { found = 1 } END { exit !found }' || die "Kind cluster ${cluster} does not exist"
context=$(kubectl --kubeconfig "${kubeconfig}" config current-context)
[[ ${context} == "kind-${cluster}" ]] || die "kubeconfig context ${context} is not kind-${cluster}"

# No live write occurs until every check below has passed.
deployment=$(kctl -n "${namespace}" get deployment "${release}-service" -o json)
jq -e '.spec.replicas == 2 and .status.replicas == 2 and .status.readyReplicas == 2 and .status.updatedReplicas == 2 and .status.observedGeneration == .metadata.generation' <<<"${deployment}" >/dev/null || die "Service deployment is not exactly two fully updated, ready replicas"
jq -e --arg namespace "${namespace}" '
  [.spec.template.spec.containers[] | select(.name == "kova-service") | .args[]] as $args |
  ["--namespace=" + $namespace,
   "--max-active-jobs=1", "--max-active-jobs-per-requester=1", "--worker-slots=1",
   "--max-queued-jobs=3", "--max-queued-jobs-per-requester=2",
   "--runner-node-selector=never=true", "--auth-mode=static", "--leader-elect=true"] |
  all(. as $arg | ($args | index($arg)) != null)
' <<<"${deployment}" >/dev/null || die "deployment args do not have the exact admission caps, static auth, leader election, and unschedulable runner selector"
principal=$(jq -r '[.spec.template.spec.containers[] | select(.name == "kova-service") | .args[] | select(startswith("--auth-static-principal=")) | sub("^--auth-static-principal="; "")] | if length == 1 then .[0] else empty end' <<<"${deployment}")
[[ -n ${principal} ]] || die "cannot determine the one static authenticated principal"
platform=$(jq -r '[.spec.template.spec.containers[] | select(.name == "kova-service") | .args[] | select(startswith("--buildkit-platform-addr=")) | split("=")[1]] | first // empty' <<<"${deployment}")
[[ ${platform} == linux/amd64 || ${platform} == linux/arm64 ]] || die "cannot determine a supported build platform"
jq -e '.items | length == 0' <<<"$(kctl get nodes -l never=true -o json)" >/dev/null || die "a node matches never=true; blocker Pod could run"

selector="app.kubernetes.io/instance=${release},app.kubernetes.io/component=service"
pods=$(kctl -n "${namespace}" get pods -l "${selector}" -o json)
jq -e '.items | length == 2 and all(.[]; .metadata.deletionTimestamp == null and ([.status.conditions[]? | select(.type == "Ready" and .status == "True")] | length == 1))' <<<"${pods}" >/dev/null || die "not exactly two ready, nonterminating Service Pods"
jq -e '[.items[].status.containerStatuses[]? | select(.name == "kova-service") | .imageID] | length == 2 and .[0] != "" and .[0] == .[1]' <<<"${pods}" >/dev/null || die "Service Pods do not run the same image ID"
pod_a=$(jq -r '[.items[].metadata.name] | sort | .[0]' <<<"${pods}")
pod_b=$(jq -r '[.items[].metadata.name] | sort | .[1]' <<<"${pods}")

jq -e '.items | length == 0' <<<"$(kctl -n "${namespace}" get kovabuilds -o json)" >/dev/null || die "runner namespace contains KovaBuilds; use a fresh dedicated namespace instead of cleanup"
jq -e '.items | length == 0' <<<"$(kctl -n "${namespace}" get pods -l app.kubernetes.io/name=kova-runner -o json)" >/dev/null || die "runner namespace contains runner Pods"
active=$(kctl -n "${namespace}" get configmap kova-service-admission -o json)
queue=$(kctl -n "${namespace}" get configmap kova-service-queue-admission -o json)
jq -e '.data["reservations.json"] | fromjson | .version == 1 and .maxJobs == 1 and .maxPerRequester == 1 and .workerSlots == 1 and (.active | length) == 0' <<<"${active}" >/dev/null || die "active ledger is missing, occupied, or configured differently"
jq -e '.data["queue.json"] | fromjson | .version == 1 and .globalLimit == 3 and .requesterLimit == 2 and (.intents | length) == 0' <<<"${queue}" >/dev/null || die "queue ledger is missing, occupied, or configured differently"

note "read-only preflight passed for ${cluster}/${namespace}: ${pod_a}, ${pod_b}; active=1, queued global=3/requester=2"
if [[ ${mode} == check ]]; then
  note "no cluster writes performed; set ADMISSION_E2E_MODE=run and SERVICE_AUTH_TOKEN to execute"
  exit 0
fi
[[ -n ${token} ]] || die "SERVICE_AUTH_TOKEN is required in live mode"

work_dir=$(mktemp -d "${TMPDIR:-/tmp}/kova-admission-e2e.XXXXXX")
run_id=$(date -u +%Y%m%dt%H%M%sz)-$(openssl rand -hex 4)
source_digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
source_uri="oci://kind-registry:5000/kova-sources/admission-${run_id}@${source_digest}"
target="kind-registry:5000/kova-admission/probe-${run_id}:dev"
blocker_key="${run_id}-blocker"
created_ids=()
port_forward_a=
port_forward_b=
success=false

build_id() {
  printf '%s\0%s' "${principal}" "$1" | openssl dgst -sha256 | awk '{ print "idem-" substr($NF, 1, 20) }'
}

cleanup() {
  local exit_code=$?
  trap - EXIT
  if [[ -n ${port_forward_a} ]]; then kill "${port_forward_a}" 2>/dev/null || true; wait "${port_forward_a}" 2>/dev/null || true; fi
  if [[ -n ${port_forward_b} ]]; then kill "${port_forward_b}" 2>/dev/null || true; wait "${port_forward_b}" 2>/dev/null || true; fi
  if [[ ${success} == true && ${exit_code} == 0 ]]; then
    note "PASS: run-scoped request and response receipts preserved at ${work_dir}"
  else
    note "FAILED: preserving evidence in ${work_dir}; test-owned CRs were NOT automatically deleted"
    note "known test-owned IDs: ${created_ids[*]:-none}; run ID: ${run_id}"
    note "all candidate keys/IDs are in ${work_dir}/candidates.txt"
    note "inspect exact CRs, Pod nonces, and both ledgers before any manual recovery; never delete a ledger"
  fi
  exit "${exit_code}"
}
trap cleanup EXIT

kubectl --kubeconfig "${kubeconfig}" -n "${namespace}" port-forward --address 127.0.0.1 "pod/${pod_a}" "${port_a}:8080" >"${work_dir}/port-forward-a.log" 2>&1 &
port_forward_a=$!
kubectl --kubeconfig "${kubeconfig}" -n "${namespace}" port-forward --address 127.0.0.1 "pod/${pod_b}" "${port_b}:8080" >"${work_dir}/port-forward-b.log" 2>&1 &
port_forward_b=$!

wait_port() {
  local port=$1 pid=$2
  local deadline=$((SECONDS + 30))
  while (( SECONDS < deadline )); do
    kill -0 "${pid}" 2>/dev/null || die "port-forward for ${port} exited"
    if curl --noproxy '*' --connect-timeout 2 --max-time 2 -fsS "http://127.0.0.1:${port}/healthz" >/dev/null 2>&1; then
      return
    fi
    sleep 1
  done
  die "port-forward for ${port} did not become healthy"
}
wait_port "${port_a}" "${port_forward_a}"
wait_port "${port_b}" "${port_forward_b}"

post_build() {
  local port=$1 key=$2 name=$3
  local payload
  payload=$(jq -nc --arg uri "${source_uri}" --arg digest "${source_digest}" --arg target "${target}" --arg platform "${platform}" --arg key "${key}" \
    '{source_uri:$uri,source_digest:$digest,targets:[{target:$target,platform:$platform}],format:"oci",concurrency:1,idempotency_key:$key}')
  if ! curl --noproxy '*' --connect-timeout 3 --max-time 15 -sS -X POST \
    -H "Authorization: Bearer ${token}" -H 'Content-Type: application/json' \
    --data-binary "${payload}" -D "${work_dir}/${name}.headers" -o "${work_dir}/${name}.body" \
    -w '%{http_code}' "http://127.0.0.1:${port}/v1/builds" >"${work_dir}/${name}.status"; then
    echo transport_error >"${work_dir}/${name}.status"
    return 1
  fi
}

assert_build_response_id() {
  local name=$1 expected=$2
  local observed
  observed=$(awk 'tolower($1) == "x-kova-build-id:" { gsub("\r", "", $2); print $2 }' "${work_dir}/${name}.headers")
  [[ ${observed} == "${expected}" ]] || die "${name} returned Build ID ${observed}, expected ${expected}"
}

blocker_id=$(build_id "${blocker_key}")
created_ids+=("${blocker_id}")
printf '%s %s\n' "${blocker_key}" "${blocker_id}" >"${work_dir}/candidates.txt"
post_build "${port_a}" "${blocker_key}" blocker-a & blocker_a_pid=$!
post_build "${port_b}" "${blocker_key}" blocker-b & blocker_b_pid=$!
blocker_failed=false
wait "${blocker_a_pid}" || blocker_failed=true
wait "${blocker_b_pid}" || blocker_failed=true
[[ ${blocker_failed} == false ]] || die "blocker transport failed"
blocker_a_status=$(<"${work_dir}/blocker-a.status")
blocker_b_status=$(<"${work_dir}/blocker-b.status")
[[ ( ${blocker_a_status} == 202 && ( ${blocker_b_status} == 200 || ${blocker_b_status} == 503 ) ) || ( ${blocker_b_status} == 202 && ( ${blocker_a_status} == 200 || ${blocker_a_status} == 503 ) ) ]] || die "concurrent same-key submissions did not produce exactly one 202 (${blocker_a_status}, ${blocker_b_status})"
assert_build_response_id blocker-a "${blocker_id}"
assert_build_response_id blocker-b "${blocker_id}"
for name in blocker-a blocker-b; do
  if [[ $(<"${work_dir}/${name}.status") == 503 ]]; then
    jq -e '.code == "queue_admission_pending"' "${work_dir}/${name}.body" >/dev/null || die "${name} returned unrelated 503"
  fi
done

deadline=$((SECONDS + 60))
while (( SECONDS < deadline )); do
  if kctl -n "${namespace}" get kovabuild "${blocker_id}" -o json 2>/dev/null | jq -e --arg key "${blocker_key}" --arg uri "${source_uri}" '.spec.idempotencyKey == $key and .spec.source.uri == $uri and .status.phase == "Starting"' >/dev/null; then
    break
  fi
  sleep 1
done
(( SECONDS < deadline )) || die "blocker did not reach Starting with this run's source and idempotency key"
jq -e '.spec.nodeSelector.never == "true" and .status.phase == "Pending"' <<<"$(kctl -n "${namespace}" get pod "kova-job-${blocker_id}" -o json)" >/dev/null || die "blocker runner is not an unschedulable Pending Pod"
note "blocker ${blocker_id} holds the one active slot"

pids=()
for i in $(seq 1 40); do
  port=${port_a}
  if (( i % 2 == 0 )); then port=${port_b}; fi
  key="${run_id}-burst-${i}"
  printf '%s %s\n' "${key}" "$(build_id "${key}")" >>"${work_dir}/candidates.txt"
  (
    while [[ ! -e ${work_dir}/go ]]; do sleep 0.01; done
    post_build "${port}" "${key}" "burst-${i}"
  ) &
  pids+=("$!")
done
touch "${work_dir}/go"
burst_failed=false
for pid in "${pids[@]}"; do wait "${pid}" || burst_failed=true; done
[[ ${burst_failed} == false ]] || die "burst request transport failed"

accepted=0
rejected=0
accepted_keys=()
for i in $(seq 1 40); do
  status=$(<"${work_dir}/burst-${i}.status")
  key="${run_id}-burst-${i}"
  case ${status} in
    202)
      accepted=$((accepted + 1))
      accepted_keys+=("${key}")
      id=$(build_id "${key}")
      created_ids+=("${id}")
      assert_build_response_id "burst-${i}" "${id}"
      ;;
    429)
      rejected=$((rejected + 1))
      jq -e '.code == "queue_capacity_exceeded" and .retryable == true' "${work_dir}/burst-${i}.body" >/dev/null || die "burst-${i} returned unrelated 429"
      ;;
    *) die "burst-${i} returned unexpected HTTP ${status}" ;;
  esac
done
(( accepted > 0 && accepted <= 2 && accepted + rejected == 40 )) || die "40-way burst violated queue cap or made no progress: accepted=${accepted}, rejected=${rejected}"

# A heavily contended burst may fill only one of the two queue positions. A
# subsequent single request must fill the remaining position without exceeding
# the same cap; this avoids a false pass from 40 contention-only 429s.
if (( accepted == 1 )); then
  fill_key="${run_id}-fill"
  printf '%s %s\n' "${fill_key}" "$(build_id "${fill_key}")" >>"${work_dir}/candidates.txt"
  post_build "${port_a}" "${fill_key}" fill || die "sequential queue fill transport failed"
  [[ $(<"${work_dir}/fill.status") == 202 ]] || die "sequential request could not fill the remaining queue slot"
  accepted_keys+=("${fill_key}")
  id=$(build_id "${fill_key}")
  created_ids+=("${id}")
  assert_build_response_id fill "${id}"
fi

queue=$(kctl -n "${namespace}" get configmap kova-service-queue-admission -o json)
active=$(kctl -n "${namespace}" get configmap kova-service-admission -o json)
expected_queue_ids=$(printf '%s\n' "${created_ids[@]:1}" | sort)
actual_queue_ids=$(jq -r '.data["queue.json"] | fromjson | .intents | keys[]' <<<"${queue}" | sort)
[[ ${actual_queue_ids} == "${expected_queue_ids}" ]] || die "queue ledger identities differ from the two accepted requests"
jq -e '.data["queue.json"] | fromjson | (.intents | length) == 2' <<<"${queue}" >/dev/null || die "queue ledger does not hold exactly two accepted builds"
jq -e --arg id "${blocker_id}" '.data["reservations.json"] | fromjson | (.active | length) == 1 and ([.active[].buildName] == [$id]) and ([.active[].slots] == [1])' <<<"${active}" >/dev/null || die "active ledger differs from the one blocker grant"
for key in "${accepted_keys[@]}"; do
  id=$(build_id "${key}")
  jq -e --arg key "${key}" --arg uri "${source_uri}" '.spec.idempotencyKey == $key and .spec.source.uri == $uri and (.status.phase == "Queued" or .status.phase == "") and (.metadata.annotations["kova.cofy.dev/queue-intent"] | length) == 32' <<<"$(kctl -n "${namespace}" get kovabuild "${id}" -o json)" >/dev/null || die "accepted queued CR ${id} does not match this run"
done

# Existing-idempotent retries must return the same CR despite a full queue.
post_build "${port_a}" "${blocker_key}" replay-a & replay_a_pid=$!
post_build "${port_b}" "${blocker_key}" replay-b & replay_b_pid=$!
replay_failed=false
wait "${replay_a_pid}" || replay_failed=true
wait "${replay_b_pid}" || replay_failed=true
[[ ${replay_failed} == false ]] || die "replay transport failed"
for name in replay-a replay-b; do
  [[ $(<"${work_dir}/${name}.status") == 200 ]] || die "${name} failed full-queue idempotent replay"
  assert_build_response_id "${name}" "${blocker_id}"
  jq -e --arg id "${blocker_id}" '.id == $id' "${work_dir}/${name}.body" >/dev/null || die "${name} returned a different build"
done
jq -e '.items | length == 3' <<<"$(kctl -n "${namespace}" get kovabuilds -o json)" >/dev/null || die "idempotent replay created an extra CR"
note "PASS: 40-way two-replica burst accepted ${accepted}, rejected ${rejected}; exactly two queued plus one active; replay returned the original CR"

# Cleanup is intentionally exact-name only. If any deletion is uncertain,
# retain receipts and stop rather than deleting unrelated namespace objects.
for id in "${created_ids[@]}"; do
  kctl -n "${namespace}" delete kovabuild "${id}" --wait=true --timeout=120s >/dev/null || die "exact CR cleanup failed for ${id}"
done
jq -e '.items | length == 0' <<<"$(kctl -n "${namespace}" get pods -l app.kubernetes.io/name=kova-runner -o json)" >/dev/null || die "a runner Pod remains after exact CR cleanup"
queue=$(kctl -n "${namespace}" get configmap kova-service-queue-admission -o json)
active=$(kctl -n "${namespace}" get configmap kova-service-admission -o json)
jq -e '.data["queue.json"] | fromjson | (.intents | length) == 0' <<<"${queue}" >/dev/null || die "queue intents remain after cleanup"
jq -e '.data["reservations.json"] | fromjson | (.active | length) == 0' <<<"${active}" >/dev/null || die "active grants remain after cleanup"
success=true
note "PASS: exact test-owned CR cleanup verified; ledgers empty; no cluster-wide cleanup performed"
