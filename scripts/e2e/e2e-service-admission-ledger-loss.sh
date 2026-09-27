#!/usr/bin/env bash

# #44 fault acceptance against an existing, disposable Kind admission cluster.
# Check mode is read-only. Run mode deliberately deletes one named active
# ledger and leaves it absent; the caller must retire the dedicated cluster.
set -euo pipefail

# shellcheck source=scripts/common.sh
source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

root=$(repo_root)
mode=${ADMISSION_E2E_MODE:-check}
cluster=${KIND_CLUSTER:-}
namespace=${NAMESPACE:-kova}
release=${RELEASE_NAME:-kova}
port_a=${ADMISSION_LEDGER_LOSS_PORT_A:-18084}
port_b=${ADMISSION_LEDGER_LOSS_PORT_B:-18085}
restart=${ADMISSION_LEDGER_LOSS_RESTART:-false}
ack=${ADMISSION_LEDGER_LOSS_ACK:-}
token=${SERVICE_AUTH_TOKEN:-}
active_name=kova-service-admission
queue_name=kova-service-queue-admission

die() { echo "error: $*" >&2; exit 1; }
note() { echo "admission-ledger-loss-e2e: $*" >&2; }
kctl() { kubectl --kubeconfig "${kubeconfig}" "$@"; }

[[ ${mode} == check || ${mode} == run ]] || die "ADMISSION_E2E_MODE must be check or run"
[[ ${restart} == true || ${restart} == false ]] || die "ADMISSION_LEDGER_LOSS_RESTART must be true or false"
[[ ${port_a} =~ ^[0-9]+$ && ${port_b} =~ ^[0-9]+$ && ${port_a} != "${port_b}" ]] || die "choose two distinct local TCP ports"
require_cmd kind
require_cmd kubectl
require_cmd jq
require_cmd curl
require_cmd openssl

# This preflight checks the Kind context, exactly two current Service replicas,
# the test-only admission caps, no KovaBuild/runner in the namespace, and both
# empty ledgers. It is always read-only, including when our mode is run.
ADMISSION_E2E_MODE=check "${root}/scripts/e2e/e2e-service-admission.sh" || die "shared admission preflight failed"
kubeconfig=${KIND_KUBECONFIG:-.kind/${cluster}.kubeconfig}
if [[ ${kubeconfig} != /* ]]; then kubeconfig=${root}/${kubeconfig}; fi

# A context name can be copied into an unrelated kubeconfig. Match the actual
# server, CA and client credentials of the live Kind cluster without printing
# or saving any credential bytes. Refuse another Kind cluster on this host.
[[ $(kind get clusters | wc -l | tr -d ' ') == 1 ]] || die "this fault test needs the only Kind cluster on the host"
kind get clusters | awk -v expected="${cluster}" '$0 == expected { found = 1 } END { exit !found }' || die "Kind cluster identity changed"
fingerprint() {
  jq -cer '
    {server:.clusters[0].cluster.server,
     ca:.clusters[0].cluster["certificate-authority-data"],
     cert:.users[0].user["client-certificate-data"],
     key:.users[0].user["client-key-data"]} |
    select(all(.[]; type == "string" and length > 0))
  ' | openssl dgst -sha256 | awk '{print $NF}'
}
actual_fingerprint=$(kubectl --kubeconfig "${kubeconfig}" config view --raw --minify -o json | fingerprint)
kind_fingerprint=$(kind get kubeconfig --name "${cluster}" | kubectl --kubeconfig /dev/stdin config view --raw --minify -o json | fingerprint)
[[ -n ${actual_fingerprint} && ${actual_fingerprint} == "${kind_fingerprint}" ]] || die "kubeconfig credentials/server do not match the live Kind cluster"

# The cluster is intentionally disposable. A second namespace with live Kova
# work would still make deleting this shared-service ledger unsafe.
jq -e '.items | length == 0' <<<"$(kctl get kovabuilds --all-namespaces -o json)" >/dev/null || die "a KovaBuild exists elsewhere in this cluster"
jq -e '.items | length == 0' <<<"$(kctl get pods --all-namespaces -l app.kubernetes.io/name=kova-runner -o json)" >/dev/null || die "a runner Pod exists elsewhere in this cluster"
jq -e '.items | length == 2 and all(.[]; any(.status.conditions[]?; .type == "Ready" and .status == "True"))' <<<"$(kctl get nodes -o json)" >/dev/null || die "the dedicated Kind cluster does not have exactly two Ready nodes"
deployment=$(kctl -n "${namespace}" get deployment "${release}-service" -o json)
deployment_uid=$(jq -er '.metadata.uid' <<<"${deployment}")
selector="app.kubernetes.io/instance=${release},app.kubernetes.io/component=service"
pods=$(kctl -n "${namespace}" get pods -l "${selector}" -o json)
pod_a=$(jq -er '[.items[].metadata.name] | sort | .[0]' <<<"${pods}")
pod_b=$(jq -er '[.items[].metadata.name] | sort | .[1]' <<<"${pods}")
for pod_name in "${pod_a}" "${pod_b}"; do
  pod=$(kctl -n "${namespace}" get pod "${pod_name}" -o json)
  rs_name=$(jq -er '[.metadata.ownerReferences[]? | select(.controller == true and .kind == "ReplicaSet") | .name] | if length == 1 then .[0] else empty end' <<<"${pod}")
  rs_uid=$(jq -er '[.metadata.ownerReferences[]? | select(.controller == true and .kind == "ReplicaSet") | .uid] | if length == 1 then .[0] else empty end' <<<"${pod}")
  rs=$(kctl -n "${namespace}" get replicaset "${rs_name}" -o json)
  jq -e --arg rsuid "${rs_uid}" --arg deployment_uid "${deployment_uid}" --arg deployment_name "${release}-service" '
    .metadata.uid == $rsuid and
    ([.metadata.ownerReferences[]? | select(.controller == true and .kind == "Deployment" and .name == $deployment_name and .uid == $deployment_uid)] | length) == 1
  ' <<<"${rs}" >/dev/null || die "Service Pod ${pod_name} is not owned by the exact Service Deployment"
done

active=$(kctl -n "${namespace}" get configmap "${active_name}" -o json)
queue=$(kctl -n "${namespace}" get configmap "${queue_name}" -o json)
active_uid=$(jq -er '.metadata.uid' <<<"${active}")
active_rv=$(jq -er '.metadata.resourceVersion' <<<"${active}")
queue_uid=$(jq -er '.metadata.uid' <<<"${queue}")
queue_rv=$(jq -er '.metadata.resourceVersion' <<<"${queue}")
queue_data=$(jq -er '.data["queue.json"]' <<<"${queue}")
jq -e '.metadata.deletionTimestamp == null and (.metadata.ownerReferences // [] | length) == 0 and (.metadata.finalizers // [] | length) == 0' <<<"${active}" >/dev/null || die "active ledger is deleting, finalized, or unexpectedly owned"
jq -e '.metadata.deletionTimestamp == null and (.metadata.ownerReferences // [] | length) == 0 and (.metadata.finalizers // [] | length) == 0' <<<"${queue}" >/dev/null || die "queue ledger is deleting, finalized, or unexpectedly owned"
note "read-only preflight passed for ${cluster}/${namespace}: exact Kind credentials, two owned Service Pods, no KovaBuilds or runner Pods, empty ledgers"
if [[ ${mode} == check ]]; then
  note "no cluster writes performed; live mode requires ADMISSION_E2E_MODE=run, ADMISSION_LEDGER_LOSS_ACK=${cluster}/${namespace}/${active_name}, and SERVICE_AUTH_TOKEN"
  exit 0
fi
[[ ${ack} == "${cluster}/${namespace}/${active_name}" ]] || die "ADMISSION_LEDGER_LOSS_ACK must exactly name the disposable Kind cluster, namespace, and active ledger"
[[ -n ${token} ]] || die "SERVICE_AUTH_TOKEN is required in live mode"

work_dir=$(mktemp -d "${TMPDIR:-/tmp}/kova-admission-ledger-loss.XXXXXX")
run_id=$(date -u +%Y%m%dt%H%M%Sz)-$(openssl rand -hex 4)
printf 'run_id=%s\ncluster=%s\nnamespace=%s\nrelease=%s\nactive_ledger_uid=%s\nqueue_ledger_uid=%s\npod_a=%s\npod_b=%s\n' \
  "${run_id}" "${cluster}" "${namespace}" "${release}" "${active_uid}" "${queue_uid}" "${pod_a}" "${pod_b}" >"${work_dir}/identities.txt"
forward_a=
forward_b=
success=false
snapshot() {
  local stage=$1
  kctl --request-timeout=5s -n "${namespace}" get deployment "${release}-service" -o json >"${work_dir}/${stage}-deployment.json" 2>"${work_dir}/${stage}-deployment.err" || true
  kctl --request-timeout=5s -n "${namespace}" get pods -l "${selector}" -o json >"${work_dir}/${stage}-service-pods.json" 2>"${work_dir}/${stage}-service-pods.err" || true
  kctl --request-timeout=5s -n "${namespace}" get kovabuilds -o json >"${work_dir}/${stage}-builds.json" 2>"${work_dir}/${stage}-builds.err" || true
  kctl --request-timeout=5s -n "${namespace}" get pods -l app.kubernetes.io/name=kova-runner -o json >"${work_dir}/${stage}-runners.json" 2>"${work_dir}/${stage}-runners.err" || true
  kctl --request-timeout=5s -n "${namespace}" get configmap "${active_name}" --ignore-not-found -o json >"${work_dir}/${stage}-active.json" 2>"${work_dir}/${stage}-active.err" || true
  kctl --request-timeout=5s -n "${namespace}" get configmap "${queue_name}" -o json >"${work_dir}/${stage}-queue.json" 2>"${work_dir}/${stage}-queue.err" || true
}
stop_forward() {
  if [[ -n ${forward_a} ]]; then kill "${forward_a}" 2>/dev/null || true; wait "${forward_a}" 2>/dev/null || true; forward_a=; fi
  if [[ -n ${forward_b} ]]; then kill "${forward_b}" 2>/dev/null || true; wait "${forward_b}" 2>/dev/null || true; forward_b=; fi
}
cleanup() {
  local exit_code=$?
  trap - EXIT
  stop_forward
  snapshot final
  if [[ ${success} == true && ${exit_code} == 0 ]]; then
    note "PASS: receipts preserved at ${work_dir}; active ledger remains absent by design"
  else
    note "FAILED: receipts preserved at ${work_dir}; inspect the exact test cluster before any recovery or deletion"
  fi
  exit "${exit_code}"
}
trap cleanup EXIT
snapshot before

kctl -n "${namespace}" port-forward --address 127.0.0.1 "pod/${pod_a}" "${port_a}:8080" >"${work_dir}/port-forward-a.log" 2>&1 &
forward_a=$!
kctl -n "${namespace}" port-forward --address 127.0.0.1 "pod/${pod_b}" "${port_b}:8080" >"${work_dir}/port-forward-b.log" 2>&1 &
forward_b=$!
wait_health() {
  local port=$1 pid=$2 deadline=$((SECONDS + 30))
  while (( SECONDS < deadline )); do
    kill -0 "${pid}" 2>/dev/null || die "port-forward ${port} exited"
    if curl --noproxy '*' --connect-timeout 2 --max-time 2 -fsS "http://127.0.0.1:${port}/healthz" >/dev/null 2>&1; then return; fi
    sleep 1
  done
  die "port-forward ${port} did not become healthy"
}
wait_health "${port_a}" "${forward_a}"
wait_health "${port_b}" "${forward_b}"
request() {
  local name=$1 port=$2 method=$3 path=$4 payload=${5:-}
  local -a args=(--noproxy '*' --connect-timeout 3 --max-time 15 -sS -X "${method}" -D "${work_dir}/${name}.headers" -o "${work_dir}/${name}.body" -w '%{http_code}')
  if [[ ${method} == POST ]]; then
    args+=(-H 'Content-Type: application/json' --data-binary "${payload}")
    if ! printf 'header = "Authorization: Bearer %s"\n' "${token}" | curl --config - "${args[@]}" "http://127.0.0.1:${port}${path}" >"${work_dir}/${name}.status"; then
      echo transport_error >"${work_dir}/${name}.status"
      die "${name} transport outcome is unknown"
    fi
  elif ! curl "${args[@]}" "http://127.0.0.1:${port}${path}" >"${work_dir}/${name}.status"; then
    echo transport_error >"${work_dir}/${name}.status"
    die "${name} transport failed"
  fi
}
assert_ready() {
  local name=$1 port=$2 expected=$3
  request "${name}" "${port}" GET /readyz
  [[ $(<"${work_dir}/${name}.status") == "${expected}" ]] || die "${name} returned HTTP $(<"${work_dir}/${name}.status"), expected ${expected}"
  if [[ ${expected} == 200 ]]; then
    jq -e '.status == "ready"' "${work_dir}/${name}.body" >/dev/null || die "${name} returned an invalid ready body"
  else
    jq -e '.code == "internal" and .retryable == true' "${work_dir}/${name}.body" >/dev/null || die "${name} returned an unrelated unavailable body"
  fi
}
assert_no_work() {
  jq -e '.items | length == 0' <<<"$(kctl get kovabuilds --all-namespaces -o json)" >/dev/null || die "a KovaBuild appeared"
  jq -e '.items | length == 0' <<<"$(kctl get pods --all-namespaces -l app.kubernetes.io/name=kova-runner -o json)" >/dev/null || die "a runner Pod appeared"
}
assert_ledger_absent() {
  local observed
  observed=$(kctl -n "${namespace}" get configmap "${active_name}" --ignore-not-found -o name) || die "active ledger absence could not be checked"
  [[ -z ${observed} ]] || die "active ledger was silently recreated"
  local current_queue
  current_queue=$(kctl -n "${namespace}" get configmap "${queue_name}" -o json)
  jq -e --arg uid "${queue_uid}" --arg data "${queue_data}" '.metadata.uid == $uid and .data["queue.json"] == $data' <<<"${current_queue}" >/dev/null || die "queue ledger changed during this fault test"
}

assert_ready before-a "${port_a}" 200
assert_ready before-b "${port_b}" 200
assert_no_work
current_queue=$(kctl -n "${namespace}" get configmap "${queue_name}" -o json)
jq -e --arg uid "${queue_uid}" --arg rv "${queue_rv}" --arg data "${queue_data}" '.metadata.uid == $uid and .metadata.resourceVersion == $rv and .data["queue.json"] == $data' <<<"${current_queue}" >/dev/null || die "queue ledger changed before exact deletion"
current=$(kctl -n "${namespace}" get configmap "${active_name}" -o json)
jq -e --arg uid "${active_uid}" --arg rv "${active_rv}" '.metadata.uid == $uid and .metadata.resourceVersion == $rv and .metadata.deletionTimestamp == null' <<<"${current}" >/dev/null || die "active ledger changed before exact deletion"
note "deleting only ${namespace}/configmap/${active_name} (${active_uid}); it will not be recreated by this script"
kctl -n "${namespace}" delete configmap "${active_name}" --wait=true --timeout=30s >"${work_dir}/active-delete.txt" || die "exact active ledger deletion failed"
assert_ledger_absent
snapshot after-delete

assert_ready after-a "${port_a}" 503
assert_ready after-b "${port_b}" 503
source_digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
source_uri="oci://kind-registry:5000/kova-sources/ledger-loss-${run_id}@${source_digest}"
target="kind-registry:5000/kova-admission/ledger-loss-${run_id}:dev"
platform=$(jq -r '[.spec.template.spec.containers[] | select(.name == "kova-service") | .args[] | select(startswith("--buildkit-platform-addr=")) | split("=")[1]] | first // empty' <<<"${deployment}")
principal=$(jq -r '[.spec.template.spec.containers[] | select(.name == "kova-service") | .args[] | select(startswith("--auth-static-principal=")) | sub("^--auth-static-principal="; "")] | first // empty' <<<"${deployment}")
[[ -n ${principal} && ( ${platform} == linux/amd64 || ${platform} == linux/arm64 ) ]] || die "could not resolve test principal or platform"
for side in a b; do
  port=${port_a}
  if [[ ${side} == b ]]; then port=${port_b}; fi
  key="${run_id}-${side}"
  build_id=$(printf '%s\0%s' "${principal}" "${key}" | openssl dgst -sha256 | awk '{print "idem-" substr($NF, 1, 20)}')
  printf '%s %s\n' "${key}" "${build_id}" >>"${work_dir}/candidates.txt"
  payload=$(jq -nc --arg uri "${source_uri}" --arg digest "${source_digest}" --arg target "${target}" --arg platform "${platform}" --arg key "${key}" \
    '{source_uri:$uri,source_digest:$digest,targets:[{target:$target,platform:$platform}],format:"oci",concurrency:1,idempotency_key:$key}')
  request "submit-${side}" "${port}" POST /v1/builds "${payload}"
  [[ $(<"${work_dir}/submit-${side}.status") == 503 ]] || die "submit-${side} was not fail-closed (HTTP $(<"${work_dir}/submit-${side}.status"))"
  jq -e '.code == "internal" and .retryable == true' "${work_dir}/submit-${side}.body" >/dev/null || die "submit-${side} returned an unrelated 503"
  assert_no_work
  assert_ledger_absent
done
note "PASS: both Service replicas returned 503 for readiness and new POSTs; no KovaBuild or runner Pod appeared"
snapshot after-submit

if [[ ${restart} == true ]]; then
  # Replacing one exact Deployment-owned Pod tests startup without deleting
  # the still-present queue ledger. Never restart a Pod if its UID or owner
  # chain changed after the original preflight.
  lease_ns=$(jq -r '[.spec.template.spec.containers[] | select(.name == "kova-service") | .args[] | select(startswith("--leader-election-namespace=")) | sub("^--leader-election-namespace="; "")] | if length == 1 then .[0] else empty end' <<<"${deployment}")
  [[ ${lease_ns} == "${namespace}" ]] || die "leader-election namespace differs from the disposable namespace"
  lease=$(kctl -n "${lease_ns}" get lease kova-service.kova.cofy.dev -o json)
  holder=$(jq -er '.spec.holderIdentity' <<<"${lease}")
  leader=${holder%%_*}
  [[ ${leader} == "${pod_a}" || ${leader} == "${pod_b}" ]] || die "Lease holder is not one of the verified Service Pods"
  replace=${pod_a}
  if [[ ${leader} == "${pod_a}" ]]; then replace=${pod_b}; fi
  before_pod=$(kctl -n "${namespace}" get pod "${replace}" -o json)
  before_uid=$(jq -er '.metadata.uid' <<<"${before_pod}")
  original_a_uid=$(jq -er --arg name "${pod_a}" '.items[] | select(.metadata.name == $name) | .metadata.uid' <<<"${pods}")
  original_b_uid=$(jq -er --arg name "${pod_b}" '.items[] | select(.metadata.name == $name) | .metadata.uid' <<<"${pods}")
  current_pods=$(kctl -n "${namespace}" get pods -l "${selector}" -o json)
  jq -e --arg a "${original_a_uid}" --arg b "${original_b_uid}" '
    [.items[].metadata.uid] | length == 2 and sort == ([$a, $b] | sort)
  ' <<<"${current_pods}" >/dev/null || die "Service Pods changed after the original ownership check"
  rs_name=$(jq -er '[.metadata.ownerReferences[]? | select(.controller == true and .kind == "ReplicaSet") | .name] | if length == 1 then .[0] else empty end' <<<"${before_pod}")
  rs_uid=$(jq -er '[.metadata.ownerReferences[]? | select(.controller == true and .kind == "ReplicaSet") | .uid] | if length == 1 then .[0] else empty end' <<<"${before_pod}")
  rs=$(kctl -n "${namespace}" get replicaset "${rs_name}" -o json)
  jq -e --arg uid "${rs_uid}" --arg deployment_uid "${deployment_uid}" --arg name "${release}-service" '
    .metadata.uid == $uid and ([.metadata.ownerReferences[]? | select(.controller == true and .kind == "Deployment" and .uid == $deployment_uid and .name == $name)] | length) == 1
  ' <<<"${rs}" >/dev/null || die "selected Pod is no longer owned by the exact Service Deployment"
  current_pod=$(kctl -n "${namespace}" get pod "${replace}" -o json)
  jq -e --arg uid "${before_uid}" '.metadata.uid == $uid and .metadata.deletionTimestamp == null' <<<"${current_pod}" >/dev/null || die "selected Pod changed before restart"
  printf 'pod=%s\nuid=%s\n' "${replace}" "${before_uid}" >"${work_dir}/restarted-pod.txt"
  note "deleting only owned follower Pod ${replace} (${before_uid}) to test no silent ledger reinitialization"
  kctl -n "${namespace}" delete pod "${replace}" --wait=false >"${work_dir}/pod-delete.txt" || die "exact Service Pod deletion failed"
  deadline=$((SECONDS + 120))
  replacement_name=
  while (( SECONDS < deadline )); do
    assert_ledger_absent
    assert_no_work
    current_pods=$(kctl -n "${namespace}" get pods -l "${selector}" -o json)
    replacement_name=$(jq -r --arg old_a "${original_a_uid}" --arg old_b "${original_b_uid}" --arg rs "${rs_uid}" '
      [.items[] | select(.metadata.uid != $old_a and .metadata.uid != $old_b and .metadata.deletionTimestamp == null and
        any(.metadata.ownerReferences[]?; .controller == true and .kind == "ReplicaSet" and .uid == $rs)) | .metadata.name] |
      if length == 1 then .[0] else empty end
    ' <<<"${current_pods}")
    [[ -z ${replacement_name} ]] || break
    sleep 2
  done
  [[ -n ${replacement_name} ]] || die "Deployment did not replace the exact deleted Service Pod"
  printf 'replacement_pod=%s\n' "${replacement_name}" >>"${work_dir}/restarted-pod.txt"
  startup_refused=false
  deadline=$((SECONDS + 120))
  while (( SECONDS < deadline )); do
    assert_ledger_absent
    assert_no_work
    if kctl -n "${namespace}" logs "pod/${replacement_name}" -c kova-service --previous --tail=100 2>/dev/null |
       grep -F 'active admission ledger is absent while queue admission ledger exists' >"${work_dir}/replacement-startup-refusal.txt"; then
      startup_refused=true
      break
    fi
    if kctl -n "${namespace}" logs "pod/${replacement_name}" -c kova-service --tail=100 2>/dev/null |
       grep -F 'active admission ledger is absent while queue admission ledger exists' >"${work_dir}/replacement-startup-refusal.txt"; then
      startup_refused=true
      break
    fi
    sleep 2
  done
  [[ ${startup_refused} == true ]] || die "replacement Pod did not report the missing-ledger startup refusal"
  sleep 10
  assert_ledger_absent
  assert_no_work
  survivor_port=${port_a}
  if [[ ${replace} == "${pod_a}" ]]; then survivor_port=${port_b}; fi
  assert_ready after-restart-survivor "${survivor_port}" 503
  snapshot after-restart
  note "PASS: replacement Service Pod did not recreate the missing ledger"
fi

success=true
note "fault test complete; the dedicated Kind cluster remains intentionally unhealthy until its owner deletes it"
