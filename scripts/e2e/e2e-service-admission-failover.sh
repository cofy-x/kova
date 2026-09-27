#!/usr/bin/env bash

# #44 leader-handoff acceptance against an existing dedicated Kind cluster.
# The default mode is read-only. Live mode deletes only the verified current
# Service leader Pod and two run-scoped KovaBuilds; it never alters a ledger.
set -euo pipefail

# shellcheck source=scripts/common.sh
source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

root=$(repo_root)
mode=${ADMISSION_E2E_MODE:-check}
cluster=${KIND_CLUSTER:-}
namespace=${NAMESPACE:-kova}
release=${RELEASE_NAME:-kova}
port=${ADMISSION_E2E_PORT:-18083}
token=${SERVICE_AUTH_TOKEN:-}
lease_name=kova-service.kova.cofy.dev

die() { echo "error: $*" >&2; exit 1; }
note() { echo "admission-failover-e2e: $*" >&2; }
kctl() { kubectl --kubeconfig "${kubeconfig}" "$@"; }

[[ ${mode} == check || ${mode} == run ]] || die "ADMISSION_E2E_MODE must be check or run"
[[ ${port} =~ ^[0-9]+$ ]] || die "ADMISSION_E2E_PORT must be a local TCP port"
require_cmd kubectl
require_cmd jq
require_cmd curl
require_cmd openssl

# Reuse the stricter two-replica, empty-ledger, exact-capacity preflight. This
# invocation is always read-only, even when this script was asked to run.
ADMISSION_E2E_MODE=check "${root}/scripts/e2e/e2e-service-admission.sh" || die "shared admission preflight failed"
kubeconfig=${KIND_KUBECONFIG:-.kind/${cluster}.kubeconfig}
if [[ ${kubeconfig} != /* ]]; then kubeconfig=${root}/${kubeconfig}; fi

deployment=$(kctl -n "${namespace}" get deployment "${release}-service" -o json)
deployment_uid=$(jq -er '.metadata.uid' <<<"${deployment}")
leader_ns=$(jq -r '[.spec.template.spec.containers[] | select(.name == "kova-service") | .args[] | select(startswith("--leader-election-namespace=")) | sub("^--leader-election-namespace="; "")] | if length == 1 then .[0] else empty end' <<<"${deployment}")
[[ ${leader_ns} == "${namespace}" ]] || die "leader-election namespace must equal the dedicated runner namespace"
selector="app.kubernetes.io/instance=${release},app.kubernetes.io/component=service"
pods=$(kctl -n "${namespace}" get pods -l "${selector}" -o json)
pod_a=$(jq -r '[.items[].metadata.name] | sort | .[0]' <<<"${pods}")
pod_b=$(jq -r '[.items[].metadata.name] | sort | .[1]' <<<"${pods}")
original_image_id=$(jq -er '[.items[].status.containerStatuses[] | select(.name == "kova-service") | .imageID] | first' <<<"${pods}")

lease_holder() {
  local json holder name
  json=$(kctl -n "${leader_ns}" get lease "${lease_name}" -o json) || return 1
  holder=$(jq -er '.spec.holderIdentity // empty' <<<"${json}") || return 1
  name=${holder%%_*}
  [[ ${name} != "${holder}" && ( ${name} == "${pod_a}" || ${name} == "${pod_b}" ) ]] || return 1
  printf '%s %s\n' "${holder}" "${name}"
}

read -r initial_holder initial_leader < <(lease_holder) || die "Lease has no holder matching the two ready Service Pods"
[[ -n ${initial_holder} && -n ${initial_leader} ]] || die "Lease has no holder matching the two ready Service Pods"
note "read-only leader preflight passed: holder ${initial_holder} belongs to ${initial_leader}"
if [[ ${mode} == check ]]; then
  note "no cluster writes performed; set ADMISSION_E2E_MODE=run and SERVICE_AUTH_TOKEN to execute"
  exit 0
fi
[[ -n ${token} ]] || die "SERVICE_AUTH_TOKEN is required in live mode"

platform=$(jq -r '[.spec.template.spec.containers[] | select(.name == "kova-service") | .args[] | select(startswith("--buildkit-platform-addr=")) | split("=")[1]] | first // empty' <<<"${deployment}")
principal=$(jq -r '[.spec.template.spec.containers[] | select(.name == "kova-service") | .args[] | select(startswith("--auth-static-principal=")) | sub("^--auth-static-principal="; "")] | first // empty' <<<"${deployment}")
[[ -n ${principal} && ( ${platform} == linux/amd64 || ${platform} == linux/arm64 ) ]] || die "cannot determine the test principal and platform"
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/kova-admission-failover.XXXXXX")
run_id=$(date -u +%Y%m%dt%H%M%sz)-$(openssl rand -hex 4)
source_digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
source_uri="oci://kind-registry:5000/kova-sources/failover-${run_id}@${source_digest}"
target="kind-registry:5000/kova-admission/failover-${run_id}:dev"
blocker_key="${run_id}-blocker"
challenger_key="${run_id}-challenger"
build_id() { printf '%s\0%s' "${principal}" "$1" | openssl dgst -sha256 | awk '{ print "idem-" substr($NF, 1, 20) }'; }
blocker_id=$(build_id "${blocker_key}")
challenger_id=$(build_id "${challenger_key}")
printf 'run_id=%s\nblocker_key=%s\nblocker_id=%s\nchallenger_key=%s\nchallenger_id=%s\n' \
  "${run_id}" "${blocker_key}" "${blocker_id}" "${challenger_key}" "${challenger_id}" >"${work_dir}/identities.txt"
printf '%s\n' "${initial_holder}" >"${work_dir}/initial-leader.txt"

forward_pid=
success=false
snapshot() {
  local name=$1
  kctl --request-timeout=5s -n "${namespace}" get deployment "${release}-service" -o json >"${work_dir}/${name}-deployment.json" 2>"${work_dir}/${name}-deployment.err" || true
  kctl --request-timeout=5s -n "${namespace}" get pods -l "${selector}" -o json >"${work_dir}/${name}-service-pods.json" 2>"${work_dir}/${name}-service-pods.err" || true
  kctl --request-timeout=5s -n "${leader_ns}" get lease "${lease_name}" -o json >"${work_dir}/${name}-lease.json" 2>"${work_dir}/${name}-lease.err" || true
  kctl --request-timeout=5s -n "${namespace}" get kovabuilds -o json >"${work_dir}/${name}-builds.json" 2>"${work_dir}/${name}-builds.err" || true
  kctl --request-timeout=5s -n "${namespace}" get pods -l app.kubernetes.io/name=kova-runner -o json >"${work_dir}/${name}-runners.json" 2>"${work_dir}/${name}-runners.err" || true
  kctl --request-timeout=5s -n "${namespace}" get configmap kova-service-admission -o json >"${work_dir}/${name}-active.json" 2>"${work_dir}/${name}-active.err" || true
  kctl --request-timeout=5s -n "${namespace}" get configmap kova-service-queue-admission -o json >"${work_dir}/${name}-queue.json" 2>"${work_dir}/${name}-queue.err" || true
}
stop_forward() {
  if [[ -n ${forward_pid} ]]; then
    kill "${forward_pid}" 2>/dev/null || true
    wait "${forward_pid}" 2>/dev/null || true
    forward_pid=
  fi
}
cleanup() {
  local exit_code=$?
  trap - EXIT
  stop_forward
  if [[ ${success} == true && ${exit_code} == 0 ]]; then
    note "PASS: run-scoped receipts preserved at ${work_dir}"
  else
    snapshot failure
    note "FAILED: receipts preserved at ${work_dir}; test-owned CRs were NOT automatically deleted"
    note "candidate CR IDs: ${blocker_id}, ${challenger_id}; inspect exact Pod and ledger state before recovery"
  fi
  exit "${exit_code}"
}
trap cleanup EXIT
snapshot before

start_forward() {
  local pod=$1 label=$2 deadline
  kctl -n "${namespace}" port-forward --address 127.0.0.1 "pod/${pod}" "${port}:8080" >"${work_dir}/${label}-port-forward.log" 2>&1 &
  forward_pid=$!
  deadline=$((SECONDS + 30))
  while (( SECONDS < deadline )); do
    kill -0 "${forward_pid}" 2>/dev/null || die "port-forward to ${pod} exited"
    if curl --noproxy '*' --connect-timeout 2 --max-time 2 -fsS "http://127.0.0.1:${port}/healthz" >/dev/null 2>&1; then return; fi
    sleep 1
  done
  die "port-forward to ${pod} did not become healthy"
}
post_build() {
  local key=$1 label=$2 payload
  payload=$(jq -nc --arg uri "${source_uri}" --arg digest "${source_digest}" --arg target "${target}" --arg platform "${platform}" --arg key "${key}" \
    '{source_uri:$uri,source_digest:$digest,targets:[{target:$target,platform:$platform}],format:"oci",concurrency:1,idempotency_key:$key}')
  if ! curl --noproxy '*' --connect-timeout 3 --max-time 15 -sS -X POST \
    -H "Authorization: Bearer ${token}" -H 'Content-Type: application/json' \
    --data-binary "${payload}" -D "${work_dir}/${label}.headers" -o "${work_dir}/${label}.body" \
    -w '%{http_code}' "http://127.0.0.1:${port}/v1/builds" >"${work_dir}/${label}.status"; then
    echo transport_error >"${work_dir}/${label}.status"
    die "${label} Create outcome is unknown; no automatic cleanup"
  fi
  [[ $(<"${work_dir}/${label}.status") == 202 ]] || die "${label} was not newly accepted (HTTP $(<"${work_dir}/${label}.status"))"
  local observed
  observed=$(awk 'tolower($1) == "x-kova-build-id:" { gsub("\r", "", $2); print $2 }' "${work_dir}/${label}.headers")
  [[ ${observed} == "$(build_id "${key}")" ]] || die "${label} returned unexpected Build ID ${observed}"
}

start_forward "${pod_a}" initial
post_build "${blocker_key}" blocker
stop_forward

# The one unschedulable runner must exist, hold exactly one active grant, and
# have no unresolved Pod Create nonce before the leader Pod is replaced.
deadline=$((SECONDS + 60))
while (( SECONDS < deadline )); do
  if build=$(kctl -n "${namespace}" get kovabuild "${blocker_id}" -o json 2>/dev/null) &&
     runner=$(kctl -n "${namespace}" get pod "kova-job-${blocker_id}" -o json 2>/dev/null) &&
     active=$(kctl -n "${namespace}" get configmap kova-service-admission -o json 2>/dev/null) &&
     queue=$(kctl -n "${namespace}" get configmap kova-service-queue-admission -o json 2>/dev/null) &&
     jq -e --arg key "${blocker_key}" --arg uri "${source_uri}" '.spec.idempotencyKey == $key and .spec.source.uri == $uri and .status.phase == "Starting"' <<<"${build}" >/dev/null &&
     jq -e '.spec.nodeSelector.never == "true" and .status.phase == "Pending" and (.metadata.annotations["kova.cofy.dev/create-attempt"] | length) == 32' <<<"${runner}" >/dev/null &&
     jq -e --arg id "${blocker_id}" '.data["reservations.json"] | fromjson | (.active | length) == 1 and ([.active[].buildName] == [$id]) and ([.active[].slots] == [1]) and ([.active[].inFlight // [] | length] == [0]) and ([.active[].closing // false] == [false])' <<<"${active}" >/dev/null &&
     jq -e '.data["queue.json"] | fromjson | (.intents | length) == 0' <<<"${queue}" >/dev/null; then
    break
  fi
  sleep 1
done
(( SECONDS < deadline )) || die "blocker did not acquire one stable active grant and Pending runner"
blocker_uid=$(jq -er '.metadata.uid' <<<"${build}")
runner_uid=$(jq -er '.metadata.uid' <<<"${runner}")
runner_nonce=$(jq -er '.metadata.annotations["kova.cofy.dev/create-attempt"]' <<<"${runner}")
jq -e --arg uid "${blocker_uid}" '[.metadata.ownerReferences[]? | select(.controller == true and .uid == $uid)] | length == 1' <<<"${runner}" >/dev/null || die "runner is not owned by this run's blocker CR"
snapshot blocker-ready

# Resolve the exact current Lease holder immediately before deletion, then
# prove that Pod is controlled by this one dedicated Service Deployment.
read -r old_holder old_leader < <(lease_holder) || die "could not resolve current leader before replacement"
[[ -n ${old_holder} && -n ${old_leader} ]] || die "current Lease holder is not a ready Service Pod"
old_pod=$(kctl -n "${namespace}" get pod "${old_leader}" -o json)
old_pod_uid=$(jq -er '.metadata.uid' <<<"${old_pod}")
replicaset_name=$(jq -r '[.metadata.ownerReferences[]? | select(.controller == true and .kind == "ReplicaSet") | .name] | if length == 1 then .[0] else empty end' <<<"${old_pod}")
replicaset_uid=$(jq -r '[.metadata.ownerReferences[]? | select(.controller == true and .kind == "ReplicaSet") | .uid] | if length == 1 then .[0] else empty end' <<<"${old_pod}")
[[ -n ${replicaset_name} && -n ${replicaset_uid} ]] || die "leader Pod has no unique controller ReplicaSet"
replicaset=$(kctl -n "${namespace}" get replicaset "${replicaset_name}" -o json)
jq -e --arg rsuid "${replicaset_uid}" --arg uid "${deployment_uid}" --arg name "${release}-service" \
  '.metadata.uid == $rsuid and ([.metadata.ownerReferences[]? | select(.controller == true and .kind == "Deployment" and .name == $name and .uid == $uid)] | length == 1)' <<<"${replicaset}" >/dev/null || die "leader Pod is not owned by this Service Deployment"
printf '%s\n' "${old_holder}" >"${work_dir}/deleted-leader-holder.txt"
printf '%s\n' "${old_leader}" >"${work_dir}/deleted-leader-pod.txt"
printf '%s\n' "${old_pod_uid}" >"${work_dir}/deleted-leader-uid.txt"
current_pod=$(kctl -n "${namespace}" get pod "${old_leader}" -o json)
jq -e --arg uid "${old_pod_uid}" '.metadata.uid == $uid and .metadata.deletionTimestamp == null' <<<"${current_pod}" >/dev/null || die "selected leader Pod changed or was already terminating"
read -r verified_holder verified_leader < <(lease_holder) || die "Lease holder changed during leader Pod ownership checks"
[[ ${verified_holder} == "${old_holder}" && ${verified_leader} == "${old_leader}" ]] || die "Lease holder changed before exact leader Pod deletion"
note "deleting exact current leader Pod ${old_leader} (${old_pod_uid}); Deployment will replace it"
kctl -n "${namespace}" delete pod "${old_leader}" --wait=false >/dev/null || die "exact leader Pod deletion failed"

deadline=$((SECONDS + 180))
new_holder=
new_leader=
while (( SECONDS < deadline )); do
  if handoff_pods=$(kctl -n "${namespace}" get pods -l "${selector}" -o json 2>/dev/null) &&
     handoff_lease=$(kctl -n "${leader_ns}" get lease "${lease_name}" -o json 2>/dev/null) &&
     handoff_deployment=$(kctl -n "${namespace}" get deployment "${release}-service" -o json 2>/dev/null); then
    candidate_holder=$(jq -r '.spec.holderIdentity // empty' <<<"${handoff_lease}")
    candidate_leader=${candidate_holder%%_*}
    if [[ -n ${candidate_holder} && ${candidate_holder} != "${old_holder}" && ${candidate_leader} != "${old_leader}" ]] &&
       jq -e --arg old "${old_leader}" --arg image "${original_image_id}" --arg holder "${candidate_leader}" \
         '.items | length == 2 and all(.[]; .metadata.name != $old and .metadata.deletionTimestamp == null and
           ([.status.conditions[]? | select(.type == "Ready" and .status == "True")] | length == 1) and
           ([.status.containerStatuses[]? | select(.name == "kova-service" and .imageID == $image)] | length == 1)) and
           ([.[].metadata.name] | index($holder)) != null' <<<"${handoff_pods}" >/dev/null &&
       jq -e '.spec.replicas == 2 and .status.readyReplicas == 2 and .status.updatedReplicas == 2 and .status.observedGeneration == .metadata.generation' <<<"${handoff_deployment}" >/dev/null; then
      new_holder=${candidate_holder}
      new_leader=${candidate_leader}
      break
    fi
  fi
  sleep 2
done
[[ -n ${new_holder} && -n ${new_leader} ]] || die "replacement Service Pods or a distinct elected leader did not become ready"
if kctl -n "${namespace}" get pod "${old_leader}" -o json >"${work_dir}/old-pod-still-present.json" 2>/dev/null; then
  die "deleted leader Pod still exists after handoff"
fi
snapshot after-handoff
note "new Lease holder ${new_holder}; submitting a challenger to prove active capacity remains charged"

start_forward "${new_leader}" replacement
post_build "${challenger_key}" challenger
stop_forward

# The replacement leader must actually reconcile the challenger to
# WaitingForCapacity. This avoids a false pass from merely reading a stale
# active ledger while the new controller is not working.
deadline=$((SECONDS + 60))
while (( SECONDS < deadline )); do
  if challenger=$(kctl -n "${namespace}" get kovabuild "${challenger_id}" -o json 2>/dev/null) &&
     jq -e --arg key "${challenger_key}" --arg uri "${source_uri}" \
       '.spec.idempotencyKey == $key and .spec.source.uri == $uri and .status.phase == "Queued" and
        any(.status.conditions[]?; .type == "Ready" and .reason == "WaitingForCapacity")' <<<"${challenger}" >/dev/null; then
    break
  fi
  sleep 1
done
(( SECONDS < deadline )) || die "replacement leader did not reconcile challenger to WaitingForCapacity"

assert_cap_held() {
  local stage=$1 builds runners active queue lease observed_nonce
  builds=$(kctl -n "${namespace}" get kovabuilds -o json)
  runners=$(kctl -n "${namespace}" get pods -l app.kubernetes.io/name=kova-runner -o json)
  active=$(kctl -n "${namespace}" get configmap kova-service-admission -o json)
  queue=$(kctl -n "${namespace}" get configmap kova-service-queue-admission -o json)
  lease=$(kctl -n "${leader_ns}" get lease "${lease_name}" -o json)
  jq -e --arg blocker "${blocker_id}" --arg challenger "${challenger_id}" --arg uri "${source_uri}" \
    '.items | length == 2 and ([.[].metadata.name] | sort) == ([$blocker, $challenger] | sort) and all(.[]; .spec.source.uri == $uri)' <<<"${builds}" >/dev/null || die "${stage}: stale or duplicate KovaBuilds observed"
  jq -e --arg id "${blocker_id}" --arg key "${blocker_key}" \
    '.items[] | select(.metadata.name == $id) | .spec.idempotencyKey == $key and .status.phase == "Starting"' <<<"${builds}" >/dev/null || die "${stage}: blocker changed phase or identity"
  jq -e --arg id "${challenger_id}" --arg key "${challenger_key}" \
    '.items[] | select(.metadata.name == $id) | .spec.idempotencyKey == $key and .status.phase == "Queued" and
     any(.status.conditions[]?; .type == "Ready" and .reason == "WaitingForCapacity")' <<<"${builds}" >/dev/null || die "${stage}: challenger is no longer waiting for capacity"
  jq -e --arg id "${blocker_id}" --arg uid "${runner_uid}" --arg nonce "${runner_nonce}" \
    '.items | length == 1 and .[0].metadata.name == ("kova-job-" + $id) and .[0].metadata.uid == $uid and
     .[0].metadata.annotations["kova.cofy.dev/create-attempt"] == $nonce and .[0].status.phase == "Pending"' <<<"${runners}" >/dev/null || die "${stage}: runner Pod was duplicated, replaced, or became runnable"
  jq -e --arg id "${blocker_id}" --arg uid "${blocker_uid}" \
    '.data["reservations.json"] | fromjson | (.active | length) == 1 and .active[$uid].buildName == $id and
     .active[$uid].slots == 1 and ((.active[$uid].inFlight // []) | length) == 0 and (.active[$uid].closing // false) == false' <<<"${active}" >/dev/null || die "${stage}: active grant changed or exceeded cap"
  jq -e --arg id "${challenger_id}" '.data["queue.json"] | fromjson | (.intents | keys) == [$id]' <<<"${queue}" >/dev/null || die "${stage}: queue intent changed or exceeded cap"
  observed_nonce=$(jq -r --arg id "${challenger_id}" '.data["queue.json"] | fromjson | .intents[$id].nonce // empty' <<<"${queue}")
  [[ ${#observed_nonce} == 32 ]] || die "${stage}: challenger intent nonce is missing"
  jq -e --arg id "${challenger_id}" --arg nonce "${observed_nonce}" \
    '.items[] | select(.metadata.name == $id) | .metadata.annotations["kova.cofy.dev/queue-intent"] == $nonce' <<<"${builds}" >/dev/null || die "${stage}: challenger intent nonce differs from CR"
  jq -e --arg holder "${new_holder}" '.spec.holderIdentity == $holder' <<<"${lease}" >/dev/null || die "${stage}: elected holder changed again"
  snapshot "${stage}"
}
assert_cap_held after-challenger
sleep 10
assert_cap_held stable
note "PASS: replacement leader reconciled challenger but retained the one original runner Pod and active grant"

# Delete the queued challenger first, so it cannot acquire the active slot as
# the blocker is removed. Only these two exact run-scoped CR names are touched.
kctl -n "${namespace}" delete kovabuild "${challenger_id}" --wait=true --timeout=120s >/dev/null || die "exact challenger CR cleanup failed"
kctl -n "${namespace}" delete kovabuild "${blocker_id}" --wait=true --timeout=120s >/dev/null || die "exact blocker CR cleanup failed"
jq -e '.items | length == 0' <<<"$(kctl -n "${namespace}" get kovabuilds -o json)" >/dev/null || die "KovaBuild remains after exact cleanup"
jq -e '.items | length == 0' <<<"$(kctl -n "${namespace}" get pods -l app.kubernetes.io/name=kova-runner -o json)" >/dev/null || die "runner Pod remains after exact cleanup"
jq -e '.data["queue.json"] | fromjson | (.intents | length) == 0' <<<"$(kctl -n "${namespace}" get configmap kova-service-queue-admission -o json)" >/dev/null || die "queue intent remains after cleanup"
jq -e '.data["reservations.json"] | fromjson | (.active | length) == 0' <<<"$(kctl -n "${namespace}" get configmap kova-service-admission -o json)" >/dev/null || die "active grant remains after cleanup"
snapshot after-cleanup
success=true
note "PASS: exact test-owned CR cleanup verified; both ledgers empty"
