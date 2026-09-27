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
promotion=${ADMISSION_FAILOVER_PROMOTION:-false}
promotion_ack=${ADMISSION_FAILOVER_PROMOTION_ACK:-}
token=
if [[ ${promotion} == false ]]; then token=${SERVICE_AUTH_TOKEN:-}; fi
unset SERVICE_AUTH_TOKEN
export -n token
lease_name=kova-service.kova.cofy.dev

die() { echo "error: $*" >&2; exit 1; }
note() { echo "admission-failover-e2e: $*" >&2; }
kctl() {
  if [[ ${promotion} == true ]]; then
    # A hung API read must not strand the opt-in fixture indefinitely.
    # Port-forward is a deliberately long-lived process; snapshots and API
    # metrics supply their own stricter/longer explicit request timeout.
    case " $* " in
      *" port-forward "*|*" --request-timeout="*) ;;
      *) kubectl --kubeconfig "${kubeconfig}" --request-timeout=15s "$@"; return ;;
    esac
  fi
  kubectl --kubeconfig "${kubeconfig}" "$@"
}

[[ ${mode} == check || ${mode} == run ]] || die "ADMISSION_E2E_MODE must be check or run"
[[ ${promotion} == false || ${promotion} == true ]] || die "ADMISSION_FAILOVER_PROMOTION must be false or true"
[[ ${port} =~ ^[0-9]+$ ]] || die "ADMISSION_E2E_PORT must be a local TCP port"
require_cmd kubectl
require_cmd kind
require_cmd jq
require_cmd curl
require_cmd openssl
require_cmd python3

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
if [[ ${promotion} == true ]]; then
  [[ $(hostname) == wayne-hk-kvm ]] || die "promotion fixture runs only on wayne-hk-kvm"
  [[ ${cluster} == kova-admission-pump ]] || die "promotion fixture requires exact kova-admission-pump Kind"
  [[ ${namespace} == kova && ${release} == kova ]] ||
    die "promotion fixture requires the exact kova namespace and release"
  [[ $(kind get clusters) == "${cluster}" ]] || die "promotion fixture requires this to be the only Kind cluster"
  jq -e '
    [.spec.template.spec.containers[] | select(.name == "kova-service") | .args[]] as $args |
    ["--wait=2h", "--max-build-duration=2h", "--runner-node-selector=never=true"] |
    all(. as $arg | ($args | index($arg)) != null)
  ' <<<"${deployment}" >/dev/null || die "promotion fixture requires a two-hour unschedulable blocker"
  jq -e '[.spec.template.spec.containers[] | select(.name == "kova-service") | .env[]? |
    select(.name == "KOVA_SERVICE_AUTH_TOKEN" and .valueFrom.secretKeyRef.name == "kova-e2e-token" and
      .valueFrom.secretKeyRef.key == "token")] | length == 1' <<<"${deployment}" >/dev/null ||
    die "promotion fixture requires the dedicated kova-e2e-token/token Secret reference"
  jq -e '.items | length == 0' <<<"$(kctl get kovabuilds --all-namespaces -o json)" >/dev/null ||
    die "promotion fixture requires no KovaBuild in any namespace"
  jq -e '.items | length == 0' <<<"$(kctl get pods --all-namespaces -l app.kubernetes.io/name=kova-runner -o json)" >/dev/null ||
    die "promotion fixture requires no runner Pod in any namespace"
  secret_identity=$(kctl -n "${namespace}" get secret kova-e2e-token -o jsonpath='{.metadata.namespace}/{.metadata.name}/{.type}/{.metadata.uid}')
  secret_uid=${secret_identity##*/}
  [[ ${secret_identity} == "${namespace}/kova-e2e-token/Opaque/${secret_uid}" &&
     ${secret_uid} =~ ^[0-9a-f-]{36}$ ]] ||
    die "promotion fixture test-only Secret identity differs"
  initial_lease=$(kctl -n "${leader_ns}" get lease "${lease_name}" -o json)
  lease_uid=$(jq -er '.metadata.uid' <<<"${initial_lease}")
  jq -e --arg holder "${initial_holder}" '.spec.holderIdentity == $holder' <<<"${initial_lease}" >/dev/null ||
    die "promotion fixture Lease changed during preflight"
  require_cmd sha256sum
  kubeconfig_sha=$(sha256sum "${kubeconfig}" | awk '{print $1}')
  live_kind_sha=$(kind get kubeconfig --name "${cluster}" | sha256sum | awk '{print $1}')
  [[ ${kubeconfig_sha} == "${live_kind_sha}" ]] ||
    die "promotion fixture kubeconfig bytes differ from this live Kind cluster"
  deployment_template=$(jq -cS '.spec.template' <<<"${deployment}")
  pod_a_uid=$(jq -er --arg name "${pod_a}" '.items[] | select(.metadata.name == $name) | .metadata.uid' <<<"${pods}")
  pod_b_uid=$(jq -er --arg name "${pod_b}" '.items[] | select(.metadata.name == $name) | .metadata.uid' <<<"${pods}")
  node_facts=$(kctl get nodes -o json)
  jq -e '.items | length == 2 and all(.[];
    [.status.conditions[]? | select(.type == "Ready" and .status == "True")] | length == 1)' \
    <<<"${node_facts}" >/dev/null || die "promotion fixture requires exactly two Ready Kind nodes"
  node_uids=$(jq -cS '[.items[] | {name:.metadata.name,uid:.metadata.uid}] | sort_by(.name)' <<<"${node_facts}")
  kctl --request-timeout=30s get --raw /metrics |
    awk '/^apiserver_request_total\{/{found=1} END{exit !found}' >/dev/null ||
    die "promotion fixture requires readable API Server request counters"
fi
note "read-only leader preflight passed: holder ${initial_holder} belongs to ${initial_leader}"
if [[ ${mode} == check ]]; then
  if [[ ${promotion} == true ]]; then
    note "no cluster writes performed; set ADMISSION_E2E_MODE=run with exact promotion ACK to use the test-only Kind Secret"
  else
    note "no cluster writes performed; set ADMISSION_E2E_MODE=run and SERVICE_AUTH_TOKEN to execute"
  fi
  exit 0
fi
if [[ ${promotion} == true ]]; then
  [[ ${promotion_ack} == "${cluster}/${namespace}/${release}-service" ]] ||
    die "promotion requires ADMISSION_FAILOVER_PROMOTION_ACK=${cluster}/${namespace}/${release}-service"
  # Opt-in live mode never inherits a bearer token. The helper fetches the
  # exact test Secret from the verified Kind into its own process memory.
  unset SERVICE_AUTH_TOKEN
  token=
else
  [[ -n ${token} ]] || die "SERVICE_AUTH_TOKEN is required in default live mode"
  [[ ${#token} -ge 16 && ${#token} -le 512 && ${token} =~ ^[[:graph:]]+$ ]] ||
    die "default test token cannot be safely passed through curl stdin config"
  case ${token} in
    *\"*|*\\*) die "default test token contains curl-config quote or backslash" ;;
  esac
fi
umask 077

platform=$(jq -r '[.spec.template.spec.containers[] | select(.name == "kova-service") | .args[] | select(startswith("--buildkit-platform-addr=")) | split("=")[1]] | first // empty' <<<"${deployment}")
principal=$(jq -r '[.spec.template.spec.containers[] | select(.name == "kova-service") | .args[] | select(startswith("--auth-static-principal=")) | sub("^--auth-static-principal="; "")] | first // empty' <<<"${deployment}")
[[ -n ${principal} && ( ${platform} == linux/amd64 || ${platform} == linux/arm64 ) ]] || die "cannot determine the test principal and platform"
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/kova-admission-failover.XXXXXX")
run_id=$(date -u +%Y%m%dt%H%M%Sz)-$(openssl rand -hex 4)
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
if [[ ${promotion} == true ]]; then
  printf 'cluster=%s\nnamespace=%s\ndeployment_uid=%s\nkubeconfig_sha256=%s\nlive_kind_sha256=%s\nnode_uids=%s\nsecret_uid=%s\n' \
    "${cluster}" "${namespace}" "${deployment_uid}" "${kubeconfig_sha}" "${live_kind_sha}" \
    "${node_uids}" "${secret_uid}" \
    >"${work_dir}/promotion-identity.txt"
fi

forward_pid=
success=false
snapshot() {
  local name=$1
  if [[ ${promotion} == true ]]; then
    kctl --request-timeout=5s -n "${namespace}" get deployment "${release}-service" -o json \
      2>"${work_dir}/${name}-deployment.err" |
      jq '{name:.metadata.name,uid:.metadata.uid,generation:.metadata.generation,
        replicas:.spec.replicas,readyReplicas:.status.readyReplicas,
        updatedReplicas:.status.updatedReplicas,observedGeneration:.status.observedGeneration}' \
      >"${work_dir}/${name}-deployment.json" || true
    kctl --request-timeout=5s -n "${namespace}" get pods -l "${selector}" -o json \
      2>"${work_dir}/${name}-service-pods.err" |
      jq '{items:[.items[] | {name:.metadata.name,uid:.metadata.uid,
        ownerReferences:[.metadata.ownerReferences[]? | {name,uid,kind,controller}],
        deleting:.metadata.deletionTimestamp,phase:.status.phase,
        ready:[.status.conditions[]? | select(.type == "Ready") | .status],
        images:[.status.containerStatuses[]? | {name,imageID}]}]}' \
      >"${work_dir}/${name}-service-pods.json" || true
  else
    kctl --request-timeout=5s -n "${namespace}" get deployment "${release}-service" -o json >"${work_dir}/${name}-deployment.json" 2>"${work_dir}/${name}-deployment.err" || true
    kctl --request-timeout=5s -n "${namespace}" get pods -l "${selector}" -o json >"${work_dir}/${name}-service-pods.json" 2>"${work_dir}/${name}-service-pods.err" || true
  fi
  kctl --request-timeout=5s -n "${leader_ns}" get lease "${lease_name}" -o json >"${work_dir}/${name}-lease.json" 2>"${work_dir}/${name}-lease.err" || true
  if [[ ${promotion} == true ]]; then
    # Unknown concurrent CR specs might contain private caller data. Keep
    # only identity/phase and whether each object belongs to this run.
    kctl --request-timeout=5s -n "${namespace}" get kovabuilds -o json \
      2>"${work_dir}/${name}-builds.err" |
      jq --arg blocker "${blocker_id}" --arg challenger "${challenger_id}" \
        '{items:[.items[] | {name:.metadata.name,uid:.metadata.uid,phase:(.status.phase // ""),
          known_run_id:(.metadata.name == $blocker or .metadata.name == $challenger)}]}' \
      >"${work_dir}/${name}-builds.json" || true
  else
    kctl --request-timeout=5s -n "${namespace}" get kovabuilds -o json >"${work_dir}/${name}-builds.json" 2>"${work_dir}/${name}-builds.err" || true
  fi
  if [[ ${promotion} == true ]]; then
    kctl --request-timeout=5s -n "${namespace}" get pods -l app.kubernetes.io/name=kova-runner -o json \
      2>"${work_dir}/${name}-runners.err" |
      jq '{items:[.items[] | {name:.metadata.name,uid:.metadata.uid,
        ownerReferences:[.metadata.ownerReferences[]? | {name,uid,kind,controller}],
        createAttempt:.metadata.annotations["kova.cofy.dev/create-attempt"],
        deleting:.metadata.deletionTimestamp,phase:.status.phase,
        never:.spec.nodeSelector.never,nodeName:.spec.nodeName}]}' \
      >"${work_dir}/${name}-runners.json" || true
  else
    kctl --request-timeout=5s -n "${namespace}" get pods -l app.kubernetes.io/name=kova-runner -o json >"${work_dir}/${name}-runners.json" 2>"${work_dir}/${name}-runners.err" || true
  fi
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
  local key=$1 label=$2 payload observed
  payload=$(jq -nc --arg uri "${source_uri}" --arg digest "${source_digest}" --arg target "${target}" --arg platform "${platform}" --arg key "${key}" \
    '{source_uri:$uri,source_digest:$digest,targets:[{target:$target,platform:$platform}],format:"oci",concurrency:1,idempotency_key:$key}')
  if [[ ${promotion} == true ]]; then
    if [[ ${label} == blocker ]]; then
      jq -e --arg uid "${deployment_uid}" --argjson template "${deployment_template}" \
        '.metadata.uid == $uid and .spec.template == $template and .spec.replicas == 2 and
         .status.readyReplicas == 2' \
        <<<"$(kctl -n "${namespace}" get deployment "${release}-service" -o json)" >/dev/null ||
        die "blocker POST: initial Service Deployment drifted"
      jq -e --arg a "${pod_a}" --arg auid "${pod_a_uid}" --arg b "${pod_b}" --arg buid "${pod_b_uid}" \
        '.items | length == 2 and
         any(.[]; .metadata.name == $a and .metadata.uid == $auid and .metadata.deletionTimestamp == null) and
         any(.[]; .metadata.name == $b and .metadata.uid == $buid and .metadata.deletionTimestamp == null)' \
        <<<"$(kctl -n "${namespace}" get pods -l "${selector}" -o json)" >/dev/null ||
        die "blocker POST: initial Service Pod UID drifted"
      jq -e --arg uid "${lease_uid}" --arg holder "${initial_holder}" \
        '.metadata.uid == $uid and .spec.holderIdentity == $holder' \
        <<<"$(kctl -n "${leader_ns}" get lease "${lease_name}" -o json)" >/dev/null ||
        die "blocker POST: initial Lease drifted"
    else
      assert_service_identity "${label}-post"
    fi
    if ! printf '%s' "${payload}" | python3 "${root}/scripts/e2e/admission_failover_io.py" post \
      --kubeconfig "${kubeconfig}" --namespace "${namespace}" --port "${port}" \
      --cluster "${cluster}" --kubeconfig-sha256 "${kubeconfig_sha}" \
      --secret-uid "${secret_uid}" \
      --expected-id "$(build_id "${key}")" --receipt "${work_dir}/${label}-response.json"; then
      die "${label} Create was not proven newly accepted; no automatic cleanup"
    fi
    return
  fi
  # curl -q disables a local .curlrc, and --config - receives the token only
  # through stdin. It is never placed in curl argv or the inherited env.
  if ! printf 'header = "Authorization: Bearer %s"\n' "${token}" | curl -q --config - \
    --noproxy '*' --connect-timeout 3 --max-time 15 -sS -X POST \
    -H 'Content-Type: application/json' --data-binary "${payload}" \
    -D "${work_dir}/${label}.headers" -o "${work_dir}/${label}.body" \
    -w '%{http_code}' "http://127.0.0.1:${port}/v1/builds" >"${work_dir}/${label}.status"; then
    echo transport_error >"${work_dir}/${label}.status"
    die "${label} Create outcome is unknown; no automatic cleanup"
  fi
  [[ $(<"${work_dir}/${label}.status") == 202 ]] || die "${label} was not newly accepted (HTTP $(<"${work_dir}/${label}.status"))"
  observed=$(awk 'tolower($1) == "x-kova-build-id:" { gsub("\r", "", $2); print $2 }' "${work_dir}/${label}.headers")
  [[ ${observed} == "$(build_id "${key}")" ]] || die "${label} returned unexpected Build ID ${observed}"
}
uid_delete() {
  local resource=$1 name=$2 uid=$3 label=$4 holder
  if [[ ${label} == leader-pod ]]; then holder=${old_holder}; else holder=${new_holder}; fi
  python3 "${root}/scripts/e2e/admission_failover_io.py" delete \
    --kubeconfig "${kubeconfig}" --kubeconfig-sha256 "${kubeconfig_sha}" \
    --cluster "${cluster}" --namespace "${namespace}" --resource "${resource}" \
    --name "${name}" --uid "${uid}" \
    --lease-name "${lease_name}" --lease-uid "${lease_uid}" --lease-holder "${holder}" \
    --receipt "${work_dir}/${label}-uid-delete.json" ||
    die "${label} UID-preconditioned delete is unproven; evidence retained"
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
if [[ ${promotion} == true ]]; then
  blocker_spec=$(jq -cS '.spec' <<<"${build}")
  active_uid=$(jq -er '.metadata.uid' <<<"${active}")
  queue_uid=$(jq -er '.metadata.uid' <<<"${queue}")
  printf 'blocker_uid=%s\nrunner_uid=%s\nactive_ledger_uid=%s\nqueue_ledger_uid=%s\nlease_uid=%s\n' \
    "${blocker_uid}" "${runner_uid}" "${active_uid}" "${queue_uid}" "${lease_uid}" >>"${work_dir}/promotion-identity.txt"
fi
snapshot blocker-ready

# Resolve the exact current Lease holder immediately before deletion, then
# prove that Pod is controlled by this one dedicated Service Deployment.
read -r old_holder old_leader < <(lease_holder) || die "could not resolve current leader before replacement"
[[ -n ${old_holder} && -n ${old_leader} ]] || die "current Lease holder is not a ready Service Pod"
old_pod=$(kctl -n "${namespace}" get pod "${old_leader}" -o json)
old_pod_uid=$(jq -er '.metadata.uid' <<<"${old_pod}")
if [[ ${promotion} == true ]]; then
  if [[ ${old_leader} == "${pod_a}" ]]; then survivor=${pod_b}; else survivor=${pod_a}; fi
  survivor_uid=$(jq -er --arg name "${survivor}" '.items[] | select(.metadata.name == $name) | .metadata.uid' <<<"${pods}")
fi
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
if [[ ${promotion} == true ]]; then
  jq -e --arg uid "${lease_uid}" --arg holder "${old_holder}" \
    '.metadata.uid == $uid and .spec.holderIdentity == $holder' \
    <<<"$(kctl -n "${leader_ns}" get lease "${lease_name}" -o json)" >/dev/null ||
    die "Lease UID or holder drifted before leader Pod deletion"
  jq -e --arg uid "${deployment_uid}" '.metadata.uid == $uid and .spec.replicas == 2' \
    <<<"$(kctl -n "${namespace}" get deployment "${release}-service" -o json)" >/dev/null ||
    die "Service Deployment drifted before leader Pod deletion"
  uid_delete pods "${old_leader}" "${old_pod_uid}" leader-pod
else
  kctl -n "${namespace}" delete pod "${old_leader}" --wait=false >/dev/null || die "exact leader Pod deletion failed"
fi

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
assert_service_identity() {
  local stage=$1 current_deployment current_pods current_lease current_nodes pod pod_name pod_rs pod_rs_uid rs
  current_deployment=$(kctl -n "${namespace}" get deployment "${release}-service" -o json)
  current_pods=$(kctl -n "${namespace}" get pods -l "${selector}" -o json)
  current_lease=$(kctl -n "${leader_ns}" get lease "${lease_name}" -o json)
  jq -e --arg uid "${deployment_uid}" \
    '.metadata.uid == $uid and .spec.replicas == 2 and .status.readyReplicas == 2 and
     .status.updatedReplicas == 2 and .status.observedGeneration == .metadata.generation' \
    <<<"${current_deployment}" >/dev/null || die "${stage}: Service Deployment identity or readiness drifted"
  jq -e --argjson template "${deployment_template}" '.spec.template == $template' \
    <<<"${current_deployment}" >/dev/null || die "${stage}: Service Deployment template drifted"
  jq -e --arg uid "${lease_uid}" --arg holder "${new_holder}" \
    '.metadata.uid == $uid and .spec.holderIdentity == $holder' <<<"${current_lease}" >/dev/null ||
    die "${stage}: Lease UID or holder drifted"
  jq -e --arg leader "${new_leader}" --arg old "${old_leader}" --arg image "${original_image_id}" \
    '.items | length == 2 and ([.[].metadata.name] | index($leader)) != null and
     all(.[]; .metadata.name != $old and .metadata.deletionTimestamp == null and
       ([.status.conditions[]? | select(.type == "Ready" and .status == "True")] | length == 1) and
       ([.status.containerStatuses[]? | select(.name == "kova-service" and .imageID == $image)] | length == 1))' \
    <<<"${current_pods}" >/dev/null || die "${stage}: Service Pod identity/readiness drifted"
  jq -e --arg name "${survivor}" --arg uid "${survivor_uid}" \
    '[.items[] | select(.metadata.name == $name and .metadata.uid == $uid)] | length == 1' \
    <<<"${current_pods}" >/dev/null || die "${stage}: original follower Pod UID drifted"
  current_nodes=$(kctl get nodes -o json)
  jq -e --argjson expected "${node_uids}" \
    '([.items[] | {name:.metadata.name,uid:.metadata.uid}] | sort_by(.name)) == $expected and
     all(.items[]; [.status.conditions[]? | select(.type == "Ready" and .status == "True")] | length == 1)' \
    <<<"${current_nodes}" >/dev/null || die "${stage}: Kind node UID or readiness drifted"
  while IFS= read -r pod_name; do
    pod=$(jq -c --arg name "${pod_name}" '.items[] | select(.metadata.name == $name)' <<<"${current_pods}")
    pod_rs=$(jq -r '[.metadata.ownerReferences[]? | select(.controller == true and .kind == "ReplicaSet") | .name] |
      if length == 1 then .[0] else empty end' <<<"${pod}")
    pod_rs_uid=$(jq -r '[.metadata.ownerReferences[]? | select(.controller == true and .kind == "ReplicaSet") | .uid] |
      if length == 1 then .[0] else empty end' <<<"${pod}")
    [[ -n ${pod_rs} && -n ${pod_rs_uid} ]] || die "${stage}: Service Pod has no exact ReplicaSet owner"
    rs=$(kctl -n "${namespace}" get replicaset "${pod_rs}" -o json)
    jq -e --arg uid "${pod_rs_uid}" --arg owner "${deployment_uid}" --arg name "${release}-service" \
      '.metadata.uid == $uid and
       ([.metadata.ownerReferences[]? | select(.controller == true and .kind == "Deployment" and
         .name == $name and .uid == $owner)] | length == 1)' <<<"${rs}" >/dev/null ||
      die "${stage}: Service Pod ReplicaSet owner drifted"
  done < <(jq -r '.items[].metadata.name' <<<"${current_pods}")
}
if [[ ${promotion} == true ]]; then assert_service_identity after-handoff; fi
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
        any(.status.conditions[]?; .type == "Ready" and .reason == "WaitingForCapacity" and .message == "waiting for an active job slot")' <<<"${challenger}" >/dev/null; then
    break
  fi
  sleep 1
done
(( SECONDS < deadline )) || die "replacement leader did not reconcile challenger to WaitingForCapacity"
challenger_uid=$(jq -er '.metadata.uid' <<<"${challenger}")
if [[ ${promotion} == true ]]; then
  challenger_spec=$(jq -cS '.spec' <<<"${challenger}")
  printf 'challenger_uid=%s\n' "${challenger_uid}" >>"${work_dir}/promotion-identity.txt"
fi

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
     any(.status.conditions[]?; .type == "Ready" and .reason == "WaitingForCapacity" and .message == "waiting for an active job slot")' <<<"${builds}" >/dev/null || die "${stage}: challenger is no longer waiting for the global active job slot"
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
  if [[ ${promotion} == true ]]; then
    jq -e --arg uid "${active_uid}" '.metadata.uid == $uid' <<<"${active}" >/dev/null ||
      die "${stage}: active ledger object changed"
    jq -e --arg uid "${queue_uid}" '.metadata.uid == $uid' <<<"${queue}" >/dev/null ||
      die "${stage}: queue ledger object changed"
    jq -e --arg uid "${lease_uid}" '.metadata.uid == $uid' <<<"${lease}" >/dev/null ||
      die "${stage}: Lease object changed"
    jq -e --arg blocker "${blocker_id}" --arg blocker_uid "${blocker_uid}" \
      --arg challenger "${challenger_id}" --arg challenger_uid "${challenger_uid}" \
      '.items | length == 2 and all(.[];
        if .metadata.name == $blocker then .metadata.uid == $blocker_uid
        elif .metadata.name == $challenger then .metadata.uid == $challenger_uid
        else false end)' <<<"${builds}" >/dev/null ||
      die "${stage}: run CR UID changed"
    jq -e --arg blocker "${blocker_id}" --arg challenger "${challenger_id}" \
      --argjson blocker_spec "${blocker_spec}" --argjson challenger_spec "${challenger_spec}" \
      '.items | all(.[];
        if .metadata.name == $blocker then .spec == $blocker_spec
        elif .metadata.name == $challenger then .spec == $challenger_spec
        else false end)' <<<"${builds}" >/dev/null || die "${stage}: immutable CR spec drifted"
    jq -e --arg uid "${blocker_uid}" --arg name "${blocker_id}" \
      '.items[0].spec.nodeSelector.never == "true" and .items[0].spec.nodeName == null and
       ([.items[0].metadata.ownerReferences[]? | select(.controller == true and .kind == "KovaBuild" and
         .name == $name and .uid == $uid)] | length == 1)' <<<"${runners}" >/dev/null ||
      die "${stage}: blocker runner owner or unschedulable state changed"
    assert_service_identity "${stage}"
  fi
  snapshot "${stage}"
}
assert_cap_held after-challenger
sleep 10
assert_cap_held stable
note "PASS: replacement leader reconciled challenger but retained the one original runner Pod and active grant"

if [[ ${promotion} == true ]]; then
  # This is a separate opt-in acceptance. The normal #44 path below still
  # deletes the queued challenger before the blocker and never tests promotion.
  all_builds=$(kctl get kovabuilds --all-namespaces -o json)
  all_runners=$(kctl get pods --all-namespaces -l app.kubernetes.io/name=kova-runner -o json)
  jq -e --arg ns "${namespace}" --arg blocker "${blocker_id}" --arg challenger "${challenger_id}" \
    '.items | length == 2 and all(.[]; .metadata.namespace == $ns and
      (.metadata.name == $blocker or .metadata.name == $challenger))' <<<"${all_builds}" >/dev/null ||
    die "pre-release: another KovaBuild appeared in the dedicated Kind"
  jq -e --arg ns "${namespace}" --arg name "kova-job-${blocker_id}" --arg uid "${runner_uid}" \
    '.items | length == 1 and .[0].metadata.namespace == $ns and .[0].metadata.name == $name and
     .[0].metadata.uid == $uid' <<<"${all_runners}" >/dev/null ||
    die "pre-release: another runner Pod appeared in the dedicated Kind"
  assert_cap_held pre-release
  release_started=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  release_seconds=${SECONDS}
  printf '%s\n' "${release_started}" >"${work_dir}/release-started.txt"
  note "releasing only blocker ${blocker_id} (${blocker_uid}) using atomic UID precondition"
  uid_delete kovabuilds "${blocker_id}" "${blocker_uid}" blocker-cr

  challenger_runner_uid=
  assert_promoted() {
    local stage=$1 builds runners active queue
    assert_service_identity "${stage}"
    builds=$(kctl -n "${namespace}" get kovabuilds -o json)
    runners=$(kctl -n "${namespace}" get pods -l app.kubernetes.io/name=kova-runner -o json)
    active=$(kctl -n "${namespace}" get configmap kova-service-admission -o json)
    queue=$(kctl -n "${namespace}" get configmap kova-service-queue-admission -o json)
    jq -e --arg uid "${active_uid}" '.metadata.uid == $uid' <<<"${active}" >/dev/null ||
      die "${stage}: active ledger UID changed"
    jq -e --arg uid "${queue_uid}" '.metadata.uid == $uid' <<<"${queue}" >/dev/null ||
      die "${stage}: queue ledger UID changed"
    jq -e --arg blocker "${blocker_id}" --arg blocker_uid "${blocker_uid}" \
      --arg challenger "${challenger_id}" --arg challenger_uid "${challenger_uid}" \
      --argjson blocker_spec "${blocker_spec}" --argjson challenger_spec "${challenger_spec}" \
      '.items | length >= 1 and length <= 2 and
       all(.[];
         (if .metadata.name == $blocker then .metadata.uid == $blocker_uid
           and .spec == $blocker_spec
          elif .metadata.name == $challenger then .metadata.uid == $challenger_uid
           and .spec == $challenger_spec
          else false end))' <<<"${builds}" >/dev/null ||
      die "${stage}: KovaBuild identity drift or unknown CR"
    jq -e --arg id "${challenger_id}" --arg key "${challenger_key}" \
      '[.items[] | select(.metadata.name == $id)] | length == 1 and
       .[0].spec.idempotencyKey == $key and
       (.[0].status.phase == "Queued" or .[0].status.phase == "Starting")' \
      <<<"${builds}" >/dev/null || die "${stage}: challenger phase or immutable request drifted"
    jq -e --arg blocker "kova-job-${blocker_id}" --arg challenger "kova-job-${challenger_id}" \
      --arg blocker_uid "${runner_uid}" --arg challenger_owner "${challenger_uid}" --arg blocker_owner "${blocker_uid}" \
      '.items | length <= 2 and all(.[];
        .spec.nodeSelector.never == "true" and .spec.nodeName == null and
        (.status.phase == "Pending" or .metadata.deletionTimestamp != null) and
        (if .metadata.name == $blocker then .metadata.uid == $blocker_uid and
           ([.metadata.ownerReferences[]? | select(.controller == true and .kind == "KovaBuild" and .uid == $blocker_owner)] | length == 1)
         elif .metadata.name == $challenger then
           ([.metadata.ownerReferences[]? | select(.controller == true and .kind == "KovaBuild" and .uid == $challenger_owner)] | length == 1)
         else false end))' <<<"${runners}" >/dev/null ||
      die "${stage}: unexpected or schedulable runner Pod"
    jq -e --arg blocker "${blocker_uid}" --arg challenger "${challenger_uid}" \
      --arg blocker_name "${blocker_id}" --arg challenger_name "${challenger_id}" \
      '.data["reservations.json"] | fromjson | .version == 1 and .maxJobs == 1 and
       (.active | length) <= 1 and ([.active | keys[]] | all(. == $blocker or . == $challenger)) and
       all(.active | to_entries[]; .value.slots == 1 and
         .value.buildName == (if .key == $blocker then $blocker_name else $challenger_name end))' \
      <<<"${active}" >/dev/null || die "${stage}: active ledger gained an unknown or extra grant"
    jq -e --arg challenger "${challenger_id}" \
      '.data["queue.json"] | fromjson | .version == 1 and
       (.intents | length) <= 1 and ([.intents | keys[]] | all(. == $challenger)) and
       all(.intents | to_entries[]; (.value.nonce | length) == 32)' \
      <<<"${queue}" >/dev/null || die "${stage}: queue ledger gained an unknown intent"
    if jq -e '.data["queue.json"] | fromjson | (.intents | length) == 1' <<<"${queue}" >/dev/null; then
      local intent_nonce
      intent_nonce=$(jq -er --arg id "${challenger_id}" '.data["queue.json"] | fromjson | .intents[$id].nonce' <<<"${queue}")
      jq -e --arg id "${challenger_id}" --arg nonce "${intent_nonce}" \
        '[.items[] | select(.metadata.name == $id)] | length == 1 and
         .[0].metadata.annotations["kova.cofy.dev/queue-intent"] == $nonce' <<<"${builds}" >/dev/null ||
        die "${stage}: queue intent nonce differs from challenger CR"
    fi
    if ! jq -e --arg id "${challenger_id}" --arg uid "${challenger_uid}" --arg key "${challenger_key}" \
      '.items | length == 1 and .[0].metadata.name == $id and .[0].metadata.uid == $uid and
       .[0].spec.idempotencyKey == $key and .[0].status.phase == "Starting"' <<<"${builds}" >/dev/null; then
      return 1
    fi
    if ! jq -e --arg uid "${challenger_uid}" --arg id "${challenger_id}" \
      '.data["reservations.json"] | fromjson | (.active | length) == 1 and
       .active[$uid].buildName == $id and .active[$uid].slots == 1 and
       ((.active[$uid].inFlight // []) | length) == 0 and (.active[$uid].closing // false) == false' \
      <<<"${active}" >/dev/null; then
      return 1
    fi
    if ! jq -e '.data["queue.json"] | fromjson | (.intents | length) == 0' <<<"${queue}" >/dev/null; then
      return 1
    fi
    if ! jq -e --arg name "kova-job-${challenger_id}" --arg owner "${challenger_uid}" \
      '.items | length == 1 and .[0].metadata.name == $name and .[0].status.phase == "Pending" and
       .[0].metadata.deletionTimestamp == null and
       ([.[0].metadata.ownerReferences[]? | select(.controller == true and .kind == "KovaBuild" and .uid == $owner)] | length == 1) and
       ([.[0].metadata.annotations["kova.cofy.dev/create-attempt"]] | .[0] | length) == 32' \
      <<<"${runners}" >/dev/null; then
      return 1
    fi
    local observed_runner_uid
    observed_runner_uid=$(jq -er '.items[0].metadata.uid' <<<"${runners}")
    if [[ -n ${challenger_runner_uid} && ${challenger_runner_uid} != "${observed_runner_uid}" ]]; then
      die "${stage}: promoted runner UID changed"
    fi
    challenger_runner_uid=${observed_runner_uid}
    snapshot "${stage}"
    return 0
  }

  deadline=$((SECONDS + 120))
  promoted=false
  while (( SECONDS < deadline )); do
    if assert_promoted promotion-ready; then
      promoted=true
      break
    fi
    sleep 2
  done
  [[ ${promoted} == true ]] || die "challenger did not become sole Starting grant and Pending runner within 120s"
  printf 'release_started=%s\npromoted_at=%s\nrelease_to_grant_seconds=%s\n' \
    "${release_started}" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$((SECONDS - release_seconds))" \
    >"${work_dir}/promotion-timing.txt"
  printf 'challenger_runner_uid=%s\n' "${challenger_runner_uid}" >>"${work_dir}/promotion-identity.txt"
  note "PASS: challenger alone acquired the freed slot; latency is observational, not proof of watch causality"

  assert_promoted pre-cleanup || die "promotion identity drifted before exact cleanup"
  uid_delete kovabuilds "${challenger_id}" "${challenger_uid}" challenger-cr
  deadline=$((SECONDS + 120))
  cleaned=false
  while (( SECONDS < deadline )); do
    assert_service_identity cleanup
    builds=$(kctl -n "${namespace}" get kovabuilds -o json)
    runners=$(kctl -n "${namespace}" get pods -l app.kubernetes.io/name=kova-runner -o json)
    active=$(kctl -n "${namespace}" get configmap kova-service-admission -o json)
    queue=$(kctl -n "${namespace}" get configmap kova-service-queue-admission -o json)
    jq -e --arg uid "${active_uid}" '.metadata.uid == $uid' <<<"${active}" >/dev/null || die "cleanup: active ledger UID drifted"
    jq -e --arg uid "${queue_uid}" '.metadata.uid == $uid' <<<"${queue}" >/dev/null || die "cleanup: queue ledger UID drifted"
    jq -e --arg name "${challenger_id}" --arg uid "${challenger_uid}" \
      --argjson spec "${challenger_spec}" \
      '.items | length <= 1 and all(.[]; .metadata.name == $name and .metadata.uid == $uid and .spec == $spec)' \
      <<<"${builds}" >/dev/null || die "cleanup: an unknown KovaBuild appeared"
    jq -e --arg name "kova-job-${challenger_id}" --arg uid "${challenger_runner_uid}" --arg owner "${challenger_uid}" \
      '.items | length <= 1 and all(.[]; .metadata.name == $name and
        .metadata.uid == $uid and .spec.nodeSelector.never == "true" and .spec.nodeName == null and
        ([.metadata.ownerReferences[]? | select(.controller == true and .kind == "KovaBuild" and .uid == $owner)] | length == 1))' \
      <<<"${runners}" >/dev/null || die "cleanup: an unknown or schedulable runner appeared"
    jq -e --arg uid "${challenger_uid}" \
      '.data["reservations.json"] | fromjson | (.active | length) <= 1 and
       ([.active | keys[]] | all(. == $uid))' <<<"${active}" >/dev/null ||
      die "cleanup: active ledger gained an unknown grant"
    jq -e '.data["queue.json"] | fromjson | (.intents | length) == 0' <<<"${queue}" >/dev/null ||
      die "cleanup: queue intent reappeared"
    if jq -e '.items | length == 0' <<<"${builds}" >/dev/null &&
       jq -e '.items | length == 0' <<<"${runners}" >/dev/null &&
       jq -e '.data["reservations.json"] | fromjson | (.active | length) == 0' <<<"${active}" >/dev/null; then
      cleaned=true
      break
    fi
    sleep 2
  done
  [[ ${cleaned} == true ]] || die "exact challenger cleanup did not settle within 120s"
  jq -e '.items | length == 0' <<<"$(kctl get kovabuilds --all-namespaces -o json)" >/dev/null ||
    die "another KovaBuild appeared after promotion cleanup"
  jq -e '.items | length == 0' <<<"$(kctl get pods --all-namespaces -l app.kubernetes.io/name=kova-runner -o json)" >/dev/null ||
    die "another runner Pod appeared after promotion cleanup"
  snapshot after-cleanup

  # The small 3/2-cap fixture can establish an idle post-cleanup baseline,
  # not explain the earlier 1000-build drain. Keep API Server request series
  # and repeat exact empty-state checks for at least two minutes. Service
  # loopback metrics are intentionally disabled in this fixture.
  api_metrics() {
    local label=$1
    kctl --request-timeout=30s get --raw /metrics |
      awk '/^apiserver_request_total\{/{print}' >"${work_dir}/${label}-apiserver-requests.prom" ||
      die "${label}: API Server request metrics unavailable"
    [[ -s ${work_dir}/${label}-apiserver-requests.prom ]] ||
      die "${label}: API Server request counter series absent"
  }
  assert_quiet_empty() {
    local stage=$1 current_active current_queue
    assert_service_identity "${stage}"
    jq -e '.items | length == 0' <<<"$(kctl get kovabuilds --all-namespaces -o json)" >/dev/null ||
      die "${stage}: KovaBuild reappeared"
    jq -e '.items | length == 0' <<<"$(kctl get pods --all-namespaces -l app.kubernetes.io/name=kova-runner -o json)" >/dev/null ||
      die "${stage}: runner Pod reappeared"
    current_active=$(kctl -n "${namespace}" get configmap kova-service-admission -o json)
    current_queue=$(kctl -n "${namespace}" get configmap kova-service-queue-admission -o json)
    jq -e --arg uid "${active_uid}" \
      '.metadata.uid == $uid and (.data["reservations.json"] | fromjson | (.active | length) == 0)' \
      <<<"${current_active}" >/dev/null || die "${stage}: active ledger drifted"
    jq -e --arg uid "${queue_uid}" \
      '.metadata.uid == $uid and (.data["queue.json"] | fromjson | (.intents | length) == 0)' \
      <<<"${current_queue}" >/dev/null || die "${stage}: queue ledger drifted"
  }
  assert_quiet_empty quiet-start
  quiet_start=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  quiet_seconds=${SECONDS}
  api_metrics quiet-start
  quiet_deadline=$((SECONDS + 120))
  while (( SECONDS < quiet_deadline )); do
    sleep 10
    assert_quiet_empty quiet-sample
  done
  api_metrics quiet-end
  printf 'start=%s\nend=%s\nelapsed_seconds=%s\nfixture_caps=active1-queued3-requester2\nservice_metrics=disabled\n' \
    "${quiet_start}" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$((SECONDS - quiet_seconds))" \
    >"${work_dir}/quiet-window.txt"
  snapshot quiet-end
  success=true
  note "PASS: UID-preconditioned release and challenger cleanup; both ledgers empty through a 120s idle observation"
  exit 0
fi

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
