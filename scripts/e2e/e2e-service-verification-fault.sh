#!/usr/bin/env bash

# #45 fault acceptance against an existing disposable two-replica Kind cluster.
# Check mode is read-only. Run mode creates only test-owned CRs and a read-only
# proxy for one run-scoped output repository; it never stops the local registry.
set -euo pipefail

# shellcheck source=scripts/common.sh
source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

root=$(repo_root)
mode=${VERIFICATION_FAULT_MODE:-check}
cluster=${KIND_CLUSTER:-}
namespace=${NAMESPACE:-kova}
release=${RELEASE_NAME:-kova}
repository=${VERIFICATION_FAULT_REPOSITORY:-}
tag=${VERIFICATION_FAULT_TAG:-dev}
ack=${VERIFICATION_FAULT_ACK:-}
proxy_image=${VERIFICATION_FAULT_PROXY_IMAGE:-python:3.12-alpine}
evidence_root=${VERIFICATION_FAULT_EVIDENCE_ROOT:-${root}/.work/verification-fault}
proxy_name=registry-verification-fault
proxy_host=${proxy_name}.${namespace}.svc.cluster.local:5000
lease_name=kova-service.kova.cofy.dev
selector="app.kubernetes.io/instance=${release},app.kubernetes.io/component=service"

die() { echo "error: $*" >&2; exit 1; }
note() { echo "verification-fault-e2e: $*" >&2; }
kctl() { kubectl --kubeconfig "${kubeconfig}" --request-timeout=15s "$@"; }

[[ ${mode} == check || ${mode} == run ]] || die "VERIFICATION_FAULT_MODE must be check or run"
[[ ${cluster} == kova-verification-* ]] || die "use a dedicated kova-verification-* Kind cluster"
[[ ${namespace} == kova && ${release} == kova ]] || die "this acceptance owns only kova/kova"
[[ ${repository} =~ ^kova-examples/verification-45-[a-z0-9-]+$ ]] || die "repository must be an exact verification-45 run repository"
[[ ${tag} =~ ^[a-z0-9-]+$ ]] || die "invalid run tag"
require_cmd kind
require_cmd kubectl
require_cmd helm
require_cmd jq
require_cmd curl
require_cmd openssl

kubeconfig=${KIND_KUBECONFIG:-.kind/${cluster}.kubeconfig}
if [[ ${kubeconfig} != /* ]]; then kubeconfig=${root}/${kubeconfig}; fi
[[ -f ${kubeconfig} ]] || die "dedicated kubeconfig is absent"
[[ $(kind get clusters | wc -l | tr -d ' ') == 1 ]] || die "only one Kind cluster may exist on this host"
kind get clusters | awk -v expected="${cluster}" '$0 == expected { found = 1 } END { exit !found }' || die "Kind cluster identity mismatch"
fingerprint() {
  jq -cer '{server:.clusters[0].cluster.server,ca:.clusters[0].cluster["certificate-authority-data"],cert:.users[0].user["client-certificate-data"],key:.users[0].user["client-key-data"]} | select(all(.[]; type == "string" and length > 0))' |
    openssl dgst -sha256 | awk '{print $NF}'
}
actual_fingerprint=$(kubectl --kubeconfig "${kubeconfig}" config view --raw --minify -o json | fingerprint)
kind_fingerprint=$(kind get kubeconfig --name "${cluster}" | kubectl --kubeconfig /dev/stdin config view --raw --minify -o json | fingerprint)
[[ -n ${actual_fingerprint} && ${actual_fingerprint} == "${kind_fingerprint}" ]] || die "kubeconfig does not match live Kind credentials/server"
jq -e '.items | length == 2 and all(.[]; any(.status.conditions[]?; .type == "Ready" and .status == "True"))' <<<"$(kctl get nodes -o json)" >/dev/null || die "Kind nodes are not 2/2 Ready"
jq -e '.items | length == 0' <<<"$(kctl get kovabuilds --all-namespaces -o json)" >/dev/null || die "KovaBuild already exists"
jq -e '.items | length == 0' <<<"$(kctl get pods --all-namespaces -l app.kubernetes.io/name=kova-runner -o json)" >/dev/null || die "runner Pod already exists"
deployment=$(kctl -n "${namespace}" get deployment "${release}-service" -o json)
jq -e '.spec.replicas == 2 and .status.readyReplicas == 2' <<<"${deployment}" >/dev/null || die "two Service replicas must be Ready"
deployment_uid=$(jq -er '.metadata.uid' <<<"${deployment}")
pods=$(kctl -n "${namespace}" get pods -l "${selector}" -o json)
jq -e '.items | length == 2 and all(.[]; any(.status.conditions[]?; .type == "Ready" and .status == "True"))' <<<"${pods}" >/dev/null || die "Service Pods are not 2/2 Ready"
active=$(kctl -n "${namespace}" get configmap kova-service-admission -o json)
queue=$(kctl -n "${namespace}" get configmap kova-service-queue-admission -o json)
jq -e '.data["reservations.json"] | fromjson | .active == {}' <<<"${active}" >/dev/null || die "active ledger is not empty"
jq -e '.data["queue.json"] | fromjson | .intents == {}' <<<"${queue}" >/dev/null || die "queue ledger is not empty"
for object in "configmap/${proxy_name}" "deployment/${proxy_name}" "service/${proxy_name}"; do
  ! kctl -n "${namespace}" get "${object}" >/dev/null 2>&1 || die "test proxy ${object} already exists"
done
registry_code=$(curl --noproxy '*' --connect-timeout 3 --max-time 5 -sS -o /dev/null -w '%{http_code}' http://127.0.0.1:5002/v2/)
[[ ${registry_code} == 200 ]] || die "local registry is not healthy"
source_headers=$(curl --noproxy '*' --connect-timeout 3 --max-time 10 -fsSI -H 'Accept: application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json' "http://127.0.0.1:5002/v2/${repository}/manifests/${tag}") || die "run-scoped source output tag is absent"
digest=$(awk 'tolower($1) == "docker-content-digest:" {gsub("\r", "", $2); print $2}' <<<"${source_headers}")
[[ ${digest} =~ ^sha256:[a-f0-9]{64}$ ]] || die "source output tag has no exact manifest digest"
note "read-only preflight passed: ${cluster}/${namespace}, 2/2 nodes and Service Pods, empty ledgers, ${repository}:${tag}@${digest}"
if [[ ${mode} == check ]]; then
  note "no writes; run requires VERIFICATION_FAULT_MODE=run and VERIFICATION_FAULT_ACK=${cluster}/${namespace}/${repository}:${tag}"
  exit 0
fi
[[ ${ack} == "${cluster}/${namespace}/${repository}:${tag}" ]] || die "explicit acknowledgement must match cluster, namespace and run-scoped tag"

mkdir -p "${evidence_root}"
work_dir=$(mktemp -d "${evidence_root}/verification-45.XXXXXX")
run_id=$(date -u +%Y%m%dt%H%M%sz)-$(openssl rand -hex 4)
first_id="verification-503-${run_id}"
second_id="verification-timeout-${run_id}"
target="${proxy_host}/${repository}:${tag}"
printf 'cluster=%s\nnamespace=%s\nrepository=%s\ntag=%s\ndigest=%s\nproxy=%s\nfirst_id=%s\nsecond_id=%s\n' \
  "${cluster}" "${namespace}" "${repository}" "${tag}" "${digest}" "${proxy_host}" "${first_id}" "${second_id}" >"${work_dir}/identities.txt"
success=false
proxy_forward=
snapshot() {
  local stage=$1
  kctl -n "${namespace}" get kovabuilds -o json >"${work_dir}/${stage}-builds.json" 2>"${work_dir}/${stage}-builds.err" || true
  kctl -n "${namespace}" get pods -l "${selector}" -o json >"${work_dir}/${stage}-service-pods.json" 2>"${work_dir}/${stage}-service-pods.err" || true
  kctl -n "${namespace}" get pods -l app.kubernetes.io/name=kova-runner -o json >"${work_dir}/${stage}-runners.json" 2>"${work_dir}/${stage}-runners.err" || true
  kctl -n "${namespace}" get lease "${lease_name}" -o json >"${work_dir}/${stage}-lease.json" 2>"${work_dir}/${stage}-lease.err" || true
  kctl -n "${namespace}" get configmap kova-service-admission -o json >"${work_dir}/${stage}-active.json" 2>"${work_dir}/${stage}-active.err" || true
  kctl -n "${namespace}" get configmap kova-service-queue-admission -o json >"${work_dir}/${stage}-queue.json" 2>"${work_dir}/${stage}-queue.err" || true
  kctl -n "${namespace}" get events --field-selector "involvedObject.kind=KovaBuild" -o json >"${work_dir}/${stage}-events.json" 2>"${work_dir}/${stage}-events.err" || true
  kctl -n "${namespace}" logs "deployment/${proxy_name}" --tail=200 >"${work_dir}/${stage}-proxy.log" 2>"${work_dir}/${stage}-proxy.err" || true
}
finish() {
  local code=$?
  trap - EXIT
  if [[ -n ${proxy_forward} ]]; then kill "${proxy_forward}" 2>/dev/null || true; wait "${proxy_forward}" 2>/dev/null || true; fi
  snapshot final
  if [[ ${success} == true && ${code} == 0 ]]; then
    note "PASS: evidence preserved at ${work_dir}; exact test CRs and proxy objects removed; dedicated Kind retained for owner review"
  else
    note "FAILED: evidence preserved at ${work_dir}; do not remove test CRs, proxy or Kind before inspection"
  fi
  exit "${code}"
}
trap finish EXIT
snapshot before

# The proxy accepts only GET/HEAD for this one repository and forwards to the
# existing registry. Its ClusterIP is reachable only inside this test Kind.
kctl -n "${namespace}" create configmap "${proxy_name}" \
  --from-file="proxy.py=${root}/scripts/e2e/registry-verification-fault-proxy.py" \
  --from-literal=mode=503 >"${work_dir}/proxy-configmap-create.txt"
jq -n --arg name "${proxy_name}" --arg namespace "${namespace}" --arg repo "${repository}" --arg image "${proxy_image}" \
  '{apiVersion:"apps/v1",kind:"Deployment",metadata:{name:$name,namespace:$namespace,labels:{"kova.cofy.dev/e2e":"verification-fault"}},spec:{replicas:1,selector:{matchLabels:{"kova.cofy.dev/e2e":"verification-fault-proxy"}},template:{metadata:{labels:{"kova.cofy.dev/e2e":"verification-fault-proxy"}},spec:{securityContext:{runAsNonRoot:true,runAsUser:65532,runAsGroup:65532},containers:[{name:"proxy",image:$image,imagePullPolicy:"IfNotPresent",command:["python3","-u","/fault/proxy.py"],env:[{name:"FAULT_REPOSITORY",value:$repo}],ports:[{containerPort:5000,name:"http"}],readinessProbe:{httpGet:{path:"/v2/",port:5000},periodSeconds:2},resources:{requests:{cpu:"20m",memory:"32Mi"},limits:{cpu:"500m",memory:"128Mi"}},volumeMounts:[{name:"fault",mountPath:"/fault",readOnly:true}]}],volumes:[{name:"fault",configMap:{name:$name}}]}}}}' |
  kctl create -f - >"${work_dir}/proxy-deployment-create.txt"
jq -n --arg name "${proxy_name}" --arg namespace "${namespace}" \
  '{apiVersion:"v1",kind:"Service",metadata:{name:$name,namespace:$namespace,labels:{"kova.cofy.dev/e2e":"verification-fault"}},spec:{type:"ClusterIP",selector:{"kova.cofy.dev/e2e":"verification-fault-proxy"},ports:[{name:"http",port:5000,targetPort:5000}]}}' |
  kctl create -f - >"${work_dir}/proxy-service-create.txt"
kctl -n "${namespace}" rollout status "deployment/${proxy_name}" --timeout=180s >"${work_dir}/proxy-rollout.txt"
kctl -n "${namespace}" port-forward --address 127.0.0.1 "service/${proxy_name}" 18087:5000 >"${work_dir}/proxy-port-forward.log" 2>&1 &
proxy_forward=$!
mode_wait() {
  local expected=$1 deadline=$((SECONDS + 70)) observed
  while (( SECONDS < deadline )); do
    kill -0 "${proxy_forward}" 2>/dev/null || die "proxy port-forward exited"
    observed=$(curl --noproxy '*' --connect-timeout 2 --max-time 2 -fsS http://127.0.0.1:18087/fault/mode 2>/dev/null || true)
    [[ ${observed} == "${expected}" ]] && return
    sleep 1
  done
  die "proxy did not enter ${expected} mode"
}
mode_wait 503
kctl -n "${namespace}" get configmap "${proxy_name}" -o json >"${work_dir}/proxy-initial-config.json"
helm upgrade "${release}" "${root}/charts/kova" --kubeconfig "${kubeconfig}" -n "${namespace}" --reuse-values --wait --timeout=180s \
  --set-string 'serviceDaemon.registryPlainHTTP[0]=kind-registry:5000' \
  --set-string "serviceDaemon.registryPlainHTTP[1]=${proxy_host}" >"${work_dir}/helm-upgrade.txt"
kctl -n "${namespace}" rollout status "deployment/${release}-service" --timeout=180s >"${work_dir}/service-rollout.txt"
helm get values "${release}" --kubeconfig "${kubeconfig}" -n "${namespace}" -o json >"${work_dir}/helm-values.json"
snapshot proxy-ready

set_mode() {
  local desired=$1
  kctl -n "${namespace}" patch configmap "${proxy_name}" --type=merge -p "{\"data\":{\"mode\":\"${desired}\"}}" >"${work_dir}/mode-${desired}-patch.txt"
  mode_wait "${desired}"
}
create_fixture() {
  local id=$1 now deadline status_payload
  kctl -n "${namespace}" scale "deployment/${release}-service" --replicas=0 >"${work_dir}/${id}-scale-down.txt"
  kctl -n "${namespace}" wait --for=delete pod -l "${selector}" --timeout=120s >"${work_dir}/${id}-drain.txt"
  now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  deadline=$(date -u -d '+4 minutes' +%Y-%m-%dT%H:%M:%SZ)
  jq -n --arg name "${id}" --arg namespace "${namespace}" --arg target "${target}" \
    '{apiVersion:"kova.cofy.dev/v1alpha1",kind:"KovaBuild",metadata:{name:$name,namespace:$namespace,labels:{"kova.cofy.dev/e2e":"verification-fault"}},spec:{requester:{username:"verification-fault-e2e"},source:{uri:"oci://kind-registry:5000/kova-sources/verification-fault@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",digest:"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},targets:[{target:$target,platform:"linux/amd64"}],build:{format:"oci",concurrency:1}}}' |
    kctl create -f - >"${work_dir}/${id}-create.txt"
  status_payload=$(jq -cn --arg now "${now}" --arg deadline "${deadline}" --arg target "${target}" --arg digest "${digest}" \
    '{status:{phase:"Verifying",reason:"InjectedDurableReceipt",verificationStartedAt:$now,verificationDeadlineAt:$deadline,verificationNextAttemptAt:$now,verificationResults:[{format:"oci",image:$target,platform:"linux/amd64",pushedDigest:$digest,state:"pending"}]}}')
  kctl -n "${namespace}" patch kovabuild "${id}" --type=merge --subresource=status -p "${status_payload}" >"${work_dir}/${id}-status-patch.txt"
  kctl -n "${namespace}" get kovabuild "${id}" -o json >"${work_dir}/${id}-seeded.json"
  kctl -n "${namespace}" scale "deployment/${release}-service" --replicas=2 >"${work_dir}/${id}-scale-up.txt"
  kctl -n "${namespace}" rollout status "deployment/${release}-service" --timeout=180s >"${work_dir}/${id}-rollout.txt"
}
wait_pending() {
  local id=$1 deadline=$((SECONDS + 90)) observed
  while (( SECONDS < deadline )); do
    observed=$(kctl -n "${namespace}" get kovabuild "${id}" -o json)
    if jq -e '.status.phase == "Verifying" and .status.verificationAttempts >= 1 and .status.verificationResults[0].state == "pending" and .status.verificationLastError == "registry manifest verification unavailable" and (.status.outputs // [] | length) == 0' <<<"${observed}" >/dev/null; then
      printf '%s\n' "${observed}" >"${work_dir}/${id}-pending.json"
      return
    fi
    if jq -e '.status.phase == "Succeeded" or .status.phase == "Failed"' <<<"${observed}" >/dev/null; then
      printf '%s\n' "${observed}" >"${work_dir}/${id}-unexpected-terminal.json"
      die "${id} reached terminal phase during injected fault"
    fi
    sleep 1
  done
  die "${id} did not retain a pending durable receipt under fault"
}
wait_succeeded() {
  local id=$1 deadline=$((SECONDS + 150)) observed
  while (( SECONDS < deadline )); do
    observed=$(kctl -n "${namespace}" get kovabuild "${id}" -o json)
    if jq -e --arg digest "${digest}" '.status.phase == "Succeeded" and .status.verificationAttempts >= 2 and .status.verificationResults[0].state == "succeeded" and .status.verificationResults[0].pushedDigest == $digest and .status.outputs[0].manifestDigest == $digest' <<<"${observed}" >/dev/null; then
      printf '%s\n' "${observed}" >"${work_dir}/${id}-succeeded.json"
      return
    fi
    if jq -e '.status.phase == "Failed"' <<<"${observed}" >/dev/null; then
      printf '%s\n' "${observed}" >"${work_dir}/${id}-unexpected-terminal.json"
      die "${id} failed after fault recovery"
    fi
    sleep 1
  done
  die "${id} did not recover to a durable verified success"
}

create_fixture "${first_id}"
wait_pending "${first_id}"
snapshot after-503
holder=$(kctl -n "${namespace}" get lease "${lease_name}" -o json | jq -er '.spec.holderIdentity')
leader=${holder%%_*}
pod=$(kctl -n "${namespace}" get pod "${leader}" -o json)
pod_uid=$(jq -er '.metadata.uid' <<<"${pod}")
rs_name=$(jq -er '[.metadata.ownerReferences[]? | select(.controller == true and .kind == "ReplicaSet") | .name] | if length == 1 then .[0] else empty end' <<<"${pod}")
rs_uid=$(jq -er '[.metadata.ownerReferences[]? | select(.controller == true and .kind == "ReplicaSet") | .uid] | if length == 1 then .[0] else empty end' <<<"${pod}")
rs=$(kctl -n "${namespace}" get replicaset "${rs_name}" -o json)
jq -e --arg uid "${rs_uid}" --arg deployment_uid "${deployment_uid}" '.metadata.uid == $uid and any(.metadata.ownerReferences[]?; .controller == true and .kind == "Deployment" and .uid == $deployment_uid)' <<<"${rs}" >/dev/null || die "Lease holder is not owned by exact test Service Deployment"
printf 'holder=%s\npod=%s\nuid=%s\n' "${holder}" "${leader}" "${pod_uid}" >"${work_dir}/leader-before-restart.txt"
kctl -n "${namespace}" delete pod "${leader}" --wait=true --timeout=120s >"${work_dir}/leader-delete.txt"
leader_deadline=$((SECONDS + 90))
while (( SECONDS < leader_deadline )); do
  new_holder=$(kctl -n "${namespace}" get lease "${lease_name}" -o json | jq -r '.spec.holderIdentity // empty')
  [[ -n ${new_holder} && ${new_holder} != "${holder}" ]] && break
  sleep 2
done
[[ -n ${new_holder} && ${new_holder} != "${holder}" ]] || die "leader did not hand off after exact Pod deletion"
kctl -n "${namespace}" rollout status "deployment/${release}-service" --timeout=180s >"${work_dir}/leader-replacement-rollout.txt"
printf 'holder=%s\n' "${new_holder}" >"${work_dir}/leader-after-restart.txt"
snapshot after-leader-restart
set_mode healthy
wait_succeeded "${first_id}"
snapshot after-503-recovery

set_mode timeout
create_fixture "${second_id}"
wait_pending "${second_id}"
snapshot after-timeout
set_mode healthy
wait_succeeded "${second_id}"
snapshot after-timeout-recovery

jq -e --arg first "${first_id}" --arg second "${second_id}" '[.items[] | select(.metadata.name == $first or .metadata.name == $second)] | length == 2' <<<"$(kctl -n "${namespace}" get kovabuilds -o json)" >/dev/null || die "exact test CR identities changed before cleanup"
kctl -n "${namespace}" delete kovabuild "${first_id}" "${second_id}" --wait=true --timeout=120s >"${work_dir}/exact-cr-delete.txt"
jq -e '.items | length == 0' <<<"$(kctl get kovabuilds --all-namespaces -o json)" >/dev/null || die "a KovaBuild remains after exact CR cleanup"
jq -e '.items | length == 0' <<<"$(kctl get pods --all-namespaces -l app.kubernetes.io/name=kova-runner -o json)" >/dev/null || die "runner Pod appeared in synthetic verification test"
jq -e '.data["reservations.json"] | fromjson | .active == {}' <<<"$(kctl -n "${namespace}" get configmap kova-service-admission -o json)" >/dev/null || die "active ledger is not empty after test"
jq -e '.data["queue.json"] | fromjson | .intents == {}' <<<"$(kctl -n "${namespace}" get configmap kova-service-queue-admission -o json)" >/dev/null || die "queue ledger is not empty after test"
snapshot after-cr-cleanup
kctl -n "${namespace}" delete service "${proxy_name}" --wait=true --timeout=60s >"${work_dir}/proxy-service-delete.txt"
kctl -n "${namespace}" delete deployment "${proxy_name}" --wait=true --timeout=60s >"${work_dir}/proxy-deployment-delete.txt"
kctl -n "${namespace}" delete configmap "${proxy_name}" --wait=true --timeout=60s >"${work_dir}/proxy-configmap-delete.txt"
success=true
note "503, bounded timeout, leader replacement, digest-pinned recovery and exact cleanup passed"
