#!/usr/bin/env bash

set -euo pipefail

source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

ROOT=$(repo_root)
CONTROLLER_IMAGE=${CONTROLLER_IMAGE:-localhost:5002/kova:controller-dev}
RUNNER_IMAGE=${RUNNER_IMAGE:-localhost:5002/kova:runner-dev}
WORKER_IMAGE=${WORKER_IMAGE:-localhost:5002/kova:worker-dev}
KIND_KUBECONFIG=${KIND_KUBECONFIG:-.kind/kova-local.kubeconfig}
RELEASE_NAME=${RELEASE_NAME:-kova}
NAMESPACE=${NAMESPACE:-kova}
CLUSTER_REGISTRY=${CLUSTER_REGISTRY:-kind-registry:5000}
REGISTRY_HOST=${REGISTRY_HOST:-localhost:5002}
SERVICE_PORT=${SERVICE_PORT:-18080}
SERVICE_AUTH_TOKEN=${SERVICE_AUTH_TOKEN:-service-e2e-token}
SERVICE_AUTH_SECRET=${SERVICE_AUTH_SECRET:-kova-service-auth}
SERVICE_TARGET=${SERVICE_TARGET:-${CLUSTER_REGISTRY}/kova-examples/simple:dev}
SERVICE_PULL_TARGET=${SERVICE_PULL_TARGET:-${REGISTRY_HOST}/kova-examples/simple:dev}
SOURCE_REPOSITORY=${SOURCE_REPOSITORY:-${REGISTRY_HOST}/kova-sources/service-e2e:latest}
E2E_SERVICE_BUILD_IMAGE=${E2E_SERVICE_BUILD_IMAGE:-true}
E2E_SERVICE_BUILD_CLI=${E2E_SERVICE_BUILD_CLI:-true}
KOVA_CLI=${KOVA_CLI:-${ROOT}/bin/kova}
SERVICE_JOB_TTL=${SERVICE_JOB_TTL:-30s}
KOVA_CHART=${KOVA_CHART:-${ROOT}/charts/kova}
KOVA_VALUES=${KOVA_VALUES:-${ROOT}/deploy/kind-values.yaml}
BASELINE_CHART=${BASELINE_CHART:-}
BASELINE_CONTROLLER_IMAGE=${BASELINE_CONTROLLER_IMAGE:-}
BASELINE_RUNNER_IMAGE=${BASELINE_RUNNER_IMAGE:-}
BASELINE_WORKER_IMAGE=${BASELINE_WORKER_IMAGE:-}
KOVA_PLATFORM=$(kova_platform)

require_cmd curl
require_cmd docker
require_cmd helm
require_cmd jq
require_cmd kubectl

if [[ "${E2E_SERVICE_BUILD_CLI}" == "true" ]]; then
  make -C "${ROOT}" kova
fi
test -x "${KOVA_CLI}"
if [[ "${E2E_SERVICE_BUILD_IMAGE}" == "true" ]]; then
  make -C "${ROOT}" image
fi

baseline_revision=""
if [[ -n "${BASELINE_CHART}" ]]; then
  if [[ -z "${BASELINE_CONTROLLER_IMAGE}" || -z "${BASELINE_RUNNER_IMAGE}" || -z "${BASELINE_WORKER_IMAGE}" ]]; then
    echo "error: baseline chart validation requires all three BASELINE_*_IMAGE values" >&2
    exit 2
  fi
  KOVA_CHART=${BASELINE_CHART} \
    CONTROLLER_IMAGE=${BASELINE_CONTROLLER_IMAGE} \
    RUNNER_IMAGE=${BASELINE_RUNNER_IMAGE} \
    WORKER_IMAGE=${BASELINE_WORKER_IMAGE} \
    "${ROOT}/scripts/kind/deploy-kind.sh"
  baseline_revision=$(helm history "${RELEASE_NAME}" \
    --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" \
    --namespace "${NAMESPACE}" -o json | jq -r 'last.revision')
else
  "${ROOT}/scripts/kind/deploy-kind.sh"
fi

helm show crds "${KOVA_CHART}" | \
  kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" apply -f -
kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" -n "${NAMESPACE}" \
  delete kovabuild --all --ignore-not-found --wait=false || true
kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" -n "${NAMESPACE}" \
  delete pod -l 'app.kubernetes.io/name=kova-runner' --ignore-not-found || true

sync_service_auth_secret() (
  local token_file
  umask 077
  token_file=$(mktemp "${TMPDIR:-/tmp}/kova-service-e2e-token.XXXXXX")
  trap 'rm -f "${token_file}"' EXIT
  printf '%s' "${SERVICE_AUTH_TOKEN}" >"${token_file}"
  kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" -n "${NAMESPACE}" \
    create secret generic "${SERVICE_AUTH_SECRET}" --from-file=token="${token_file}" \
    --dry-run=client -o yaml | \
    kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" apply -f - >/dev/null
)
sync_service_auth_secret

helm upgrade --install "${RELEASE_NAME}" "${KOVA_CHART}" \
  --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" \
  --namespace "${NAMESPACE}" --create-namespace --wait --timeout 180s \
  -f "${KOVA_VALUES}" \
  --set-string "images.controller.repository=${CONTROLLER_IMAGE%:*}" \
  --set-string "images.controller.tag=${CONTROLLER_IMAGE##*:}" \
  --set-string "images.runner.repository=${RUNNER_IMAGE%:*}" \
  --set-string "images.runner.tag=${RUNNER_IMAGE##*:}" \
  --set-string "images.worker.repository=${WORKER_IMAGE%:*}" \
  --set-string "images.worker.tag=${WORKER_IMAGE##*:}" \
  --set-string "worker.platform=${KOVA_PLATFORM}" \
  --set serviceDaemon.enabled=true \
  --set-string serviceDaemon.authentication.mode=static \
  --set-string "serviceDaemon.authentication.staticTokenSecret.name=${SERVICE_AUTH_SECRET}" \
  --set-string serviceDaemon.authentication.staticTokenSecret.key=token \
  --set-string serviceDaemon.authentication.staticPrincipal=kova:e2e \
  --set "serviceDaemon.jobTTL=${SERVICE_JOB_TTL}" \
  --set serviceDaemon.runnerImagePullSecret= \
  --set-string "serviceDaemon.registryPlainHTTP[0]=${CLUSTER_REGISTRY}"

kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" -n "${NAMESPACE}" \
  create rolebinding "${RELEASE_NAME}-e2e-submitter" \
  --role="${RELEASE_NAME}-service-submitter" --user=kova:e2e \
  --dry-run=client -o yaml | \
  kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" apply -f - >/dev/null
kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" -n "${NAMESPACE}" \
  rollout status "deployment/${RELEASE_NAME}-service" --timeout=180s

too_many_targets=$(jq -n --arg registry "${CLUSTER_REGISTRY}" --arg platform "${KOVA_PLATFORM}" \
  '[range(0;101) | {target: ($registry + "/kova-examples/limit-" + tostring + ":dev"), platform: $platform}]')
if jq -n --argjson targets "${too_many_targets}" --arg namespace "${NAMESPACE}" --arg uri \
  "oci://${CLUSTER_REGISTRY}/kova-sources/invalid@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" \
  '{apiVersion:"kova.cofy.dev/v1alpha1",kind:"KovaBuild",metadata:{name:"too-many-targets",namespace:$namespace},spec:{requester:{username:"e2e"},targets:$targets,source:{uri:$uri,digest:"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},build:{format:"oci",concurrency:1}}}' | \
  kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" create --dry-run=server -f - >/dev/null 2>&1; then
  echo "error: Kubernetes API accepted 101 logical targets" >&2
  exit 1
fi

assert_kubernetes_rejects_targets() {
  local label=$1 targets=$2
  if jq -n --argjson targets "${targets}" --arg namespace "${NAMESPACE}" --arg uri \
    "oci://${CLUSTER_REGISTRY}/kova-sources/invalid@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" \
    '{apiVersion:"kova.cofy.dev/v1alpha1",kind:"KovaBuild",metadata:{generateName:"invalid-targets-",namespace:$namespace},spec:{requester:{username:"e2e"},targets:$targets,source:{uri:$uri,digest:"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},build:{format:"oci",concurrency:1}}}' | \
    kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" create --dry-run=server -f - >/dev/null 2>&1; then
    echo "error: Kubernetes API accepted ${label} targets" >&2
    exit 1
  fi
}

assert_kubernetes_rejects_targets empty-array '[]'
assert_kubernetes_rejects_targets empty-string "[{\"target\":\"\",\"platform\":\"${KOVA_PLATFORM}\"}]"
assert_kubernetes_rejects_targets duplicate "[{\"target\":\"${SERVICE_TARGET}\",\"platform\":\"${KOVA_PLATFORM}\"},{\"target\":\"${SERVICE_TARGET}\",\"platform\":\"${KOVA_PLATFORM}\"}]"
assert_kubernetes_rejects_targets whitespace "[{\"target\":\" ${SERVICE_TARGET}\",\"platform\":\"${KOVA_PLATFORM}\"}]"
assert_kubernetes_rejects_targets invalid "[{\"target\":\"not a reference\",\"platform\":\"${KOVA_PLATFORM}\"}]"
assert_kubernetes_rejects_targets digest-only \
  "[{\"target\":\"${CLUSTER_REGISTRY}/kova-examples/simple@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"platform\":\"${KOVA_PLATFORM}\"}]"
overlong_target=$(jq -nr --arg registry "${CLUSTER_REGISTRY}" '$registry + "/kova-examples/" + ("a" * 500) + ":dev"')
assert_kubernetes_rejects_targets overlong "[{\"target\":\"${overlong_target}\",\"platform\":\"${KOVA_PLATFORM}\"}]"
assert_kubernetes_rejects_targets unsupported-platform "[{\"target\":\"${SERVICE_TARGET}\",\"platform\":\"linux/s390x\"}]"

source_push=$("${KOVA_CLI}" source push --target "${SERVICE_TARGET}" \
  --platform "${KOVA_PLATFORM}" \
  --repository "${SOURCE_REPOSITORY}" \
  --registry-plain-http "${REGISTRY_HOST}" \
  "${ROOT}/examples/simple")
source_uri=$(printf '%s' "${source_push}" | jq -r '.uri')
source_digest=$(printf '%s' "${source_push}" | jq -r '.digest')
source_uri=${source_uri/oci:\/\/${REGISTRY_HOST}/oci:\/\/${CLUSTER_REGISTRY}}
[[ "${source_uri}" == oci://*@sha256:* ]]
[[ "${source_digest}" =~ ^sha256:[a-f0-9]{64}$ ]]

port_forward_log=$(mktemp)
kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" -n "${NAMESPACE}" \
  port-forward "svc/${RELEASE_NAME}-service" "${SERVICE_PORT}:8080" >"${port_forward_log}" 2>&1 &
port_forward_pid=$!
cleanup() {
  kill "${port_forward_pid}" >/dev/null 2>&1 || true
  wait "${port_forward_pid}" 2>/dev/null || true
  rm -f "${port_forward_log}"
}
trap cleanup EXIT
wait_for_tcp 127.0.0.1 "${SERVICE_PORT}" 30

BASE="http://127.0.0.1:${SERVICE_PORT}"
auth_header=(-H "Authorization: Bearer ${SERVICE_AUTH_TOKEN}")
curl -fsS "${BASE}/healthz" >/dev/null
curl -fsS "${BASE}/version" | jq -e '.api_version == "v1"' >/dev/null
KOVA_SERVICE_TOKEN=${SERVICE_AUTH_TOKEN} "${KOVA_CLI}" --service-url "${BASE}" doctor | \
  jq -e '.checks | all(.status == "ok")' >/dev/null

create_build() {
  local target=$1
  jq -n --arg source_uri "${source_uri}" --arg source_digest "${source_digest}" \
    --arg target "${target}" --arg platform "${KOVA_PLATFORM}" --arg registry "${CLUSTER_REGISTRY}" \
    '{source_uri:$source_uri,source_digest:$source_digest,targets:[{target:$target,platform:$platform}],format:"both",concurrency:1,timeout:600,fail_fast:true,verbose:true,variables:["KOVA_IMAGE_REGISTRY="+$registry]}' | \
    curl -fsS -X POST "${auth_header[@]}" -H 'Content-Type: application/json' \
      --data-binary @- "${BASE}/v1/builds"
}

wait_for_terminal() {
  local job_id=$1 deadline status response
  deadline=$((SECONDS + 900))
  while (( SECONDS < deadline )); do
    response=$(curl -fsS "${auth_header[@]}" "${BASE}/v1/builds/${job_id}")
    status=$(printf '%s' "${response}" | jq -r '.status')
    case "${status}" in
      succeeded|failed|cancelled)
        printf '%s\n' "${status}"
        return
        ;;
    esac
    sleep 5
  done
  echo timed-out
}

# The runner builds the target embedded in the immutable bundle. Asking the
# controller to verify a different target must fail deterministically.
failed_response=$(create_build "${SERVICE_TARGET}-expected-failure")
failed_job_id=$(printf '%s' "${failed_response}" | jq -r '.id')
failed_status=$(wait_for_terminal "${failed_job_id}")
if [[ "${failed_status}" != "failed" ]]; then
  echo "error: immutable-source failure fixture ended with ${failed_status}" >&2
  exit 1
fi
failed_repository=${SERVICE_PULL_TARGET#*/}
failed_repository=${failed_repository%:*}
failed_tag=${SERVICE_PULL_TARGET##*:}-expected-failure
for unexpected_tag in "${failed_tag}" "${failed_tag}_nydus_v3"; do
  if curl -fsS "http://${REGISTRY_HOST}/v2/${failed_repository}/manifests/${unexpected_tag}" >/dev/null 2>&1; then
    echo "error: target-contract failure pushed an undeclared image: ${unexpected_tag}" >&2
    exit 1
  fi
done

# A caller retries with the same immutable source URI and digest. Kova owns no
# recovery state; the new request independently produces and verifies the OCI image.
create_response=$(create_build "${SERVICE_TARGET}")
job_id=$(printf '%s' "${create_response}" | jq -r '.id')
status=$(wait_for_terminal "${job_id}")
if [[ "${status}" != "succeeded" ]]; then
  echo "error: immutable-source retry ended with ${status}" >&2
  curl -fsS "${auth_header[@]}" "${BASE}/v1/builds/${job_id}" >&2 || true
  exit 1
fi

results=$(curl -fsS "${auth_header[@]}" "${BASE}/v1/builds/${job_id}/results")
printf '%s' "${results}" | jq -e --arg image "${SERVICE_TARGET}" --arg digest "${source_digest}" --arg platform "${KOVA_PLATFORM}" \
  '.source_digest == $digest and (.outputs | length) == 2 and
   ([.outputs[].format] | sort) == ["nydus", "oci"] and
   (.outputs | all(. as $output |
     $output.platform == $platform and
     ($output.manifest_digest | startswith("sha256:")) and
     ($output.immutable_ref | endswith("@" + $output.manifest_digest)))) and
   (.outputs | any(.format == "oci" and .image == $image)) and
   (.outputs | any(.format == "nydus" and .image == ($image + "_nydus_v3")))' >/dev/null
kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" -n "${NAMESPACE}" get kovabuild "${job_id}" \
  -o jsonpath='{.status.outputs[0].manifestDigest}' | grep -E '^sha256:[a-f0-9]{64}$' >/dev/null

terminal_logs_status=$(curl -sS -o /dev/null -w '%{http_code}' "${auth_header[@]}" \
  "${BASE}/v1/builds/${job_id}/logs?tail_lines=100")
if [[ "${terminal_logs_status}" != "410" ]]; then
  echo "error: terminal logs must not be retained by Kova; HTTP ${terminal_logs_status}" >&2
  exit 1
fi

docker pull "${SERVICE_PULL_TARGET}"

cleanup_deadline=$((SECONDS + 120))
while (( SECONDS < cleanup_deadline )); do
  remaining=0
  for completed_job_id in "${failed_job_id}" "${job_id}"; do
    if kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" -n "${NAMESPACE}" \
      get kovabuild "${completed_job_id}" >/dev/null 2>&1; then
      remaining=1
    fi
  done
  [[ "${remaining}" == "0" ]] && break
  sleep 5
done
for completed_job_id in "${failed_job_id}" "${job_id}"; do
  if kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" -n "${NAMESPACE}" \
    get kovabuild "${completed_job_id}" >/dev/null 2>&1; then
    echo "error: KovaBuild ${completed_job_id} was not removed after TTL" >&2
    exit 1
  fi
done

if [[ -n "${baseline_revision}" ]]; then
  helm rollback "${RELEASE_NAME}" "${baseline_revision}" \
    --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" --namespace "${NAMESPACE}" \
    --wait --timeout 180s
  kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" -n "${NAMESPACE}" \
    rollout status "deployment/${RELEASE_NAME}" --timeout=180s
fi
