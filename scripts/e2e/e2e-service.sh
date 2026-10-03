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
SERVICE_RUNNER_NAMESPACE=${SERVICE_RUNNER_NAMESPACE:-kova-service-jobs-$(date -u +%Y%m%d%H%M%S)-$$}
SERVICE_RECEIPT_NAMESPACE=${SERVICE_RECEIPT_NAMESPACE:-kova-service-receipts-$(date -u +%Y%m%d%H%M%S)-$$}
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
SERVICE_REPLICAS=${SERVICE_REPLICAS:-1}
SERVICE_MAX_ACTIVE_JOBS=${SERVICE_MAX_ACTIVE_JOBS:-20}
SERVICE_MAX_ACTIVE_JOBS_PER_REQUESTER=${SERVICE_MAX_ACTIVE_JOBS_PER_REQUESTER:-4}
SERVICE_MAX_QUEUED_JOBS=${SERVICE_MAX_QUEUED_JOBS:-1000}
SERVICE_MAX_QUEUED_JOBS_PER_REQUESTER=${SERVICE_MAX_QUEUED_JOBS_PER_REQUESTER:-100}
SERVICE_WORKER_SLOTS=${SERVICE_WORKER_SLOTS:-20}
# Dedicated local-fixture capacity identity; an environment managing shared
# workers must supply its externally assigned identity instead.
SERVICE_WORKER_POOL_ID=${SERVICE_WORKER_POOL_ID:-kind-fixture/${SERVICE_RUNNER_NAMESPACE}}
KOVA_CHART=${KOVA_CHART:-${ROOT}/charts/kova}
KOVA_VALUES=${KOVA_VALUES:-${ROOT}/deploy/kind-values.yaml}
BASELINE_CHART=${BASELINE_CHART:-}
BASELINE_CONTROLLER_IMAGE=${BASELINE_CONTROLLER_IMAGE:-}
BASELINE_RUNNER_IMAGE=${BASELINE_RUNNER_IMAGE:-}
BASELINE_WORKER_IMAGE=${BASELINE_WORKER_IMAGE:-}
REQUIRE_LEGACY_CRD=${REQUIRE_LEGACY_CRD:-false}
RESULT_JSONL=${RESULT_JSONL:-}
KOVA_PLATFORM=$(kova_platform)

if [[ "${KIND_KUBECONFIG}" == /* ]]; then
  kind_kubeconfig=${KIND_KUBECONFIG}
else
  kind_kubeconfig=${ROOT}/${KIND_KUBECONFIG}
fi

case ${REQUIRE_LEGACY_CRD} in
  true|false) ;;
  *) echo "error: REQUIRE_LEGACY_CRD must be true or false" >&2; exit 2 ;;
esac
if [[ "${REQUIRE_LEGACY_CRD}" == true && -z "${BASELINE_CHART}" ]]; then
  echo 'error: REQUIRE_LEGACY_CRD requires BASELINE_CHART' >&2
  exit 2
fi
if [[ -n "${BASELINE_CHART}" && "${SERVICE_RUNNER_NAMESPACE}" == "${NAMESPACE}" ]]; then
  echo 'error: baseline Service migration requires a fresh runner namespace' >&2
  exit 2
fi
if [[ "${SERVICE_RUNNER_NAMESPACE}" == "${NAMESPACE}" ]]; then
  echo 'error: Service E2E requires a separate, never-used runner namespace' >&2
  exit 2
fi
if [[ "${SERVICE_RECEIPT_NAMESPACE}" == "${NAMESPACE}" || "${SERVICE_RECEIPT_NAMESPACE}" == "${SERVICE_RUNNER_NAMESPACE}" ]]; then
  echo 'error: Service E2E requires a separate, never-used receipt namespace' >&2
  exit 2
fi

require_cmd curl
require_cmd docker
require_cmd helm
require_cmd jq
require_cmd kubectl
require_cmd python3

# Rendering is read-only. Reject an environment overlay that would redirect
# the Service away from the namespace whose external receipt we will create.
service_render=$(helm template "${RELEASE_NAME}" "${KOVA_CHART}" --namespace "${NAMESPACE}" \
  -f "${KOVA_VALUES}" -f "${ROOT}/scripts/chart/genesis-test-values.yaml" \
  --set serviceDaemon.enabled=true --show-only templates/service-daemon-deployment.yaml)
if ! awk -v expected="${NAMESPACE}" '
  /^metadata:$/ { metadata = 1; next }
  metadata && /^[^ ]/ { metadata = 0 }
  metadata && /^  namespace:/ { if ($2 != expected) exit 1; found++ }
  END { if (found != 1) exit 1 }
' <<<"${service_render}"; then
  echo 'error: Service values redirect the deployment outside the selected namespace' >&2
  exit 2
fi

# An existing live Service is not a fresh-install fixture. Never delete all
# old CRs/runners or disable their controller just to make a new test fit.
if [[ -z "${BASELINE_CHART}" && -f "${kind_kubeconfig}" ]]; then
  old_service=$(kubectl --kubeconfig "${kind_kubeconfig}" -n "${NAMESPACE}" \
    get deployment "${RELEASE_NAME}-service" --ignore-not-found -o json)
  if [[ -n "${old_service}" ]] && ! jq -e '.spec.replicas == 0' <<<"${old_service}" >/dev/null; then
    echo 'error: existing Service must be externally stopped and drained before a fresh E2E install' >&2
    exit 2
  fi
  if [[ -n "${old_service}" ]]; then
    old_runner_namespace=$(jq -er '
      [.spec.template.spec.containers[].args[]? | select(startswith("--namespace="))] |
      if length == 1 then .[0] | ltrimstr("--namespace=") else error("ambiguous runner namespace") end
    ' <<<"${old_service}")
    KUBECONFIG="${kind_kubeconfig}" NAMESPACE="${NAMESPACE}" \
      RUNNER_NAMESPACE="${old_runner_namespace}" RELEASE_NAME="${RELEASE_NAME}" \
      "${ROOT}/scripts/deployment/verify-kovabuild-drained.sh"
  fi
fi

# Opt-in receipts are allocated before any Kind/image side effects. The base
# names a private .work/ location; each invocation gets a distinct file.
receipt_file=""
receipt_run_id=""
if [[ -n "${RESULT_JSONL}" ]]; then
  require_cmd git
  require_cmd python3
  receipt_run_id=$(python3 -c 'import datetime,secrets; print("service-e2e-"+datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dt%H%M%Sz")+"-"+secrets.token_hex(4))')
  receipt_file=$(KOVA_SERVICE_E2E_RECEIPT_SECRET="${SERVICE_AUTH_TOKEN}" \
    python3 "${ROOT}/scripts/e2e/service-receipts.py" init \
      --root "${ROOT}" --base "${RESULT_JSONL}" --run-id "${receipt_run_id}" \
      --revision "$(git -C "${ROOT}" rev-parse HEAD)" --cluster "${KIND_CLUSTER:-kova-local}" \
      --namespace "${NAMESPACE}" --runner-namespace "${SERVICE_RUNNER_NAMESPACE}")
  echo "Service E2E receipts: ${receipt_file}" >&2
fi

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
    KIND_VALUES="${BASELINE_VALUES:-}" \
    WORKER_PLATFORM='' \
    CONTROLLER_IMAGE=${BASELINE_CONTROLLER_IMAGE} \
    RUNNER_IMAGE=${BASELINE_RUNNER_IMAGE} \
    WORKER_IMAGE=${BASELINE_WORKER_IMAGE} \
    KIND_LOAD_IMAGES=false \
    HELM_LEGACY_IMAGE_VALUES=true \
    VERIFY_RETRY_CRD_SCHEMA=false \
    "${ROOT}/scripts/kind/deploy-kind.sh"
  kubectl --kubeconfig "${kind_kubeconfig}" -n "${NAMESPACE}" \
    scale "deployment/${RELEASE_NAME}-service" --replicas=0 >/dev/null
  drain_deadline=$((SECONDS + 120))
  until KUBECONFIG="${kind_kubeconfig}" NAMESPACE="${NAMESPACE}" RELEASE_NAME="${RELEASE_NAME}" \
    "${ROOT}/scripts/deployment/verify-kovabuild-drained.sh"; do
    if (( SECONDS >= drain_deadline )); then
      echo 'error: old Service did not drain after scale-down' >&2
      exit 1
    fi
    sleep 2
  done
  if [[ "${REQUIRE_LEGACY_CRD}" == true ]]; then
    KUBECONFIG="${kind_kubeconfig}" \
      "${ROOT}/scripts/deployment/verify-kovabuild-crd.sh" --expect-legacy
    KUBECONFIG="${kind_kubeconfig}" \
      "${ROOT}/scripts/deployment/probe-kovabuild-status.sh" --expect-pruned
    KUBECONFIG="${kind_kubeconfig}" NAMESPACE="${NAMESPACE}" RELEASE_NAME="${RELEASE_NAME}" \
      "${ROOT}/scripts/deployment/verify-kovabuild-drained.sh"
  fi
  baseline_revision=$(helm history "${RELEASE_NAME}" \
    --kubeconfig "${kind_kubeconfig}" \
    --namespace "${NAMESPACE}" -o json | jq -r 'last.revision')
  helm show crds "${KOVA_CHART}" | \
    kubectl --kubeconfig "${kind_kubeconfig}" apply -f -
  KUBECONFIG="${kind_kubeconfig}" \
    "${ROOT}/scripts/deployment/verify-kovabuild-crd.sh"
  KUBECONFIG="${kind_kubeconfig}" \
    "${ROOT}/scripts/deployment/probe-kovabuild-status.sh" --expect-persisted
else
  "${ROOT}/scripts/kind/deploy-kind.sh"
fi

legacy_upgrade_probe=legacy-starting-upgrade-probe
legacy_upgrade_pod=kova-job-${legacy_upgrade_probe}
if [[ -n "${BASELINE_CHART}" ]]; then
  # The production drain gate must pass immediately before upgrading. The
  # optional legacy-CRD fixture below then injects an intentionally unsafe
  # old build to prove the new namespace does not touch it.
  KUBECONFIG="${kind_kubeconfig}" NAMESPACE="${NAMESPACE}" RELEASE_NAME="${RELEASE_NAME}" \
    "${ROOT}/scripts/deployment/verify-kovabuild-drained.sh"
fi
if [[ "${REQUIRE_LEGACY_CRD}" == true ]]; then
  # Inject a late old Starting runner after the production drain gate passes.
  # The new controller must not touch its old namespace if an operator bypasses
  # that gate or a late old request reaches the API server.
  KUBECONFIG="${kind_kubeconfig}" NAMESPACE="${NAMESPACE}" RELEASE_NAME="${RELEASE_NAME}" \
    "${ROOT}/scripts/deployment/verify-kovabuild-drained.sh"
  jq -n --arg namespace "${NAMESPACE}" --arg name "${legacy_upgrade_probe}" \
    --arg target "${SERVICE_TARGET}" --arg platform "${KOVA_PLATFORM}" \
    '{apiVersion:"kova.cofy.dev/v1alpha1",kind:"KovaBuild",metadata:{name:$name,namespace:$namespace},
      spec:{requester:{username:"migration-e2e"},targets:[{target:$target,platform:$platform}],
        source:{uri:"oci://registry.invalid/source@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          digest:"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
        build:{format:"oci",concurrency:1}}}' | \
    kubectl --kubeconfig "${kind_kubeconfig}" create -f - >/dev/null
  jq -n --arg namespace "${NAMESPACE}" --arg name "${legacy_upgrade_pod}" \
    --arg image "${BASELINE_RUNNER_IMAGE}" \
    '{apiVersion:"v1",kind:"Pod",metadata:{name:$name,namespace:$namespace,
        labels:{"app.kubernetes.io/name":"kova-runner"}},
      spec:{restartPolicy:"Never",securityContext:{runAsNonRoot:true,runAsUser:65532,runAsGroup:65532},
        containers:[{name:"runner",image:$image,imagePullPolicy:"IfNotPresent",command:["kovad","daemon"],
          readinessProbe:{exec:{command:["/usr/bin/test","-S","/tmp/kova.sock"]},periodSeconds:1,timeoutSeconds:1}}]}}' | \
    kubectl --kubeconfig "${kind_kubeconfig}" create -f - >/dev/null
  kubectl --kubeconfig "${kind_kubeconfig}" -n "${NAMESPACE}" \
    wait "pod/${legacy_upgrade_pod}" --for=condition=Ready --timeout=120s
  legacy_status=$(kubectl --kubeconfig "${kind_kubeconfig}" -n "${NAMESPACE}" \
    exec "${legacy_upgrade_pod}" -- kovad transport --method GET --path /api/v1/build/status)
  if ! jq -e '.status == "idle" and .capabilities == null' <<<"${legacy_status}" >/dev/null; then
    echo "error: old runner is not idle without protocol capability: ${legacy_status}" >&2
    exit 1
  fi
  kubectl --kubeconfig "${kind_kubeconfig}" -n "${NAMESPACE}" \
    patch kovabuild "${legacy_upgrade_probe}" --type=merge --subresource=status \
    -p "{\"status\":{\"phase\":\"Starting\",\"runnerPodName\":\"${legacy_upgrade_pod}\",\"allocatedConcurrency\":1}}" >/dev/null
  if gate_output=$(KUBECONFIG="${kind_kubeconfig}" NAMESPACE="${NAMESPACE}" RELEASE_NAME="${RELEASE_NAME}" \
    "${ROOT}/scripts/deployment/verify-kovabuild-drained.sh" 2>&1); then
    echo 'error: quiescence gate accepted an old Starting runner' >&2
    exit 1
  fi
  if [[ "${gate_output}" != *"${legacy_upgrade_probe}:Starting"* || "${gate_output}" != *"${legacy_upgrade_pod}"* ]]; then
    echo "error: quiescence gate did not identify the old Starting runner: ${gate_output}" >&2
    exit 1
  fi
fi

# Resolve only the already-published local fixture tag. Remote callers must
# supply their exact immutable manifest reference; this never pushes an image.
RUNNER_IMAGE=$(python3 "${ROOT}/scripts/kind/resolve-runner-image.py" "${RUNNER_IMAGE}")
genesis_context=$(kubectl --kubeconfig "${kind_kubeconfig}" config current-context)
genesis_system_uid=$(kubectl --kubeconfig "${kind_kubeconfig}" get namespace kube-system -o jsonpath='{.metadata.uid}')
mkdir -p "${ROOT}/.work/service-genesis"
genesis_values=$(python3 "${ROOT}/scripts/kind/create-service-genesis.py" \
  --kubeconfig "${kind_kubeconfig}" --context "${genesis_context}" \
  --kube-system-uid "${genesis_system_uid}" \
  --kova-cli "${KOVA_CLI}" --release "${RELEASE_NAME}" \
  --service-namespace "${NAMESPACE}" --runner-namespace "${SERVICE_RUNNER_NAMESPACE}" \
  --receipt-namespace "${SERVICE_RECEIPT_NAMESPACE}" \
  --worker-pool-id "${SERVICE_WORKER_POOL_ID}" --runner-image "${RUNNER_IMAGE}" \
  --acknowledge "${genesis_context}/${NAMESPACE}/${SERVICE_RUNNER_NAMESPACE}/${SERVICE_RECEIPT_NAMESPACE}" \
  --output-directory "${ROOT}/.work/service-genesis/${SERVICE_RUNNER_NAMESPACE}" \
  --max-active-jobs "${SERVICE_MAX_ACTIVE_JOBS}" \
  --max-active-jobs-per-requester "${SERVICE_MAX_ACTIVE_JOBS_PER_REQUESTER}" \
  --worker-slots "${SERVICE_WORKER_SLOTS}" \
  --max-queued-jobs "${SERVICE_MAX_QUEUED_JOBS}" \
  --max-queued-jobs-per-requester "${SERVICE_MAX_QUEUED_JOBS_PER_REQUESTER}")
echo "Genesis installation evidence: $(dirname "${genesis_values}")" >&2

sync_service_auth_secret() (
  local token_file
  umask 077
  token_file=$(mktemp "${TMPDIR:-/tmp}/kova-service-e2e-token.XXXXXX")
  trap 'rm -f "${token_file}"' EXIT
  printf '%s' "${SERVICE_AUTH_TOKEN}" >"${token_file}"
  kubectl --kubeconfig "${kind_kubeconfig}" -n "${NAMESPACE}" \
    create secret generic "${SERVICE_AUTH_SECRET}" --from-file=token="${token_file}" \
    --dry-run=client -o yaml | \
    kubectl --kubeconfig "${kind_kubeconfig}" apply -f - >/dev/null
)
sync_service_auth_secret

service_replicas=${SERVICE_REPLICAS}
if [[ -n "${BASELINE_CHART}" ]]; then
  # Keep the new controller stopped through Helm --wait. No old and new
  # Service processes may overlap across this protocol migration.
  service_replicas=0
fi
helm_image_args=()
append_helm_role_image_args controller "${CONTROLLER_IMAGE}"
append_helm_role_image_args runner "${RUNNER_IMAGE}"
append_helm_role_image_args worker "${WORKER_IMAGE}"
helm upgrade --install "${RELEASE_NAME}" "${KOVA_CHART}" \
  --kubeconfig "${kind_kubeconfig}" \
  --namespace "${NAMESPACE}" --create-namespace --wait --timeout 180s \
  -f "${KOVA_VALUES}" \
  -f "${genesis_values}" \
  "${helm_image_args[@]}" \
  --set-string "worker.platform=${KOVA_PLATFORM}" \
  --set serviceDaemon.enabled=true \
  --set "serviceDaemon.replicas=${service_replicas}" \
  --set-string "serviceDaemon.runnerNamespace=${SERVICE_RUNNER_NAMESPACE}" \
  --set-string "serviceDaemon.admissionGenesis.recoveryReceiptNamespace=${SERVICE_RECEIPT_NAMESPACE}" \
  --set "serviceDaemon.maxActiveJobs=${SERVICE_MAX_ACTIVE_JOBS}" \
  --set "serviceDaemon.maxActiveJobsPerRequester=${SERVICE_MAX_ACTIVE_JOBS_PER_REQUESTER}" \
  --set "serviceDaemon.maxQueuedJobs=${SERVICE_MAX_QUEUED_JOBS}" \
  --set "serviceDaemon.maxQueuedJobsPerRequester=${SERVICE_MAX_QUEUED_JOBS_PER_REQUESTER}" \
  --set "serviceDaemon.workerSlots=${SERVICE_WORKER_SLOTS}" \
  --set-string serviceDaemon.authentication.mode=static \
  --set-string "serviceDaemon.authentication.staticTokenSecret.name=${SERVICE_AUTH_SECRET}" \
  --set-string serviceDaemon.authentication.staticTokenSecret.key=token \
  --set-string serviceDaemon.authentication.staticPrincipal=kova:e2e \
  --set "serviceDaemon.jobTTL=${SERVICE_JOB_TTL}" \
  --set serviceDaemon.runnerImagePullSecret= \
  --set-string "serviceDaemon.registryPlainHTTP[0]=${CLUSTER_REGISTRY}"

kubectl --kubeconfig "${kind_kubeconfig}" -n "${SERVICE_RUNNER_NAMESPACE}" \
  create rolebinding "${RELEASE_NAME}-e2e-submitter" \
  --role="${RELEASE_NAME}-service-submitter" --user=kova:e2e \
  --dry-run=client -o yaml | \
  kubectl --kubeconfig "${kind_kubeconfig}" apply -f - >/dev/null

if [[ -n "${BASELINE_CHART}" ]]; then
  kubectl --kubeconfig "${kind_kubeconfig}" -n "${NAMESPACE}" \
    scale "deployment/${RELEASE_NAME}-service" --replicas="${SERVICE_REPLICAS}" >/dev/null
  kubectl --kubeconfig "${kind_kubeconfig}" -n "${NAMESPACE}" \
    rollout status "deployment/${RELEASE_NAME}-service" --timeout=180s
fi
if [[ "${REQUIRE_LEGACY_CRD}" == true ]]; then
  # The upgraded Service watches only the fresh runner namespace. A late old
  # Starting CR and idle old runner must remain untouched in the old one.
  sleep 5
  observed=$(kubectl --kubeconfig "${kind_kubeconfig}" -n "${NAMESPACE}" \
    get kovabuild "${legacy_upgrade_probe}" -o json)
  if ! jq -e '.status.phase == "Starting" and .status.runnerPodName == "kova-job-legacy-starting-upgrade-probe"' \
    <<<"${observed}" >/dev/null; then
    echo "error: upgraded Service touched an old-namespace Starting build: ${observed}" >&2
    exit 1
  fi
  legacy_status=$(kubectl --kubeconfig "${kind_kubeconfig}" -n "${NAMESPACE}" \
    exec "${legacy_upgrade_pod}" -- kovad transport --method GET --path /api/v1/build/status)
  if ! jq -e '.status == "idle"' <<<"${legacy_status}" >/dev/null; then
    echo "error: upgraded Service submitted to the old-namespace runner: ${legacy_status}" >&2
    exit 1
  fi
  kubectl --kubeconfig "${kind_kubeconfig}" -n "${NAMESPACE}" \
    delete kovabuild "${legacy_upgrade_probe}" --wait=true --timeout=60s >/dev/null
  kubectl --kubeconfig "${kind_kubeconfig}" -n "${NAMESPACE}" \
    delete pod "${legacy_upgrade_pod}" --wait=true --timeout=60s >/dev/null
fi
kubectl --kubeconfig "${kind_kubeconfig}" -n "${NAMESPACE}" \
  rollout status "deployment/${RELEASE_NAME}-service" --timeout=180s

too_many_targets=$(jq -n --arg registry "${CLUSTER_REGISTRY}" --arg platform "${KOVA_PLATFORM}" \
  '[range(0;101) | {target: ($registry + "/kova-examples/limit-" + tostring + ":dev"), platform: $platform}]')
if jq -n --argjson targets "${too_many_targets}" --arg namespace "${SERVICE_RUNNER_NAMESPACE}" --arg uri \
  "oci://${CLUSTER_REGISTRY}/kova-sources/invalid@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" \
  '{apiVersion:"kova.cofy.dev/v1alpha1",kind:"KovaBuild",metadata:{name:"too-many-targets",namespace:$namespace},spec:{requester:{username:"e2e"},targets:$targets,source:{uri:$uri,digest:"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},build:{format:"oci",concurrency:1}}}' | \
  kubectl --kubeconfig "${kind_kubeconfig}" create --dry-run=server -f - >/dev/null 2>&1; then
  echo "error: Kubernetes API accepted 101 logical targets" >&2
  exit 1
fi

assert_kubernetes_rejects_targets() {
  local label=$1 targets=$2
  if jq -n --argjson targets "${targets}" --arg namespace "${SERVICE_RUNNER_NAMESPACE}" --arg uri \
    "oci://${CLUSTER_REGISTRY}/kova-sources/invalid@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" \
    '{apiVersion:"kova.cofy.dev/v1alpha1",kind:"KovaBuild",metadata:{generateName:"invalid-targets-",namespace:$namespace},spec:{requester:{username:"e2e"},targets:$targets,source:{uri:$uri,digest:"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},build:{format:"oci",concurrency:1}}}' | \
    kubectl --kubeconfig "${kind_kubeconfig}" create --dry-run=server -f - >/dev/null 2>&1; then
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
kubectl --kubeconfig "${kind_kubeconfig}" -n "${NAMESPACE}" \
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
  local target=$1 key=$2
  jq -n --arg source_uri "${source_uri}" --arg source_digest "${source_digest}" \
    --arg target "${target}" --arg platform "${KOVA_PLATFORM}" --arg registry "${CLUSTER_REGISTRY}" --arg key "${key}" \
    '{source_uri:$source_uri,source_digest:$source_digest,targets:[{target:$target,platform:$platform}],format:"both",concurrency:1,timeout:600,fail_fast:true,verbose:true,variables:["KOVA_IMAGE_REGISTRY="+$registry]} + (if $key == "" then {} else {idempotency_key:$key} end)' | \
    curl -fsS -X POST "${auth_header[@]}" -H 'Content-Type: application/json' \
      --data-binary @- "${BASE}/v1/builds"
}

record_receipt_event() {
  local event=$1 stage=$2 target=$3 key=$4 job_id=${5:-}
  [[ -n "${receipt_file}" ]] || return 0
  KOVA_SERVICE_E2E_RECEIPT_SECRET="${SERVICE_AUTH_TOKEN}" \
    python3 "${ROOT}/scripts/e2e/service-receipts.py" event \
      --root "${ROOT}" --path "${receipt_file}" --run-id "${receipt_run_id}" \
      --event "${event}" --stage "${stage}" --target "${target}" --key "${key}" \
      --source-uri "${source_uri}" --source-digest "${source_digest}" --job-id "${job_id}"
}

submit_build() {
  local stage=$1 target=$2 key="" response submitted_job_id
  if [[ -n "${receipt_file}" ]]; then
    key="${receipt_run_id}-${stage}"
    if ! record_receipt_event submit_attempt "${stage}" "${target}" "${key}" </dev/null; then
      return 1
    fi
  fi
  if ! response=$(create_build "${target}" "${key}"); then
    # A failed POST can still have created a build. Keep the attempt and stop.
    [[ -z "${receipt_file}" ]] || record_receipt_event submit_unconfirmed "${stage}" "${target}" "${key}" </dev/null
    return 1
  fi
  if [[ -n "${receipt_file}" ]]; then
    if ! submitted_job_id=$(printf '%s' "${response}" | record_receipt_event submitted "${stage}" "${target}" "${key}"); then
      record_receipt_event submit_unconfirmed "${stage}" "${target}" "${key}" </dev/null
      return 1
    fi
  else
    submitted_job_id=$(printf '%s' "${response}" | jq -r '.id')
  fi
  printf '%s\n' "${submitted_job_id}"
}

wait_for_terminal() {
  local job_id=$1 deadline status response
  deadline=$((SECONDS + 900))
  while (( SECONDS < deadline )); do
    if ! response=$(curl -fsS "${auth_header[@]}" "${BASE}/v1/builds/${job_id}"); then
      return 1
    fi
    if ! status=$(printf '%s' "${response}" | jq -r '.status'); then
      return 1
    fi
    case "${status}" in
      succeeded|failed|cancelled)
        printf '%s\n' "${response}"
        return
        ;;
    esac
    sleep 5
  done
  return 124
}

# The runner builds the target embedded in the immutable bundle. Asking the
# controller to verify a different target must fail deterministically.
failed_target="${SERVICE_TARGET}-expected-failure"
failed_job_id=$(submit_build negative "${failed_target}")
if ! failed_terminal=$(wait_for_terminal "${failed_job_id}"); then
  [[ -z "${receipt_file}" ]] || record_receipt_event observation_incomplete negative "${failed_target}" "${receipt_run_id}-negative" "${failed_job_id}" </dev/null
  echo 'error: immutable-source failure fixture ended with timed-out or unreadable status' >&2
  exit 1
fi
failed_status=$(printf '%s' "${failed_terminal}" | jq -r '.status')
if [[ -n "${receipt_file}" ]]; then
  printf '%s' "${failed_terminal}" | record_receipt_event terminal negative "${failed_target}" "${receipt_run_id}-negative" "${failed_job_id}"
fi
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
job_id=$(submit_build positive "${SERVICE_TARGET}")
if ! positive_terminal=$(wait_for_terminal "${job_id}"); then
  [[ -z "${receipt_file}" ]] || record_receipt_event observation_incomplete positive "${SERVICE_TARGET}" "${receipt_run_id}-positive" "${job_id}" </dev/null
  echo 'error: immutable-source retry ended with timed-out or unreadable status' >&2
  exit 1
fi
status=$(printf '%s' "${positive_terminal}" | jq -r '.status')
if [[ -n "${receipt_file}" ]]; then
  printf '%s' "${positive_terminal}" | record_receipt_event terminal positive "${SERVICE_TARGET}" "${receipt_run_id}-positive" "${job_id}"
fi
if [[ "${status}" != "succeeded" ]]; then
  echo "error: immutable-source retry ended with ${status}" >&2
  curl -fsS "${auth_header[@]}" "${BASE}/v1/builds/${job_id}" >&2 || true
  exit 1
fi

results=$(curl -fsS "${auth_header[@]}" "${BASE}/v1/builds/${job_id}/results")
if [[ -n "${receipt_file}" ]]; then
  printf '%s' "${results}" | record_receipt_event results positive "${SERVICE_TARGET}" "${receipt_run_id}-positive" "${job_id}"
fi
printf '%s' "${results}" | jq -e --arg image "${SERVICE_TARGET}" --arg digest "${source_digest}" --arg platform "${KOVA_PLATFORM}" \
  '.source_digest == $digest and (.outputs | length) == 2 and
   ([.outputs[].format] | sort) == ["nydus", "oci"] and
   (.outputs | all(. as $output |
     $output.platform == $platform and
     ($output.manifest_digest | startswith("sha256:")) and
     ($output.immutable_ref | endswith("@" + $output.manifest_digest)))) and
   (.outputs | any(.format == "oci" and .image == $image)) and
   (.outputs | any(.format == "nydus" and .image == ($image + "_nydus_v3")))' >/dev/null
printf '%s' "${results}" | jq -r \
  '"Service E2E: negative contract fixture failed as expected; positive build succeeded; source=" + .source_digest +
   "; outputs=" + ([.outputs[] | .format + "@" + .manifest_digest] | join(","))'
kubectl --kubeconfig "${kind_kubeconfig}" -n "${SERVICE_RUNNER_NAMESPACE}" get kovabuild "${job_id}" \
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
    if kubectl --kubeconfig "${kind_kubeconfig}" -n "${SERVICE_RUNNER_NAMESPACE}" \
      get kovabuild "${completed_job_id}" >/dev/null 2>&1; then
      remaining=1
    fi
  done
  [[ "${remaining}" == "0" ]] && break
  sleep 5
done
for completed_job_id in "${failed_job_id}" "${job_id}"; do
  if kubectl --kubeconfig "${kind_kubeconfig}" -n "${SERVICE_RUNNER_NAMESPACE}" \
    get kovabuild "${completed_job_id}" >/dev/null 2>&1; then
    echo "error: KovaBuild ${completed_job_id} was not removed after TTL" >&2
    exit 1
  fi
done

if [[ -n "${baseline_revision}" ]]; then
  kubectl --kubeconfig "${kind_kubeconfig}" -n "${NAMESPACE}" \
    scale "deployment/${RELEASE_NAME}-service" --replicas=0 >/dev/null
  drained_namespace() {
    local runner_namespace=$1
    KUBECONFIG="${kind_kubeconfig}" NAMESPACE="${NAMESPACE}" \
      RUNNER_NAMESPACE="${runner_namespace}" RELEASE_NAME="${RELEASE_NAME}" \
      "${ROOT}/scripts/deployment/verify-kovabuild-drained.sh"
  }
  rollback_deadline=$((SECONDS + 120))
  until drained_namespace "${SERVICE_RUNNER_NAMESPACE}" && drained_namespace "${NAMESPACE}"; do
    if (( SECONDS >= rollback_deadline )); then
      echo 'error: upgraded Service did not drain before baseline rollback' >&2
      exit 1
    fi
    sleep 2
  done
  helm rollback "${RELEASE_NAME}" "${baseline_revision}" \
    --kubeconfig "${kind_kubeconfig}" --namespace "${NAMESPACE}" \
    --wait --timeout 180s
  kubectl --kubeconfig "${kind_kubeconfig}" -n "${NAMESPACE}" \
    rollout status "deployment/${RELEASE_NAME}" --timeout=180s
fi

if [[ -n "${receipt_file}" ]]; then
  record_receipt_event verified positive "${SERVICE_TARGET}" "${receipt_run_id}-positive" "${job_id}" </dev/null
fi
