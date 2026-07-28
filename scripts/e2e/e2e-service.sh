#!/usr/bin/env bash

set -euo pipefail

source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

ROOT=$(repo_root)
CONTROLLER_IMAGE=${CONTROLLER_IMAGE:-localhost:5002/kova:controller-dev}
RUNNER_IMAGE=${RUNNER_IMAGE:-localhost:5002/kova:runner-dev}
WORKER_IMAGE=${WORKER_IMAGE:-localhost:5002/kova:worker-dev}
BUILDKIT_ADDR=${BUILDKIT_ADDR:-tcp://kova.kova.svc:9094}
KIND_KUBECONFIG=${KIND_KUBECONFIG:-.kind/kova-local.kubeconfig}
RELEASE_NAME=${RELEASE_NAME:-kova}
NAMESPACE=${NAMESPACE:-kova}
WORK_DIR=${WORK_DIR:-.work}
SOURCE_ZIP=${SOURCE_ZIP:-${WORK_DIR}/source-service.zip}
RESULT_JSONL=${RESULT_JSONL:-${WORK_DIR}/result-service.jsonl}
CLUSTER_REGISTRY=${CLUSTER_REGISTRY:-host.docker.internal:5002}
REGISTRY_HOST=${REGISTRY_HOST:-localhost:5002}
SERVICE_PORT=${SERVICE_PORT:-18080}
SERVICE_AUTH_TOKEN=${SERVICE_AUTH_TOKEN:-service-e2e-token}
SERVICE_AUTH_SECRET=${SERVICE_AUTH_SECRET:-kova-service-auth}
SERVICE_TARGET=${SERVICE_TARGET:-${CLUSTER_REGISTRY}/kova-examples/simple:dev}
SERVICE_PULL_TARGET=${SERVICE_PULL_TARGET:-${REGISTRY_HOST}/kova-examples/simple:dev}
E2E_SERVICE_BUILD_IMAGE=${E2E_SERVICE_BUILD_IMAGE:-true}
SERVICE_JOB_TTL=${SERVICE_JOB_TTL:-30s}
SOURCE_STORE_MOUNT=${SOURCE_STORE_MOUNT:-/var/lib/kova/sources}
SOURCE_PVC=${SOURCE_PVC:-kova-sources}

require_cmd curl
require_cmd docker
require_cmd helm
require_cmd jq
require_cmd kubectl
require_cmd zip

make -C "${ROOT}" kova
if [[ "${E2E_SERVICE_BUILD_IMAGE}" == "true" ]]; then
  make -C "${ROOT}" image
fi
"${ROOT}/scripts/kind/deploy-kind.sh"

kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" \
  apply -f "${ROOT}/charts/kova/crds/kova.cofy.dev_kovabuilds.yaml"

kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" \
  -n "${NAMESPACE}" delete kovabuild --all \
  --ignore-not-found || true

cleanup_deadline=$((SECONDS + 120))
while (( SECONDS < cleanup_deadline )); do
  remaining_builds=$(kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" \
    -n "${NAMESPACE}" get kovabuild --no-headers 2>/dev/null | wc -l | tr -d ' ')
  if [[ "${remaining_builds}" == "0" ]]; then
    break
  fi
  sleep 5
done

remaining_builds=$(kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" \
  -n "${NAMESPACE}" get kovabuild --no-headers 2>/dev/null | wc -l | tr -d ' ')
if [[ "${remaining_builds}" != "0" ]]; then
  while IFS= read -r build_resource; do
    [[ -z "${build_resource}" ]] && continue
    kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" \
      -n "${NAMESPACE}" patch "${build_resource}" \
      --type=merge -p '{"metadata":{"finalizers":[]}}' || true
  done < <(kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" \
    -n "${NAMESPACE}" get kovabuild -o name 2>/dev/null)
  kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" \
    -n "${NAMESPACE}" delete kovabuild --all \
    --ignore-not-found --wait=false || true
fi

kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" \
  -n "${NAMESPACE}" delete pod \
  -l 'app.kubernetes.io/name=kova-runner' \
  --ignore-not-found || true

if kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" \
  -n "${NAMESPACE}" get pvc "${SOURCE_PVC}" \
  -o jsonpath='{.metadata.deletionTimestamp}' 2>/dev/null | grep -q .; then
  kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" \
    -n "${NAMESPACE}" patch pvc "${SOURCE_PVC}" \
    --type=merge -p '{"metadata":{"finalizers":[]}}' || true
  kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" \
    -n "${NAMESPACE}" wait --for=delete "pvc/${SOURCE_PVC}" --timeout=120s || true
fi

kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" \
  -n "${NAMESPACE}" delete deployment "${RELEASE_NAME}-service" \
  --ignore-not-found

sync_service_auth_secret() (
  local token_file
  umask 077
  token_file=$(mktemp "${TMPDIR:-/tmp}/kova-service-e2e-token.XXXXXX")
  trap 'rm -f "${token_file}"' EXIT
  printf '%s' "${SERVICE_AUTH_TOKEN}" >"${token_file}"
  kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" \
    -n "${NAMESPACE}" create secret generic "${SERVICE_AUTH_SECRET}" \
    --from-file=token="${token_file}" \
    --dry-run=client -o yaml | \
    kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" apply -f - >/dev/null
)

sync_service_auth_secret

helm upgrade --install "${RELEASE_NAME}" "${ROOT}/charts/kova" \
  --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" \
  --namespace "${NAMESPACE}" \
  --create-namespace \
  --wait \
  --timeout 180s \
  -f "${ROOT}/deploy/kind-values.yaml" \
  --set-string "images.controller.repository=${CONTROLLER_IMAGE%:*}" \
  --set-string "images.controller.tag=${CONTROLLER_IMAGE##*:}" \
  --set-string "images.runner.repository=${RUNNER_IMAGE%:*}" \
  --set-string "images.runner.tag=${RUNNER_IMAGE##*:}" \
  --set-string "images.worker.repository=${WORKER_IMAGE%:*}" \
  --set-string "images.worker.tag=${WORKER_IMAGE##*:}" \
  --set "serviceDaemon.enabled=true" \
  --set-string "serviceDaemon.authentication.mode=static" \
  --set-string "serviceDaemon.authentication.staticTokenSecret.name=${SERVICE_AUTH_SECRET}" \
  --set-string "serviceDaemon.authentication.staticTokenSecret.key=token" \
  --set "serviceDaemon.jobTTL=${SERVICE_JOB_TTL}" \
  --set "serviceDaemon.runnerImagePullSecret=" \
  --set "artifactStore.filesystem.pvc.create=true" \
  --set "artifactStore.filesystem.pvc.accessModes[0]=ReadWriteOnce"

kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" \
  -n "${NAMESPACE}" rollout status deploy/"${RELEASE_NAME}-service" --timeout=180s

package_service_source() (
  local context_dir output
  context_dir=$(mktemp -d "${TMPDIR:-/tmp}/kova-service-context.XXXXXX")
  trap 'rm -rf "${context_dir}"' EXIT
  mkdir -p "${context_dir}/simple"
  cp "${ROOT}/examples/simple/Dockerfile" "${context_dir}/simple/Dockerfile"
  cp "${ROOT}/examples/simple/hello.txt" "${context_dir}/simple/hello.txt"
  jq -n --arg target "${SERVICE_TARGET}" '{target:$target}' \
    >"${context_dir}/simple/metadata.json"
  output="${ROOT}/${SOURCE_ZIP}"
  mkdir -p "$(dirname "${output}")"
  rm -f "${output}"
  (cd "${context_dir}" && zip -qr "${output}" simple)
)

package_service_source

kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" \
  -n "${NAMESPACE}" delete pod \
  -l 'app.kubernetes.io/name=kova-runner,kova.cofy.dev/job-id' \
  --ignore-not-found || true
kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" \
  -n "${NAMESPACE}" delete pod \
  -l 'app.kubernetes.io/name=kova-runner,kova.cofy.dev/build-id' \
  --ignore-not-found || true

port_forward_log=$(mktemp)
kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" \
  -n "${NAMESPACE}" port-forward "svc/${RELEASE_NAME}-service" "${SERVICE_PORT}:8080" \
  >"${port_forward_log}" 2>&1 &
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
unauthorized_status=$(curl -sS -o /dev/null -w '%{http_code}' \
  -H 'Authorization: Bearer wrong-token' "${BASE}/v1/builds") # gitleaks:allow
if [[ "${unauthorized_status}" != "401" ]]; then
  echo "error: service API accepted an invalid bearer token (HTTP ${unauthorized_status})" >&2
  exit 1
fi

create_response=$(curl -fsS -X POST \
  "${auth_header[@]}" \
  -F "file=@${ROOT}/${SOURCE_ZIP}" \
  -F "format=oci" \
  -F "target=${SERVICE_TARGET}" \
  -F "concurrency=1" \
  -F "timeout=600" \
  -F "fail-fast=true" \
  -F "verbose=true" \
  -F "var=KOVA_IMAGE_REGISTRY=${CLUSTER_REGISTRY}" \
  "${BASE}/v1/builds")

job_id=$(printf '%s' "${create_response}" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
if [[ -z "${job_id}" ]]; then
  echo "error: failed to parse job id from response: ${create_response}" >&2
  exit 1
fi

deadline=$((SECONDS + 900))
status=""
while (( SECONDS < deadline )); do
  job_response=$(curl -fsS "${auth_header[@]}" "${BASE}/v1/builds/${job_id}")
  status=$(printf '%s' "${job_response}" | sed -n 's/.*"status":"\([^"]*\)".*/\1/p')
  case "${status}" in
    succeeded)
      break
      ;;
    failed|cancelled)
      echo "error: service job ${job_id} finished with status ${status}" >&2
      printf '%s\n' "${job_response}" >&2
      curl -fsS "${auth_header[@]}" "${BASE}/v1/builds/${job_id}/logs?tail_lines=200" >&2 || true
      exit 1
      ;;
  esac
  sleep 5
done

if [[ "${status}" != "succeeded" ]]; then
  echo "error: timed out waiting for service job ${job_id}; last status ${status}" >&2
  curl -fsS "${auth_header[@]}" "${BASE}/v1/builds/${job_id}/logs?tail_lines=200" >&2 || true
  exit 1
fi

curl -fsS "${auth_header[@]}" "${BASE}/v1/builds/${job_id}/logs?tail_lines=100" >/dev/null
curl -fsS -X POST "${auth_header[@]}" "${BASE}/v1/builds/${job_id}/export?oci=true" > "${ROOT}/${RESULT_JSONL}"
grep -F "${SERVICE_TARGET}" "${ROOT}/${RESULT_JSONL}" >/dev/null

kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" \
  -n "${NAMESPACE}" get kovabuild "${job_id}" \
  -o jsonpath='{.status.phase}' | grep -F Succeeded >/dev/null

curl -fsS "${auth_header[@]}" "${BASE}/v1/builds" | grep -F "${job_id}" >/dev/null
docker pull "${SERVICE_PULL_TARGET}"

cleanup_deadline=$((SECONDS + 120))
while (( SECONDS < cleanup_deadline )); do
  if ! kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" \
    -n "${NAMESPACE}" get kovabuild "${job_id}" >/dev/null 2>&1; then
    break
  fi
  sleep 5
done

if kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" \
  -n "${NAMESPACE}" get kovabuild "${job_id}" >/dev/null 2>&1; then
  echo "error: KovaBuild ${job_id} was not removed after TTL" >&2
  exit 1
fi

if kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" \
  -n "${NAMESPACE}" get pod "kova-job-${job_id}" >/dev/null 2>&1; then
  echo "error: runner Pod kova-job-${job_id} was not removed after TTL" >&2
  exit 1
fi

service_pod=$(kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" \
  -n "${NAMESPACE}" get pod \
  -l 'app.kubernetes.io/component=service' \
  -o jsonpath='{.items[0].metadata.name}')
kubectl --kubeconfig "${ROOT}/${KIND_KUBECONFIG}" \
  -n "${NAMESPACE}" exec "${service_pod}" -- \
  test ! -e "${SOURCE_STORE_MOUNT}/builds/${job_id}"
