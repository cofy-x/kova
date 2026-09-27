#!/usr/bin/env bash

# Isolated old-chart -> current-CRD -> current-controller migration smoke.
set -euo pipefail
umask 077

source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

ROOT=$(repo_root)
BASELINE_VERSION=${BASELINE_VERSION:-v0.1.0-rc.9}
KEEP_KIND_CLUSTER=${KEEP_KIND_CLUSTER:-false}
KIND_CLUSTER=kova-crd-upgrade
KIND_KUBECONFIG=.kind/kova-crd-upgrade.kubeconfig
REGISTRY_NAME=kind-registry-crd-upgrade
REGISTRY_PORT=5003
REGISTRY_HOST=localhost:5003
CLUSTER_REGISTRY=kind-registry-crd-upgrade:5000
SERVICE_PORT=18081
KIND_CONFIG=deploy/crd-upgrade-kind-cluster.yaml
KIND_VALUES=deploy/crd-upgrade-kind-values.yaml
KOVA_VALUES=${ROOT}/deploy/crd-upgrade-kind-values.yaml
CONTROLLER_IMAGE=localhost:5003/kova:controller-crd-upgrade
RUNNER_IMAGE=localhost:5003/kova:runner-crd-upgrade
WORKER_IMAGE=localhost:5003/kova:worker-crd-upgrade
KOVA_CHART=${KOVA_CHART:-${ROOT}/charts/kova}
KOVA_CLI=${ROOT}/bin/kova
START_OBSERVABILITY=false
KIND_WORKERS=1

if [[ "${KOVA_CHART}" != /* ]]; then
  KOVA_CHART=${ROOT}/${KOVA_CHART}
fi
if [[ ! -e "${KOVA_CHART}" ]]; then
  echo "error: candidate chart ${KOVA_CHART} does not exist" >&2
  exit 2
fi

case ${KEEP_KIND_CLUSTER} in
  true|false) ;;
  *) echo 'error: KEEP_KIND_CLUSTER must be true or false' >&2; exit 2 ;;
esac
if [[ ! "${BASELINE_VERSION}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
  echo "error: invalid BASELINE_VERSION ${BASELINE_VERSION}" >&2
  exit 2
fi

require_cmd docker
require_cmd helm
require_cmd kubectl
require_kind
docker info >/dev/null

if kind get clusters | grep -qx "${KIND_CLUSTER}"; then
  echo "error: test-owned Kind cluster ${KIND_CLUSTER} already exists; refusing to reuse it" >&2
  exit 1
fi
if docker inspect "${REGISTRY_NAME}" >/dev/null 2>&1; then
  echo "error: test-owned registry ${REGISTRY_NAME} already exists; refusing to reuse it" >&2
  exit 1
fi
if [[ -e "${ROOT}/${KIND_KUBECONFIG}" ]]; then
  echo "error: test-owned kubeconfig ${ROOT}/${KIND_KUBECONFIG} already exists" >&2
  exit 1
fi
if command -v nc >/dev/null 2>&1; then
  if nc -z 127.0.0.1 "${REGISTRY_PORT}" || nc -z 127.0.0.1 "${SERVICE_PORT}"; then
    echo "error: port ${REGISTRY_PORT} or ${SERVICE_PORT} is already in use" >&2
    exit 1
  fi
fi

chart_dir=$(mktemp -d "${TMPDIR:-/tmp}/kova-crd-upgrade-chart.XXXXXX")
created_resources=false
creation_attempted=false
cleanup() {
  local status=$?
  trap - EXIT
  rm -r -- "${chart_dir}" || status=1
  if [[ "${created_resources}" == true ]]; then
    if [[ "${status}" == 0 && "${KEEP_KIND_CLUSTER}" == false ]]; then
      if kind delete cluster --name "${KIND_CLUSTER}"; then
        docker rm -f "${REGISTRY_NAME}" >/dev/null || status=1
        rm -f -- "${ROOT}/${KIND_KUBECONFIG}"
      else
        echo "error: failed to delete ${KIND_CLUSTER}; registry and kubeconfig were preserved" >&2
        status=1
      fi
    else
      echo "Preserved ${KIND_CLUSTER}, ${REGISTRY_NAME}, and ${ROOT}/${KIND_KUBECONFIG} for inspection." >&2
    fi
  elif [[ "${creation_attempted}" == true ]]; then
    echo "Cluster creation failed; inspect ${KIND_CLUSTER}, ${REGISTRY_NAME}, and ${ROOT}/${KIND_KUBECONFIG} for partial resources." >&2
  fi
  exit "${status}"
}
trap cleanup EXIT

helm pull oci://ghcr.io/cofy-x/charts/kova \
  --version "${BASELINE_VERSION#v}" --destination "${chart_dir}"
BASELINE_CHART=${chart_dir}/kova-${BASELINE_VERSION#v}.tgz
helm show chart "${BASELINE_CHART}" | grep -Fx "appVersion: ${BASELINE_VERSION}" >/dev/null
baseline_crds=$(helm show crds "${BASELINE_CHART}")
if [[ "${baseline_crds}" != *'name: kovabuilds.kova.cofy.dev'* || \
  "${baseline_crds}" == *'pollFailureSince:'* || "${baseline_crds}" == *'pollFailureCount:'* ]]; then
  echo "error: ${BASELINE_VERSION} chart does not have the required old KovaBuild CRD" >&2
  exit 1
fi

export KIND_CLUSTER KIND_KUBECONFIG REGISTRY_NAME REGISTRY_PORT REGISTRY_HOST CLUSTER_REGISTRY
export SERVICE_PORT KIND_CONFIG KIND_VALUES KOVA_VALUES KOVA_CHART KOVA_CLI START_OBSERVABILITY KIND_WORKERS
export CONTROLLER_IMAGE RUNNER_IMAGE WORKER_IMAGE
export NO_PROXY="${NO_PROXY:-localhost,127.0.0.1},${REGISTRY_NAME},${CLUSTER_REGISTRY}"
export no_proxy="${NO_PROXY}"

make -C "${ROOT}" kova image
creation_attempted=true
"${ROOT}/scripts/kind/kind-create.sh"
created_resources=true
"${ROOT}/scripts/kind/kind-load.sh"

BASELINE_CHART=${BASELINE_CHART} \
BASELINE_CONTROLLER_IMAGE=ghcr.io/cofy-x/kova:controller-${BASELINE_VERSION} \
BASELINE_RUNNER_IMAGE=ghcr.io/cofy-x/kova:runner-${BASELINE_VERSION} \
BASELINE_WORKER_IMAGE=ghcr.io/cofy-x/kova:worker-${BASELINE_VERSION} \
REQUIRE_LEGACY_CRD=true \
E2E_SERVICE_BUILD_CLI=false \
E2E_SERVICE_BUILD_IMAGE=false \
KIND_LOAD_IMAGES=false \
"${ROOT}/scripts/e2e/e2e-service.sh"
