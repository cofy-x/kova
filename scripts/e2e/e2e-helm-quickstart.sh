#!/usr/bin/env bash

set -euo pipefail

source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

ROOT=$(repo_root)
QUICKSTART_TAG=${QUICKSTART_TAG:-v0.0.0-dev}
KIND_CLUSTER=${KIND_CLUSTER:-kova-quickstart}
KIND_CONFIG=${KIND_CONFIG:-deploy/quickstart-kind-cluster.yaml}
KIND_KUBECONFIG=${KIND_KUBECONFIG:-.kind/${KIND_CLUSTER}.kubeconfig}
KIND_WORKERS=${KIND_WORKERS:-1}
KIND_VALUES=${KIND_VALUES:-deploy/quickstart-kind-values.yaml}
KOVA_RUNNER_NAME=${KOVA_RUNNER_NAME:-quickstart}
KEEP_KIND_CLUSTER=${KEEP_KIND_CLUSTER:-false}
REUSE_KIND_CLUSTER=${REUSE_KIND_CLUSTER:-false}

require_cmd helm
require_kind

case ${KEEP_KIND_CLUSTER} in
  true|false) ;;
  *)
    echo "error: KEEP_KIND_CLUSTER must be true or false" >&2
    exit 2
    ;;
esac

case ${REUSE_KIND_CLUSTER} in
  true|false) ;;
  *)
    echo "error: REUSE_KIND_CLUSTER must be true or false" >&2
    exit 2
    ;;
esac

if [[ "${KIND_KUBECONFIG}" == /* ]]; then
  kind_kubeconfig=${KIND_KUBECONFIG}
else
  kind_kubeconfig=${ROOT}/${KIND_KUBECONFIG}
fi

owns_kind_cluster=false
if kind get clusters | grep -qx "${KIND_CLUSTER}"; then
  if [[ "${REUSE_KIND_CLUSTER}" != "true" ]]; then
    echo "error: kind cluster ${KIND_CLUSTER} already exists" >&2
    echo "Delete it first or set REUSE_KIND_CLUSTER=true to reuse it without automatic deletion." >&2
    exit 2
  fi
  echo "Reusing caller-owned kind cluster ${KIND_CLUSTER}; it will not be deleted." >&2
else
  owns_kind_cluster=true
fi

chart_dir=$(mktemp -d "${TMPDIR:-/tmp}/kova-quickstart-chart.XXXXXX")
cleanup() {
  local status=$?

  rm -rf "${chart_dir}"
  if [[ "${owns_kind_cluster}" == "true" ]]; then
    if [[ "${KEEP_KIND_CLUSTER}" == "true" ]]; then
      echo "Keeping kind cluster ${KIND_CLUSTER} for debugging." >&2
      echo "Delete it with: kind delete cluster --name ${KIND_CLUSTER}" >&2
    else
      kind delete cluster --name "${KIND_CLUSTER}" || true
      rm -f "${kind_kubeconfig}"
    fi
  fi

  return "${status}"
}
trap cleanup EXIT

DIST_DIR=${chart_dir} "${ROOT}/scripts/release/package-chart.sh" \
  "${QUICKSTART_TAG}" >/dev/null
chart=${chart_dir}/kova-${QUICKSTART_TAG#v}.tgz

export KIND_CLUSTER KIND_CONFIG KIND_KUBECONFIG KIND_WORKERS KIND_VALUES KOVA_RUNNER_NAME
export KOVA_CHART=${chart}
case ${KIND_VALUES} in
  /*) export KOVA_VALUES=${KIND_VALUES} ;;
  *) export KOVA_VALUES=${ROOT}/${KIND_VALUES} ;;
esac
export START_OBSERVABILITY=false
export KOVA_DAEMON_OTEL_ENABLED=false

"${ROOT}/scripts/e2e/e2e-service.sh"
