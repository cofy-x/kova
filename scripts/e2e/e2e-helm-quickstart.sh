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

require_cmd helm

chart_dir=$(mktemp -d "${TMPDIR:-/tmp}/kova-quickstart-chart.XXXXXX")
trap 'rm -rf "${chart_dir}"' EXIT

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
