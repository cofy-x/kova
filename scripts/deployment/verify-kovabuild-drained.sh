#!/usr/bin/env bash

# Read-only pre-upgrade gate. Submission must remain frozen until the new controller is ready.
set -euo pipefail

KUBECTL=${KUBECTL:-kubectl}
NAMESPACE=${NAMESPACE:-kova}
RUNNER_NAMESPACE=${RUNNER_NAMESPACE:-${NAMESPACE}}
RELEASE_NAME=${RELEASE_NAME:-kova}
SERVICE_DEPLOYMENT_NAME=${SERVICE_DEPLOYMENT_NAME:-${RELEASE_NAME}-service}

if [[ -z "${NAMESPACE}" || -z "${RUNNER_NAMESPACE}" || -z "${RELEASE_NAME}" || -z "${SERVICE_DEPLOYMENT_NAME}" ]]; then
  echo 'error: NAMESPACE, RUNNER_NAMESPACE, RELEASE_NAME, and SERVICE_DEPLOYMENT_NAME are required' >&2
  exit 2
fi

if ! builds=$("${KUBECTL}" -n "${RUNNER_NAMESPACE}" get kovabuilds.kova.cofy.dev -o json); then
  echo 'error: cannot list KovaBuilds; block the controller upgrade' >&2
  exit 1
fi
if ! pods=$("${KUBECTL}" -n "${RUNNER_NAMESPACE}" get pods -o json); then
  echo 'error: cannot list Pods; block the controller upgrade' >&2
  exit 1
fi
if ! service_pods=$("${KUBECTL}" -n "${NAMESPACE}" get pods \
  -l "app.kubernetes.io/instance=${RELEASE_NAME},app.kubernetes.io/component=service" -o json); then
  echo 'error: cannot list Service Pods; block the controller upgrade' >&2
  exit 1
fi
if ! deployments=$("${KUBECTL}" -n "${NAMESPACE}" get deployments \
  -l "app.kubernetes.io/instance=${RELEASE_NAME},app.kubernetes.io/component=service" -o json); then
  echo 'error: cannot list Service deployments; block the controller upgrade' >&2
  exit 1
fi

for document in "${builds}" "${pods}" "${service_pods}" "${deployments}"; do
  if ! jq -e '(.items | type) == "array"' <<<"${document}" >/dev/null; then
    echo 'error: Kubernetes returned an invalid resource list; block the controller upgrade' >&2
    exit 1
  fi
done

if ! jq -e --arg name "${SERVICE_DEPLOYMENT_NAME}" \
  '(.items | length) == 1 and .items[0].metadata.name == $name' \
  <<<"${deployments}" >/dev/null; then
  echo "error: expected exactly one Service deployment named ${NAMESPACE}/${SERVICE_DEPLOYMENT_NAME}; block the controller upgrade" >&2
  exit 1
fi

remaining_builds=$(jq -r '[.items[] | "\(.metadata.name):\(.status.phase // "<unset>")"] | join(", ")' <<<"${builds}")
runner_pods=$(jq -r '[.items[] | select(.metadata.labels["app.kubernetes.io/name"] == "kova-runner" or any(.metadata.ownerReferences[]?; .kind == "KovaBuild")) | .metadata.name] | join(", ")' <<<"${pods}")
remaining_service_pods=$(jq -r '[.items[] | .metadata.name] | join(", ")' <<<"${service_pods}")
running_services=$(jq -r '[.items[] | select((.spec.replicas // 1) != 0 or (.status.replicas // 0) != 0 or (.status.readyReplicas // 0) != 0) | .metadata.name] | join(", ")' <<<"${deployments}")

if [[ -n "${remaining_builds}" || -n "${runner_pods}" || -n "${remaining_service_pods}" || -n "${running_services}" ]]; then
  echo "error: Kova is not quiescent in ${RUNNER_NAMESPACE} (Service: ${NAMESPACE}); block the controller upgrade" >&2
  [[ -z "${remaining_builds}" ]] || echo "  KovaBuilds (including terminal receipts): ${remaining_builds}" >&2
  [[ -z "${runner_pods}" ]] || echo "  runner Pods: ${runner_pods}" >&2
  [[ -z "${remaining_service_pods}" ]] || echo "  Service Pods: ${remaining_service_pods}" >&2
  [[ -z "${running_services}" ]] || echo "  Service deployments with replicas: ${running_services}" >&2
  exit 1
fi

echo "Kova runner namespace ${RUNNER_NAMESPACE} and Service ${NAMESPACE}/${RELEASE_NAME} are quiescent; keep submission frozen through the controller upgrade"
