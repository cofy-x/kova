#!/usr/bin/env bash

# Run after applying this release's CRD and before starting its controller.
set -euo pipefail

KUBECTL=${KUBECTL:-kubectl}
CRD=kovabuilds.kova.cofy.dev
SCHEMA_PATH='{.spec.versions[?(@.name=="v1alpha1")].schema.openAPIV3Schema.properties.status.properties.pollFailureSince.type}{"|"}{.spec.versions[?(@.name=="v1alpha1")].schema.openAPIV3Schema.properties.status.properties.pollFailureSince.format}{"|"}{.spec.versions[?(@.name=="v1alpha1")].schema.openAPIV3Schema.properties.status.properties.pollFailureCount.type}{"|"}{.spec.versions[?(@.name=="v1alpha1")].schema.openAPIV3Schema.properties.status.properties.pollFailureCount.format}'
MODE=${1:-current}

case ${MODE} in
  current|--expect-legacy) ;;
  *) echo "usage: $0 [--expect-legacy]" >&2; exit 2 ;;
esac

if ! "${KUBECTL}" wait --for=condition=Established "crd/${CRD}" --timeout=60s >&2; then
  echo "error: ${CRD} is not Established; block the controller upgrade" >&2
  exit 1
fi

if ! schema=$("${KUBECTL}" get crd "${CRD}" -o "jsonpath=${SCHEMA_PATH}"); then
  echo "error: cannot read ${CRD} schema; block the controller upgrade" >&2
  exit 1
fi

if [[ "${MODE}" == '--expect-legacy' ]]; then
  if [[ "${schema}" != '|||' ]]; then
    echo "error: ${CRD} is not the expected legacy schema (${schema}); stop the migration test" >&2
    exit 1
  fi
  echo "${CRD} is Established with legacy v1alpha1 status retry fields absent"
else
  if [[ "${schema}" != 'string|date-time|integer|int32' ]]; then
    echo "error: ${CRD} v1alpha1 status retry schema is missing or incompatible (${schema}); block the controller upgrade" >&2
    exit 1
  fi
  echo "${CRD} is Established with compatible v1alpha1 status retry fields"
fi
