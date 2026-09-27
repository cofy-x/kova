#!/usr/bin/env bash

# Run after applying this release's CRD and before starting its controller.
set -euo pipefail

KUBECTL=${KUBECTL:-kubectl}
CRD=kovabuilds.kova.cofy.dev
STATUS_PATH='.spec.versions[?(@.name=="v1alpha1")].schema.openAPIV3Schema.properties.status.properties'
RETRY_SCHEMA_PATH="{${STATUS_PATH}.pollFailureSince.type}{\"|\"}{${STATUS_PATH}.pollFailureSince.format}{\"|\"}{${STATUS_PATH}.pollFailureCount.type}{\"|\"}{${STATUS_PATH}.pollFailureCount.format}"
SCHEMA_PATH="${RETRY_SCHEMA_PATH}{\"|\"}{range ${STATUS_PATH}.phase.enum[*]}{@}{\",\"}{end}{\"|\"}{${STATUS_PATH}.verificationStartedAt.type}{\"|\"}{${STATUS_PATH}.verificationStartedAt.format}{\"|\"}{${STATUS_PATH}.verificationDeadlineAt.type}{\"|\"}{${STATUS_PATH}.verificationDeadlineAt.format}{\"|\"}{${STATUS_PATH}.verificationNextAttemptAt.type}{\"|\"}{${STATUS_PATH}.verificationNextAttemptAt.format}{\"|\"}{${STATUS_PATH}.verificationAttempts.type}{\"|\"}{${STATUS_PATH}.verificationAttempts.format}{\"|\"}{${STATUS_PATH}.verificationLastError.type}{\"|\"}{${STATUS_PATH}.verificationResults.type}{\"|\"}{${STATUS_PATH}.verificationResults.maxItems}{\"|\"}{${STATUS_PATH}.verificationResults.items.properties.pushedDigest.pattern}{\"|\"}{range ${STATUS_PATH}.verificationResults.items.properties.state.enum[*]}{@}{\",\"}{end}"
EXPECTED_SCHEMA='string|date-time|integer|int32|Queued,Starting,Running,Verifying,FailedVerifying,Succeeded,Failed,Cancelled,|string|date-time|string|date-time|string|date-time|integer|int32|string|array|200|^sha256:[a-f0-9]{64}$|pending,succeeded,failed,'
MODE=${1:-current}

case ${MODE} in
  current|--expect-legacy) ;;
  *) echo "usage: $0 [--expect-legacy]" >&2; exit 2 ;;
esac

if ! "${KUBECTL}" wait --for=condition=Established "crd/${CRD}" --timeout=60s >&2; then
  echo "error: ${CRD} is not Established; block the controller upgrade" >&2
  exit 1
fi

query_path=${SCHEMA_PATH}
if [[ "${MODE}" == '--expect-legacy' ]]; then
  query_path=${RETRY_SCHEMA_PATH}
fi
if ! schema=$("${KUBECTL}" get crd "${CRD}" -o "jsonpath=${query_path}"); then
  echo "error: cannot read ${CRD} schema; block the controller upgrade" >&2
  exit 1
fi

if [[ "${MODE}" == '--expect-legacy' ]]; then
  if [[ "${schema}" != '|||' ]]; then
    echo "error: ${CRD} is not the expected legacy schema (${schema}); stop the migration test" >&2
    exit 1
  fi
  echo "${CRD} is Established with legacy v1alpha1 verification fields absent"
else
  if [[ "${schema}" != "${EXPECTED_SCHEMA}" ]]; then
    echo "error: ${CRD} v1alpha1 status verification schema is missing or incompatible (${schema}); block the controller upgrade" >&2
    exit 1
  fi
  echo "${CRD} is Established with compatible v1alpha1 status verification fields"
fi
