#!/usr/bin/env bash

set -euo pipefail

source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

ROOT=$(repo_root)
action=${1:-}
shift || true

KIND_KUBECONFIG=${KIND_KUBECONFIG:-.kind/kova-local.kubeconfig}
NAMESPACE=${NAMESPACE:-kova}
MINIO_NAME=${MINIO_NAME:-kova-e2e-minio}
MINIO_SECRET=${MINIO_SECRET:-kova-e2e-s3}
S3_BUCKET=${S3_BUCKET:-kova-builds}
MINIO_IMAGE=${MINIO_IMAGE:-minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e}
MINIO_MC_IMAGE=${MINIO_MC_IMAGE:-minio/mc:RELEASE.2025-08-13T08-35-41Z@sha256:a7fe349ef4bd8521fb8497f55c6042871b2ae640607cf99d9bede5e9bdf11727}

case "${KIND_KUBECONFIG}" in
  /*) kubeconfig=${KIND_KUBECONFIG} ;;
  *) kubeconfig=${ROOT}/${KIND_KUBECONFIG} ;;
esac

require_cmd kubectl
require_cmd openssl

job_name=${MINIO_NAME}-configure

wait_for_job() {
  if ! kubectl --kubeconfig "${kubeconfig}" -n "${NAMESPACE}" \
      wait --for=condition=complete "job/${job_name}" --timeout=120s; then
    kubectl --kubeconfig "${kubeconfig}" -n "${NAMESPACE}" \
      logs "job/${job_name}" --all-containers=true >&2 || true
    return 1
  fi
}

apply_client_job() {
  local script=$1 build_ids=${2:-} indented_script
  indented_script="              ${script//$'\n'/$'\n              '}"
  kubectl --kubeconfig "${kubeconfig}" -n "${NAMESPACE}" \
    delete job "${job_name}" --ignore-not-found --wait=true >/dev/null
  kubectl --kubeconfig "${kubeconfig}" apply -f - >/dev/null <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: ${job_name}
  namespace: ${NAMESPACE}
spec:
  backoffLimit: 1
  ttlSecondsAfterFinished: 60
  template:
    spec:
      restartPolicy: Never
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
        runAsGroup: 1000
      containers:
        - name: mc
          image: ${MINIO_MC_IMAGE}
          command: ["/bin/sh", "-ec"]
          args:
            - |
              mc alias set storage http://${MINIO_NAME}:9000 "\${KOVA_S3_ACCESS_KEY}" "\${KOVA_S3_SECRET_KEY}"
${indented_script}
          env:
            - name: HOME
              value: /tmp
            - name: KOVA_S3_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: ${MINIO_SECRET}
                  key: KOVA_S3_ACCESS_KEY
            - name: KOVA_S3_SECRET_KEY
              valueFrom:
                secretKeyRef:
                  name: ${MINIO_SECRET}
                  key: KOVA_S3_SECRET_KEY
            - name: BUILD_IDS
              value: "${build_ids}"
EOF
  wait_for_job
}

deploy() {
  local access_file secret_file secret_value
  kubectl --kubeconfig "${kubeconfig}" create namespace "${NAMESPACE}" \
    --dry-run=client -o yaml | \
    kubectl --kubeconfig "${kubeconfig}" apply -f - >/dev/null

  if ! kubectl --kubeconfig "${kubeconfig}" -n "${NAMESPACE}" \
      get secret "${MINIO_SECRET}" >/dev/null 2>&1; then
    umask 077
    access_file=$(mktemp "${TMPDIR:-/tmp}/kova-minio-access.XXXXXX")
    secret_file=$(mktemp "${TMPDIR:-/tmp}/kova-minio-secret.XXXXXX")
    trap 'rm -f "${access_file}" "${secret_file}"' RETURN
    secret_value=$(openssl rand -hex 24)
    printf '%s' kovae2e >"${access_file}"
    printf '%s' "${secret_value}" >"${secret_file}"
    kubectl --kubeconfig "${kubeconfig}" -n "${NAMESPACE}" \
      create secret generic "${MINIO_SECRET}" \
      --from-file=KOVA_S3_ACCESS_KEY="${access_file}" \
      --from-file=MINIO_ROOT_USER="${access_file}" \
      --from-file=KOVA_S3_SECRET_KEY="${secret_file}" \
      --from-file=MINIO_ROOT_PASSWORD="${secret_file}" >/dev/null
  fi

  kubectl --kubeconfig "${kubeconfig}" apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Service
metadata:
  name: ${MINIO_NAME}
  namespace: ${NAMESPACE}
spec:
  selector:
    app.kubernetes.io/name: ${MINIO_NAME}
  ports:
    - name: s3
      port: 9000
      targetPort: s3
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${MINIO_NAME}
  namespace: ${NAMESPACE}
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: ${MINIO_NAME}
  template:
    metadata:
      labels:
        app.kubernetes.io/name: ${MINIO_NAME}
    spec:
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
        runAsGroup: 1000
        fsGroup: 1000
      containers:
        - name: minio
          image: ${MINIO_IMAGE}
          args: ["server", "/data", "--address", ":9000"]
          env:
            - name: MINIO_ROOT_USER
              valueFrom:
                secretKeyRef:
                  name: ${MINIO_SECRET}
                  key: MINIO_ROOT_USER
            - name: MINIO_ROOT_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: ${MINIO_SECRET}
                  key: MINIO_ROOT_PASSWORD
          ports:
            - name: s3
              containerPort: 9000
          readinessProbe:
            httpGet:
              path: /minio/health/ready
              port: s3
          volumeMounts:
            - name: data
              mountPath: /data
          securityContext:
            allowPrivilegeEscalation: false
            capabilities:
              drop: ["ALL"]
      volumes:
        - name: data
          emptyDir: {}
EOF

  kubectl --kubeconfig "${kubeconfig}" -n "${NAMESPACE}" \
    rollout status "deployment/${MINIO_NAME}" --timeout=180s
  apply_client_job "mc mb --ignore-existing storage/${S3_BUCKET}"
}

assert_empty() {
  local build_ids=() build_id joined client_script
  if [[ "$#" -eq 0 ]]; then
    read -r -d '' client_script <<EOF || true
objects=\$(mc ls --recursive "storage/${S3_BUCKET}/" 2>/dev/null || true)
if [ -n "\${objects}" ]; then
  echo "S3 E2E bucket is not empty" >&2
  printf "%s\\n" "\${objects}" >&2
  exit 1
fi
EOF
    apply_client_job "${client_script}"
    return
  fi
  for build_id in "$@"; do
    if [[ ! "${build_id}" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]]; then
      echo "error: invalid build ID for S3 assertion: ${build_id}" >&2
      exit 2
    fi
    build_ids+=("${build_id}")
  done
  joined=${build_ids[*]}
  read -r -d '' client_script <<EOF || true
for id in \${BUILD_IDS}; do
    objects=\$(mc ls --recursive "storage/${S3_BUCKET}/builds/\${id}/" 2>/dev/null || true)
    if [ -n "\${objects}" ]; then
      echo "artifacts remain for build \${id}" >&2
      printf "%s\\n" "\${objects}" >&2
      exit 1
    fi
done
EOF
  apply_client_job "${client_script}" "${joined}"
}

case "${action}" in
  deploy) deploy ;;
  assert-empty) assert_empty "$@" ;;
  *)
    echo "usage: $0 deploy | assert-empty [build-id...]" >&2
    exit 2
    ;;
esac
