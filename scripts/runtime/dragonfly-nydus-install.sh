#!/usr/bin/env bash

set -euo pipefail

source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

ROOT=$(repo_root)
KIND_CLUSTER=${KIND_CLUSTER:-kova-local}
KIND_KUBECONFIG=${KIND_KUBECONFIG:-.kind/${KIND_CLUSTER}.kubeconfig}
DRAGONFLY_NAMESPACE=${DRAGONFLY_NAMESPACE:-dragonfly-system}
DRAGONFLY_RELEASE_NAME=${DRAGONFLY_RELEASE_NAME:-dragonfly}
NYDUS_RELEASE_NAME=${NYDUS_RELEASE_NAME:-nydus}
DRAGONFLY_CHART_VERSION=${DRAGONFLY_CHART_VERSION:-1.6.27}
NYDUS_SNAPSHOTTER_CHART_VERSION=${NYDUS_SNAPSHOTTER_CHART_VERSION:-0.0.10}
DRAGONFLY_VALUES=${DRAGONFLY_VALUES:-deploy/dragonfly-values.yaml}
NYDUS_VALUES=${NYDUS_VALUES:-deploy/nydus-values.yaml}
LOCAL_PROXY_PORT=${LOCAL_PROXY_PORT:-7890}
DRAGONFLY_PROXY_MODE=${DRAGONFLY_PROXY_MODE:-auto}
DRAGONFLY_PROXY_NO_PROXY=${DRAGONFLY_PROXY_NO_PROXY:-127.0.0.1,localhost,kind-registry,kind-registry:5000,.svc,.cluster.local,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16}
REGISTRY_HOST=${REGISTRY_HOST:-localhost:5002}
REGISTRY_ALIASES=${REGISTRY_ALIASES:-localhost:5002 host.docker.internal:5002}
REGISTRY_NAME=${REGISTRY_NAME:-kind-registry}

require_cmd docker
require_cmd helm
require_cmd kind
require_cmd kubectl

if ! kind get clusters | grep -qx "${KIND_CLUSTER}"; then
  echo "error: kind cluster ${KIND_CLUSTER} does not exist; run make kind-create first" >&2
  exit 1
fi

KUBECONFIG="${ROOT}/${KIND_KUBECONFIG}"
export KUBECONFIG

proxy_url=${HTTP_PROXY:-${http_proxy:-}}
if [[ -z "${proxy_url}" ]]; then
  proxy_url=$(detect_host_http_proxy || true)
fi
if [[ -n "${proxy_url}" ]]; then
  proxy_url=${proxy_url/127.0.0.1/host.docker.internal}
  proxy_url=${proxy_url/localhost/host.docker.internal}
fi

proxy_enabled=0
case "${DRAGONFLY_PROXY_MODE}" in
  on)
    proxy_enabled=1
    ;;
  off)
    proxy_enabled=0
    ;;
  auto)
    if [[ -n "${proxy_url}" ]]; then
      proxy_enabled=1
    fi
    ;;
  *)
    echo "error: DRAGONFLY_PROXY_MODE must be auto, on, or off" >&2
    exit 1
    ;;
esac

for node in $(kind_worker_nodes "${KIND_CLUSTER}"); do
  echo "Using overlayfs while installing Dragonfly on ${node}" >&2
  docker exec "${node}" bash -lc "
set -euo pipefail
conf=/etc/containerd/config.toml
if grep -q 'snapshotter = \"nydus\"' \"\$conf\"; then
  sed -i 's/snapshotter = \"nydus\"/snapshotter = \"overlayfs\"/g' \"\$conf\"
  systemctl restart containerd
fi
"
done

helm repo add dragonfly https://dragonflyoss.github.io/helm-charts/ >/dev/null 2>&1 || true
helm repo update >/dev/null

helm_args=(
  --namespace "${DRAGONFLY_NAMESPACE}"
  --create-namespace
  --version "${DRAGONFLY_CHART_VERSION}"
  -f "${ROOT}/${DRAGONFLY_VALUES}"
  --wait
  --timeout 20m
)

if [[ "${proxy_enabled}" == "1" ]]; then
  echo "Using Dragonfly client proxy: ${proxy_url}" >&2
  helm_args+=(
    --set-json "client.extraEnvVars=[{\"name\":\"HTTP_PROXY\",\"value\":\"${proxy_url}\"},{\"name\":\"HTTPS_PROXY\",\"value\":\"${proxy_url}\"},{\"name\":\"NO_PROXY\",\"value\":\"${DRAGONFLY_PROXY_NO_PROXY}\"},{\"name\":\"http_proxy\",\"value\":\"${proxy_url}\"},{\"name\":\"https_proxy\",\"value\":\"${proxy_url}\"},{\"name\":\"no_proxy\",\"value\":\"${DRAGONFLY_PROXY_NO_PROXY}\"}]"
    --set-json "seedClient.extraEnvVars=[{\"name\":\"HTTP_PROXY\",\"value\":\"${proxy_url}\"},{\"name\":\"HTTPS_PROXY\",\"value\":\"${proxy_url}\"},{\"name\":\"NO_PROXY\",\"value\":\"${DRAGONFLY_PROXY_NO_PROXY}\"},{\"name\":\"http_proxy\",\"value\":\"${proxy_url}\"},{\"name\":\"https_proxy\",\"value\":\"${proxy_url}\"},{\"name\":\"no_proxy\",\"value\":\"${DRAGONFLY_PROXY_NO_PROXY}\"}]"
  )
fi

helm upgrade --install "${DRAGONFLY_RELEASE_NAME}" dragonfly/dragonfly "${helm_args[@]}"

helm upgrade --install "${NYDUS_RELEASE_NAME}" dragonfly/nydus-snapshotter \
  --namespace "${DRAGONFLY_NAMESPACE}" \
  --create-namespace \
  --version "${NYDUS_SNAPSHOTTER_CHART_VERSION}" \
  -f "${ROOT}/${NYDUS_VALUES}" \
  --wait \
  --timeout 20m

kubectl rollout status deployment/"${DRAGONFLY_RELEASE_NAME}"-manager -n "${DRAGONFLY_NAMESPACE}" --timeout=300s
kubectl rollout status daemonset/"${DRAGONFLY_RELEASE_NAME}"-client -n "${DRAGONFLY_NAMESPACE}" --timeout=300s
kubectl rollout restart daemonset/"${NYDUS_RELEASE_NAME}"-nydus-snapshotter -n "${DRAGONFLY_NAMESPACE}"
kubectl rollout status daemonset/"${NYDUS_RELEASE_NAME}"-nydus-snapshotter -n "${DRAGONFLY_NAMESPACE}" --timeout=300s

for node in $(kind_worker_nodes "${KIND_CLUSTER}"); do
  echo "Patching containerd for nydus on ${node}" >&2
  docker exec "${node}" bash -lc "
set -euo pipefail
conf=/etc/containerd/config.toml
tmp=\$(mktemp)

awk '
BEGIN { proxy_seen=0; in_nydus=0 }
{
  if (\$0 == \"[proxy_plugins]\") {
    proxy_seen++
    if (proxy_seen > 1) next
  }
  if (\$0 == \"[proxy_plugins.nydus]\") {
    in_nydus=1
    next
  }
  if (in_nydus == 1 && \$0 ~ /^\\[/) {
    in_nydus=0
  }
  if (in_nydus == 1) next
  print
}
' \"\$conf\" > \"\$tmp\"
mv \"\$tmp\" \"\$conf\"

if ! grep -q '^\\[proxy_plugins\\]\$' \"\$conf\"; then
  printf '\\n[proxy_plugins]\\n' >> \"\$conf\"
fi

sed -i 's/snapshotter = \"overlayfs\"/snapshotter = \"nydus\"/g' \"\$conf\"
sed -i 's/discard_unpacked_layers = true/discard_unpacked_layers = false/g' \"\$conf\"

if grep -q 'disable_snapshot_annotations' \"\$conf\"; then
  sed -i 's/disable_snapshot_annotations = true/disable_snapshot_annotations = false/g' \"\$conf\"
else
  awk '
  {
    print
    if (\$0 ~ /snapshotter = \"nydus\"/ && done == 0) {
      print \"  disable_snapshot_annotations = false\"
      done=1
    }
  }
  ' \"\$conf\" > \"\$tmp\"
  mv \"\$tmp\" \"\$conf\"
fi

cat >> \"\$conf\" <<'EOF'

[proxy_plugins.nydus]
  type = \"snapshot\"
  address = \"/run/containerd-nydus/containerd-nydus-grpc.sock\"
EOF

mkdir -p /run/containerd-nydus /var/lib/containerd-nydus
for registry_host in ${REGISTRY_ALIASES}; do
  mkdir -p /etc/containerd/certs.d/\${registry_host}
  printf '%s\n' \
    \"server = \\\"http://\${registry_host}\\\"\" \
    '' \
    '[host.\"http://127.0.0.1:4001\"]' \
    'capabilities = [\"pull\", \"resolve\"]' \
    'skip_verify = true' \
    '' \
    '[host.\"http://127.0.0.1:4001\".header]' \
    'X-Dragonfly-Registry = \"http://${REGISTRY_NAME}:5000\"' \
    > /etc/containerd/certs.d/\${registry_host}/hosts.toml

  mirror_table=\"[plugins.\\\"io.containerd.grpc.v1.cri\\\".registry.mirrors.\\\"\${registry_host}\\\"]\"
  if ! grep -Fq \"\${mirror_table}\" \"\$conf\"; then
    printf '\\n%s\\n  endpoint = [\"http://${REGISTRY_NAME}:5000\"]\\n' \"\${mirror_table}\" >> \"\$conf\"
  fi
done

if [[ \"${proxy_enabled}\" == \"1\" ]]; then
  mkdir -p /etc/systemd/system/containerd.service.d
  cat > /etc/systemd/system/containerd.service.d/http-proxy.conf <<'EOF'
[Service]
Environment=\"HTTP_PROXY=${proxy_url}\"
Environment=\"HTTPS_PROXY=${proxy_url}\"
Environment=\"NO_PROXY=${DRAGONFLY_PROXY_NO_PROXY}\"
Environment=\"http_proxy=${proxy_url}\"
Environment=\"https_proxy=${proxy_url}\"
Environment=\"no_proxy=${DRAGONFLY_PROXY_NO_PROXY}\"
EOF
else
  rm -f /etc/systemd/system/containerd.service.d/http-proxy.conf
fi

systemctl daemon-reload
systemctl restart containerd
for i in \$(seq 1 30); do
  if systemctl is-active --quiet containerd; then
    exit 0
  fi
  sleep 1
done

systemctl status containerd --no-pager || true
exit 1
"
done

kubectl wait --for=condition=Ready node --all --timeout=180s
kubectl get pods -n "${DRAGONFLY_NAMESPACE}" -o wide
